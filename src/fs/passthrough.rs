// Passthrough :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using the PathFilesystem wrapper over the FUSE Filesystem trait.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};

use super::inode_translator::*;
use super::super::libc_wrappers;

use fuse::*;
use time::*;

mod libc {
    pub use ::libc::*;

    // stuff missing from the libc crate.
    extern "system" {
        // Specified by POSIX.1-2008; not sure why this is missing.
        pub fn fchown(fd: c_int, uid: uid_t, gid: gid_t) -> c_int;

        // On Mac OS X, off_t is always 64 bits.
        // https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/64bitPorting/transition/transition.html
        #[cfg(target_os = "macos")]
        pub fn truncate(path: *const c_char, size: off_t) -> c_int;

        // On Linux, off_t is architecture-dependent, and this is provided for 32-bit systems:
        #[cfg(target_os = "linux")]
        pub fn truncate64(path: *const c_char, size: off64_t) -> c_int;

        #[cfg(target_os = "macos")]
        pub fn lutimes(path: *const c_char, times: *const timeval) -> c_int;
    }

    #[cfg(target_os = "macos")]
    pub fn truncate64(path: *const c_char, size: off_t) -> c_int {
        truncate(path, size)
    }

    #[cfg(target_os = "macos")]
    fn timespec_to_timeval(timespec: &timespec) -> timeval {
        timeval {
            tv_sec: timespec.tv_sec,
            tv_usec: timespec.tv_nsec * 1000,
        }
    }

    pub const UTIME_OMIT: i64 = ((11 << 30) - 21);

    // Mac OS X does not support futimens; map it to futimes with lower precision.
    #[cfg(target_os = "macos")]
    pub fn futimens(fd: c_int, times: *const timespec) -> c_int {
        unsafe {
            let mut times_osx = [timespec_to_timeval(&*times),
                                 timespec_to_timeval(&*times.offset(1))];

            let mut stat: Option<stat64> = None;

            if (*times).tv_nsec == UTIME_OMIT {
                // atime is unspecified

                stat = match libc_wrappers::fstat(fd as u64) {
                    Ok(s) => Some(s),
                    Err(e) => return e,
                };

                times_osx[0].tv_sec = stat.unwrap().st_atime;
                times_osx[0].tv_usec = stat.unwrap().st_atime_nsec * 1000;
            }

            if (*times.offset(1)).tv_nsec == UTIME_OMIT {
                // mtime is unspecified

                if stat.is_none() {
                    stat = match libc_wrappers::fstat(fd as u64) {
                        Ok(s) => Some(s),
                        Err(e) => return e,
                    };
                }

                times_osx[1].tv_sec = stat.unwrap().st_mtime;
                times_osx[1].tv_usec = stat.unwrap().st_mtime_nsec * 1000;
            }

            futimes(fd, &times_osx as *const timeval)
        }
    }

    // Mac OS X does not support utimensat; map it to lutimes with lower precision.
    // The relative path feature of utimensat is not supported by this workaround.
    #[cfg(target_os = "macos")]
    pub fn utimensat(_dirfd_ignored: c_int, path: *const c_char, times: *const timespec) -> c_int {
        use super::super::super::libc_wrappers;
        unsafe {
            assert_eq!(*path, b'/' as c_char); // relative paths are not supported here!
            let mut times_osx = [timespec_to_timeval(&*times),
                                 timespec_to_timeval(&*times.offset(1))];

            let mut stat: Option<stat64> = None;
            fn stat_if_needed(path: *const c_char, stat: &mut Option<stat64>) -> Result<(), c_int> {
                use std::ffi::{CStr, OsString};
                use std::os::unix::ffi::OsStringExt;
                if stat.is_none() {
                    let path_c = unsafe { CStr::from_ptr(path) } .to_owned();
                    let path_os = OsString::from_vec(path_c.into_bytes());
                    *stat = Some(try!(libc_wrappers::lstat(path_os)));
                }
                Ok(())
            }

            if (*times).tv_nsec == UTIME_OMIT {
                // atime is unspecified

                if let Err(e) = stat_if_needed(path, &mut stat) {
                    return e;
                }

                times_osx[0].tv_sec = stat.unwrap().st_atime;
                times_osx[0].tv_usec = stat.unwrap().st_atime_nsec * 1000;
            }

            if (*times.offset(1)).tv_nsec == UTIME_OMIT {
                // mtime is unspecified

                if stat.is_none() {
                    if let Err(e) = stat_if_needed(path, &mut stat) {
                        return e;
                    }
                }
                times_osx[1].tv_sec = stat.unwrap().st_mtime;
                times_osx[1].tv_usec = stat.unwrap().st_mtime_nsec * 1000;
            }

            lutimes(path, &times_osx as *const timeval)
        }
    }
}

pub struct Passthrough {
    pub target: OsString,
}

fn mode_to_filetype(mode: libc::mode_t) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO  => FileType::NamedPipe,
        libc::S_IFSOCK => {
            warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
            FileType::NamedPipe
        },
        _ => { panic!("unknown file type"); }
    }
}

fn stat_to_fuse(stat: libc::stat64) -> FileAttr {
    let kind = mode_to_filetype(stat.st_mode);

    let mode = stat.st_mode & 0o7777; // st_mode encodes the type AND the mode.

    FileAttr {
        ino: 0,
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: Timespec { sec: stat.st_atime as i64, nsec: stat.st_atime_nsec as i32 },
        mtime: Timespec { sec: stat.st_mtime as i64, nsec: stat.st_mtime_nsec as i32 },
        ctime: Timespec { sec: stat.st_ctime as i64, nsec: stat.st_ctime_nsec as i32 },
        crtime: Timespec { sec: 0, nsec: 0 },
        kind: kind,
        perm: mode as u16,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        flags: 0,
    }
}

impl Passthrough {
    fn real_path(&self, partial: &Path) -> OsString {
        PathBuf::from(&self.target)
                .join(partial.strip_prefix("/").unwrap())
                .into_os_string()
    }

    fn stat_real(&self, path: &Path) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(path);
        debug!("stat_real: {:?}", real);

        match libc_wrappers::lstat(real) {
            Ok(stat) => {
                Ok(stat_to_fuse(stat))
            },
            Err(e) => {
                let err = io::Error::from_raw_os_error(e);
                error!("lstat({:?}): {}", path, err);
                Err(err)
            }
        }
    }
}

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

impl PathFilesystem for Passthrough {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        debug!("init");
        Ok(())
    }

    fn destroy(&self, _req: RequestInfo) {
        debug!("destroy");
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultGetattr {
        debug!("getattr: {:?}", path);

        if let Some(fh) = fh {
            match libc_wrappers::fstat(fh) {
                Ok(stat) => Ok((TTL, stat_to_fuse(stat))),
                Err(e) => Err(e)
            }
        } else {
            match self.stat_real(path) {
                Ok(attr) => Ok((TTL, attr)),
                Err(e) => Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn lookup(&self, _req: RequestInfo, parent: &Path, name: &Path) -> ResultLookup {
        debug!("lookup: {:?}/{:?}", parent, name);

        let path = PathBuf::from(parent).join(name);
        match self.stat_real(&path) {
            Ok(attr) => Ok((TTL, attr, 0)),
            Err(e) => Err(e.raw_os_error().unwrap()),
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let real = self.real_path(path);
        debug!("opendir: {:?}", real);
        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh, 0)),
            Err(e) => Err(e)
        }
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        libc_wrappers::closedir(fh)
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64) -> ResultReaddir {
        debug!("readdir: {:?}", path);
        let mut entries: Vec<DirectoryEntry> = vec![];

        if fh == 0 {
            error!("readdir: missing fh");
            return Err(libc::EINVAL);
        }

        if offset == 0 {
            entries.push(DirectoryEntry { name: PathBuf::from("."), kind: FileType::Directory });
            entries.push(DirectoryEntry { name: PathBuf::from(".."), kind: FileType::Directory });
        }

        loop {
            match libc_wrappers::readdir(fh) {
                Ok(Some(entry)) => {
                    let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                    let name_path = PathBuf::from(OsStr::from_bytes(name_c.to_bytes()));

                    let filetype = match entry.d_type {
                        libc::DT_DIR => FileType::Directory,
                        libc::DT_REG => FileType::RegularFile,
                        libc::DT_LNK => FileType::Symlink,
                        libc::DT_BLK => FileType::BlockDevice,
                        libc::DT_CHR => FileType::CharDevice,
                        libc::DT_FIFO => FileType::NamedPipe,
                        libc::DT_SOCK => {
                            warn!("FUSE doesn't support Socket file type; translating to NamedPipe instead.");
                            FileType::NamedPipe
                        },
                        0 | _ => {
                            let entry_path = PathBuf::from(path).join(&name_path);
                            let real_path = self.real_path(&entry_path);
                            match libc_wrappers::lstat(real_path) {
                                Ok(stat64) => mode_to_filetype(stat64.st_mode),
                                Err(errno) => {
                                    let ioerr = io::Error::from_raw_os_error(errno);
                                    panic!("lstat failed after readdir_r gave no file type for {:?}: {}",
                                           entry_path, ioerr);
                                }
                            }
                        }
                    };

                    entries.push(DirectoryEntry {
                        name: name_path,
                        kind: filetype,
                    })
                },
                Ok(None) => { break; },
                Err(e) => {
                    error!("readdir: {:?}: {}", path, e);
                    return Err(e);
                }
            }
        }

        Ok(entries)
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        debug!("open: {:?} flags={:#x}", path, flags);

        let real = self.real_path(path);
        match libc_wrappers::open(real, flags as libc::c_int) {
            Ok(fh) => Ok((fh, flags)),
            Err(e) => {
                error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn release(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        debug!("release: {:?}", path);
        libc_wrappers::close(fh)
    }

    fn read(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        let mut data = Vec::<u8>::with_capacity(size as usize);
        unsafe { data.set_len(size as usize) };

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return Err(e.raw_os_error().unwrap());
        }
        match file.read(&mut data) {
            Ok(n) => { data.truncate(n); },
            Err(e) => {
                error!("read {:?}, {:#x} @ {:#x}: {}", path, size, offset, e);
                return Err(e.raw_os_error().unwrap());
            }
        }

        Ok(data)
    }

    fn write(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64, data: &[u8], _flags: u32) -> ResultWrite {
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return Err(e.raw_os_error().unwrap());
        }
        let nwritten: u32 = match file.write(data) {
            Ok(n) => n as u32,
            Err(e) => {
                error!("write {:?}, {:#x} @ {:#x}: {}", path, data.len(), offset, e);
                return Err(e.raw_os_error().unwrap());
            }
        };

        Ok(nwritten)
    }

    fn flush(&self, _req: RequestInfo, path: &Path, fh: u64, _lock_owner: u64) -> ResultEmpty {
        debug!("flush: {:?}", path);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.flush() {
            error!("flush({:?}): {}", path, e);
            return Err(e.raw_os_error().unwrap());
        }

        Ok(())
    }

    fn fsync(&self, _req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
        debug!("fsync: {:?}, data={:?}", path, datasync);
        let file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = if datasync {
            file.sync_data()
        } else {
            file.sync_all()
        } {
            error!("fsync({:?}, {:?}): {}", path, datasync, e);
            return Err(e.raw_os_error().unwrap());
        }

        Ok(())
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, mode: u32) -> ResultEmpty {
        debug!("chown: {:?} to {:#o}", path, mode);

        let result = if let Some(fh) = fh {
            unsafe { libc::fchmod(fh as libc::c_int, mode as libc::mode_t) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chmod(path_c.as_ptr(), mode as libc::mode_t)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chown({:?}, {:#o}): {}", path, mode, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn chown(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, uid: Option<u32>, gid: Option<u32>) -> ResultEmpty {
        let uid = uid.unwrap_or(::std::u32::MAX);   // docs say "-1", but uid_t is unsigned
        let gid = gid.unwrap_or(::std::u32::MAX);   // ditto for gid_t
        debug!("chmod: {:?} to {}:{}", path, uid, gid);

        let result = if let Some(fd) = fh {
            unsafe { libc::fchown(fd as libc::c_int, uid, gid) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::chown(path_c.as_ptr(), uid, gid)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("chmod({:?}, {}, {}): {}", path, uid, gid, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!("truncate: {:?} to {:#x}", path, size);

        let result = if let Some(fd) = fh {
            unsafe { libc::ftruncate64(fd as libc::c_int, size as i64) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::truncate64(path_c.as_ptr(), size as i64)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("truncate({:?}, {}): {}", path, size, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn utimens(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>) -> ResultEmpty {
        debug!("utimens: {:?}: {:?}, {:?}", path, atime, mtime);


        fn timespec_to_libc(time: Option<Timespec>) -> libc::timespec {
            if let Some(time) = time {
                libc::timespec {
                    tv_sec: time.sec,
                    tv_nsec: time.nsec as i64,
                }
            } else {
                libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_OMIT,
                }
            }
        }

        let times = [timespec_to_libc(atime), timespec_to_libc(mtime)];

        let result = if let Some(fd) = fh {
            unsafe { libc::futimens(fd as libc::c_int, &times as *const libc::timespec) }
        } else {
            let real = self.real_path(path);
            unsafe {
                let path_c = CString::from_vec_unchecked(real.into_vec());
                libc::utimensat(0, path_c.as_ptr(), &times as *const libc::timespec, 0)
            }
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("utimens({:?}, {:?}, {:?}): {}", path, atime, mtime, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        debug!("readlink: {:?}", path);

        let real = self.real_path(path);
        match ::std::fs::read_link(real) {
            Ok(target) => Ok(target.into_os_string().into_vec()),
            Err(e) => Err(e.raw_os_error().unwrap()),
        }
    }
}

/// A file that is not closed upon leaving scope.
struct UnmanagedFile {
    inner: Option<File>,
}

impl UnmanagedFile {
    unsafe fn new(fd: u64) -> UnmanagedFile {
        UnmanagedFile {
            inner: Some(File::from_raw_fd(fd as i32))
        }
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_all()
    }
    fn sync_data(&self) -> io::Result<()> {
        self.inner.as_ref().unwrap().sync_data()
    }
}

impl Drop for UnmanagedFile {
    fn drop(&mut self) {
        // Release control of the file descriptor so it is not closed.
        let file = self.inner.take().unwrap();
        file.into_raw_fd();
    }
}

impl Read for UnmanagedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read(buf)
    }
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.inner.as_ref().unwrap().read_to_end(buf)
    }
}

impl Write for UnmanagedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.as_ref().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.as_ref().unwrap().flush()
    }
}

impl Seek for UnmanagedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.as_ref().unwrap().seek(pos)
    }
}
