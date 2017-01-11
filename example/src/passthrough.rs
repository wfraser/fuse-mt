// PassthroughFS :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using fuse_mt::FilesystemMT.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};

use super::libc_extras::libc;
use super::libc_wrappers;

use fuse_mt::*;
use time::*;

pub struct PassthroughFS {
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
            warn!("FUSE doesn't yet support Socket file type; translating to NamedPipe instead.");
            FileType::NamedPipe
        },
        // This is committed to rust-fuse but not yet released.
        // See: https://github.com/zargony/rust-fuse/pull/79
        //libc::S_IFSOCK => FileType::Socket,
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

#[cfg(target_os = "macos")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks,
        bfree: statfs.f_bfree,
        bavail: statfs.f_bavail,
        files: statfs.f_files,
        ffree: statfs.f_ffree,
        bsize: statfs.f_bsize as u32,
        namelen: 0, // TODO
        frsize: 0, // TODO
    }
}

#[cfg(target_os = "linux")]
fn statfs_to_fuse(statfs: libc::statfs) -> Statfs {
    Statfs {
        blocks: statfs.f_blocks as u64,
        bfree: statfs.f_bfree as u64,
        bavail: statfs.f_bavail as u64,
        files: statfs.f_files as u64,
        ffree: statfs.f_ffree as u64,
        bsize: statfs.f_bsize as u32,
        namelen: statfs.f_namelen as u32,
        frsize: statfs.f_frsize as u32,
    }
}

impl PassthroughFS {
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

impl FilesystemMT for PassthroughFS {
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

    fn lookup(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEntry {
        debug!("lookup: {:?}/{:?}", parent, name);

        let path = PathBuf::from(parent).join(name);
        match self.stat_real(&path) {
            Ok(attr) => Ok((TTL, attr, 0)),
            Err(e) => {
                error!("stat_real({:?}): {}", path, e);
                Err(e.raw_os_error().unwrap())
            },
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
        let real = self.real_path(path);
        debug!("opendir: {:?} (flags = {:#o})", real, _flags);
        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh, 0)),
            Err(e) => {
                let ioerr = io::Error::from_raw_os_error(e);
                error!("opendir({:?}): {}", path, ioerr);
                Err(e)
            }
        }
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        libc_wrappers::closedir(fh)
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
        debug!("readdir: {:?}", path);
        let mut entries: Vec<DirectoryEntry> = vec![];

        if fh == 0 {
            error!("readdir: missing fh");
            return Err(libc::EINVAL);
        }

        loop {
            match libc_wrappers::readdir(fh) {
                Ok(Some(entry)) => {
                    let name_c = unsafe { CStr::from_ptr(entry.d_name.as_ptr()) };
                    let name = OsStr::from_bytes(name_c.to_bytes()).to_owned();

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
                            let entry_path = PathBuf::from(path).join(&name);
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
                        name: name,
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

    fn write(&self, _req: RequestInfo, path: &Path, fh: u64, offset: u64, data: Vec<u8>, _flags: u32) -> ResultWrite {
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let mut file = unsafe { UnmanagedFile::new(fh) };

        if let Err(e) = file.seek(SeekFrom::Start(offset)) {
            error!("seek({:?}, {}): {}", path, offset, e);
            return Err(e.raw_os_error().unwrap());
        }
        let nwritten: u32 = match file.write(&data) {
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
                    tv_sec: time.sec as libc::time_t,
                    tv_nsec: time.nsec as libc::time_t,
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
                libc::utimensat(libc::AT_FDCWD, path_c.as_ptr(), &times as *const libc::timespec, libc::AT_SYMLINK_NOFOLLOW)
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

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        debug!("statfs: {:?}", path);

        let real = self.real_path(path);
        let mut buf: libc::statfs = unsafe { ::std::mem::zeroed() };
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.into_vec());
            libc::statfs(path_c.as_ptr(), &mut buf)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("statfs({:?}): {}", path, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(statfs_to_fuse(buf))
        }
    }

    fn fsyncdir(&self, _req: RequestInfo, path: &Path, fh: u64, datasync: bool) -> ResultEmpty {
        debug!("fsyncdir: {:?} (datasync = {:?})", path, datasync);

        // TODO: what does datasync mean with regards to a directory handle?
        let result = unsafe { libc::fsync(fh as libc::c_int) };
        if -1 == result {
            let e = io::Error::last_os_error();
            error!("fsyncdir({:?}): {}", path, e);
            Err(e.raw_os_error().unwrap())
        } else {
            Ok(())
        }
    }

    fn mknod(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, mode: u32, rdev: u32) -> ResultEntry {
        debug!("mknod: {:?}/{:?} (mode={:#o}, rdev={})", parent_path, name, mode, rdev);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mknod(path_c.as_ptr(), mode as libc::mode_t, rdev as libc::dev_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mknod({:?}, {}, {}): {}", real, mode, rdev, e);
            Err(e.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr), 0)),
                Err(e) => Err(e),   // if this happens, yikes
            }
        }
    }

    fn mkdir(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        debug!("mkdir {:?}/{:?} (mode={:#o})", parent_path, name, mode);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let result = unsafe {
            let path_c = CString::from_vec_unchecked(real.as_os_str().as_bytes().to_vec());
            libc::mkdir(path_c.as_ptr(), mode as libc::mode_t)
        };

        if -1 == result {
            let e = io::Error::last_os_error();
            error!("mkdir({:?}, {:#o}): {}", real, mode, e);
            Err(e.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok((TTL, stat_to_fuse(attr), 0)),
                Err(e) => {
                    error!("lstat after mkdir({:?}, {:#o}): {}", real, mode, e);
                    Err(e)   // if this happens, yikes
                },
            }
        }
    }

    fn unlink(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("unlink {:?}/{:?}", parent_path, name);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        fs::remove_file(&real)
            .map_err(|ioerr| {
                error!("unlink({:?}): {}", real, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn rmdir(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("rmdir: {:?}/{:?}", parent_path, name);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        fs::remove_dir(&real)
            .map_err(|ioerr| {
                error!("rmdir({:?}): {}", real, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn symlink(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, target: &Path) -> ResultEntry {
        debug!("symlink: {:?}/{:?} -> {:?}", parent_path, name, target);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        match ::std::os::unix::fs::symlink(target, &real) {
            Ok(()) => {
                match libc_wrappers::lstat(real.clone().into_os_string()) {
                    Ok(attr) => Ok((TTL, stat_to_fuse(attr), 0)),
                    Err(e) => {
                        error!("lstat after symlink({:?}, {:?}): {}", real, target, e);
                        Err(e)
                    },
                }
            },
            Err(e) => {
                error!("symlink({:?}, {:?}): {}", real, target, e);
                Err(e.raw_os_error().unwrap())
            }
        }
    }

    fn rename(&self, _req: RequestInfo, parent_path: &Path, name: &OsStr, newparent_path: &Path, newname: &OsStr) -> ResultEmpty {
        debug!("rename: {:?}/{:?} -> {:?}/{:?}", parent_path, name, newparent_path, newname);

        let real = PathBuf::from(self.real_path(parent_path)).join(name);
        let newreal = PathBuf::from(self.real_path(newparent_path)).join(newname);
        fs::rename(&real, &newreal)
            .map_err(|ioerr| {
                error!("rename({:?}, {:?}): {}", real, newreal, ioerr);
                ioerr.raw_os_error().unwrap()
            })
    }

    fn link(&self, _req: RequestInfo, path: &Path, newparent: &Path, newname: &OsStr) -> ResultEntry {
        debug!("link: {:?} -> {:?}/{:?}", path, newparent, newname);

        let real = self.real_path(path);
        let newreal = PathBuf::from(self.real_path(newparent)).join(newname);
        match fs::hard_link(&real, &newreal) {
            Ok(()) => {
                match libc_wrappers::lstat(real.clone()) {
                    Ok(attr) => Ok((TTL, stat_to_fuse(attr), 0)),
                    Err(e) => {
                        error!("lstat after link({:?}, {:?}): {}", real, newreal, e);
                        Err(e)
                    },
                }
            },
            Err(e) => {
                error!("link({:?}, {:?}): {}", real, newreal, e);
                Err(e.raw_os_error().unwrap())
            },
        }
    }

    fn create(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32, flags: u32) -> ResultCreate {
        debug!("create: {:?}/{:?} (mode={:#o}, flags={:#x})", parent, name, mode, flags);

        let real = PathBuf::from(self.real_path(parent)).join(name);
        let fd = unsafe {
            let real_c = CString::from_vec_unchecked(real.clone().into_os_string().into_vec());
            libc::open(real_c.as_ptr(), flags as i32 | libc::O_CREAT | libc::O_EXCL, mode)
        };

        if -1 == fd {
            let ioerr = io::Error::last_os_error();
            error!("create({:?}): {}", real, ioerr);
            Err(ioerr.raw_os_error().unwrap())
        } else {
            match libc_wrappers::lstat(real.clone().into_os_string()) {
                Ok(attr) => Ok(CreatedEntry {
                    ttl: TTL,
                    attr: stat_to_fuse(attr),
                    generation: 0,
                    fh: fd as u64,
                    flags: flags,
                }),
                Err(e) => {
                    error!("lstat after create({:?}): {}", real, io::Error::from_raw_os_error(e));
                    Err(e)
                },
            }

        }
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        debug!("listxattr: {:?}", path);

        let real = self.real_path(path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            unsafe { data.set_len(size as usize) };
            let nread = try!(libc_wrappers::llistxattr(real, data.as_mut_slice()));
            data.truncate(nread);
            Ok(Xattr::Data(data))
        } else {
            let nbytes = try!(libc_wrappers::llistxattr(real, &mut[]));
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        debug!("getxattr: {:?} {:?} {}", path, name, size);

        let real = self.real_path(path);

        if size > 0 {
            let mut data = Vec::<u8>::with_capacity(size as usize);
            unsafe { data.set_len(size as usize) };
            let nread = try!(libc_wrappers::lgetxattr(real, name.to_owned(), data.as_mut_slice()));
            data.truncate(nread);
            Ok(Xattr::Data(data))
        } else {
            let nbytes = try!(libc_wrappers::lgetxattr(real, name.to_owned(), &mut []));
            Ok(Xattr::Size(nbytes as u32))
        }
    }

    fn setxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, value: &[u8], flags: u32, position: u32) -> ResultEmpty {
        debug!("setxattr: {:?} {:?} {} bytes, flags = {:#x}, pos = {}", path, name, value.len(), flags, position);
        let real = self.real_path(path);
        libc_wrappers::lsetxattr(real, name.to_owned(), value, flags, position)
    }

    fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
        debug!("removexattr: {:?} {:?}", path, name);
        let real = self.real_path(path);
        libc_wrappers::lremovexattr(real, name.to_owned())
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
