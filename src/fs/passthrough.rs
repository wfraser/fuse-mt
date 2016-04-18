// Passthrough :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using the PathFilesystem wrapper over the FUSE Filesystem trait.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CStr, OsStr, OsString};
use std::fs::File;
use std::io::{self, Read, Write, Seek, SeekFrom};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::{Path, PathBuf};

use super::inode_translator::*;
use super::super::libc_wrappers;

use fuse::*;
use libc;
use time::*;

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

impl Passthrough {
    fn real_path(&self, partial: &Path) -> OsString {
        PathBuf::from(&self.target)
                .join(partial.strip_prefix("/").unwrap())
                .into_os_string()
    }

    fn stat_real(&mut self, path: &Path) -> io::Result<FileAttr> {
        let real: OsString = self.real_path(path);
        debug!("stat_real: {:?}", real);

        match libc_wrappers::lstat(real) {
            Ok(stat) => {
                let kind = mode_to_filetype(stat.st_mode);

                let mode = stat.st_mode & 0o7777; // st_mode encodes the type AND the mode.

                Ok(FileAttr {
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
                })
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
    fn init(&mut self, _req: &Request) -> ResultEmpty {
        debug!("init");
        Ok(())
    }

    fn destroy(&mut self, _req: &Request) {
        debug!("destroy");
    }

    fn getattr(&mut self, _req: &Request, path: &Path) -> ResultGetattr {
        debug!("getattr: {:?}", path);

        match self.stat_real(path) {
            Ok(attr) => Ok((TTL, attr)),
            Err(e) => Err(e.raw_os_error().unwrap())
        }
    }

    fn lookup(&mut self, _req: &Request, parent: &Path, name: &Path) -> ResultLookup {
        debug!("lookup: {:?}/{:?}", parent, name);

        let path = PathBuf::from(parent).join(name);
        match self.stat_real(&path) {
            Ok(attr) => Ok((TTL, attr, 0)),
            Err(e) => Err(e.raw_os_error().unwrap()),
        }
    }

    fn opendir(&mut self, _req: &Request, path: &Path, _flags: u32) -> ResultOpen {
        let real = self.real_path(path);
        debug!("opendir: {:?}", real);
        match libc_wrappers::opendir(real) {
            Ok(fh) => Ok((fh as u64, 0)),
            Err(e) => Err(e)
        }
    }

    fn releasedir(&mut self, _req: &Request, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
        debug!("releasedir: {:?}", path);
        libc_wrappers::closedir(fh as usize)
    }

    fn readdir(&mut self, _req: &Request, path: &Path, fh: u64, offset: u64) -> ResultReaddir {
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
            match libc_wrappers::readdir(fh as usize) {
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

    fn open(&mut self, _req: &Request, path: &Path, flags: u32) -> ResultOpen {
        debug!("open: {:?} flags={:#x}", path, flags);

        let real = self.real_path(path);
        match libc_wrappers::open(real, flags as libc::c_int) {
            Ok(fh) => Ok((fh as u64, flags)),
            Err(e) => {
                error!("open({:?}): {}", path, io::Error::from_raw_os_error(e));
                Err(e)
            }
        }
    }

    fn release(&mut self, _req: &Request, path: &Path, fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        debug!("release: {:?}", path);
        libc_wrappers::close(fh as usize)
    }

    fn read(&mut self, _req: &Request, path: &Path, fh: u64, offset: u64, size: u32) -> ResultData {
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let mut file = unsafe { File::from_raw_fd(fh as libc::c_int) };

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

        // Release control of the file descriptor so it is not closed when this function returns.
        file.into_raw_fd();

        Ok(data)
    }

    fn write(&mut self, _req: &Request, path: &Path, fh: u64, offset: u64, data: &[u8], _flags: u32) -> ResultWrite {
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let mut file = unsafe { File::from_raw_fd(fh as libc::c_int) };

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

        // Release control of the file descriptor so it is not closed when this function returns.
        file.into_raw_fd();

        Ok(nwritten)
    }

    fn flush(&mut self, _req: &Request, path: &Path, fh: u64, _lock_owner: u64) -> ResultEmpty {
        debug!("flush: {:?}", path);
        let mut file = unsafe { File::from_raw_fd(fh as libc::c_int) };

        if let Err(e) = file.flush() {
            error!("flush({:?}): {}", path, e);
            return Err(e.raw_os_error().unwrap());
        }

        // Release control of the file descriptor so it is not closed when this function returns.
        file.into_raw_fd();

        Ok(())
    }
}
