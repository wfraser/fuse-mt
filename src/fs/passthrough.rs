// Passthrough :: A filesystem that passes all calls through to another underlying filesystem.
//
// Implemented using the PathFilesystem wrapper over the FUSE Filesystem trait.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CStr, OsStr, OsString};
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
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

                let mut mode = stat.st_mode & 0o7777; // st_mode encodes the type AND the mode.
                /*
                    mode &= !0o222; // disable the write bits if we're not in RW mode.
                */

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

    fn opendir(&mut self, _req: &Request, path: &Path, _flags: u32) -> ResultOpendir {
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
}
