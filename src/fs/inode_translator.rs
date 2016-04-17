use std::path::{Path, PathBuf};
use std::rc::Rc;

use fuse::*;
use libc;
use time;

use super::inode_table::*;

pub struct DirectoryEntry {
    pub name: PathBuf,
    pub kind: FileType,
}

pub type ResultEmpty = Result<(), libc::c_int>;
pub type ResultGetattr = Result<(time::Timespec, FileAttr), libc::c_int>;
pub type ResultLookup = Result<(time::Timespec, FileAttr, u64), libc::c_int>;
pub type ResultOpendir = Result<(u64, u32), libc::c_int>;
pub type ResultReaddir = Result<Vec<DirectoryEntry>, libc::c_int>;

pub trait PathFilesystem {
    fn init(&mut self, _req: &Request) -> ResultEmpty {
        Err(0)
    }

    fn destroy(&mut self, _req: &Request) {
        // Nothing.
    }

    fn getattr(&mut self, _req: &Request, _path: &Path) -> ResultGetattr {
        Err(libc::ENOSYS)
    }

    fn lookup(&mut self, _req: &Request, _parent: &Path, _name: &Path) -> ResultLookup {
        Err(libc::ENOSYS)
    }

    fn opendir(&mut self, _req: &Request, _path: &Path, _flags: u32) -> ResultOpendir {
        Err(libc::ENOSYS)
    }

    fn releasedir(&mut self, _req: &Request, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn readdir(&mut self, _req: &Request, _path: &Path, _fh: u64, _offset: u64) -> ResultReaddir {
        Err(libc::ENOSYS)
    }
}

pub struct InodeTranslator<T> {
    target: T,
    inodes: InodeTable,
}

impl<T: PathFilesystem> InodeTranslator<T> {
    pub fn new(target_fs: T) -> InodeTranslator<T> {
        let mut translator = InodeTranslator {
            target: target_fs,
            inodes: InodeTable::new()
        };
        translator.inodes.add(Rc::new(PathBuf::from("/")));
        translator
    }
}

impl<T: PathFilesystem> Filesystem for InodeTranslator<T> {
    fn init(&mut self, req: &Request) -> Result<(), libc::c_int> {
        debug!("init");
        self.target.init(req)
    }

    fn destroy(&mut self, req: &Request) {
        debug!("destroy");
        self.target.destroy(req);
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        if let Some(path) = self.inodes.get_path(ino) {
            debug!("getattr: {:?}", path);
            match self.target.getattr(req, &path) {
                Ok((ref ttl, ref attr)) => reply.attr(ttl, attr),
                Err(e) => reply.error(e),
            }
        } else {
            reply.error(libc::EINVAL);
        }
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        if let Some(parent_path) = self.inodes.get_path(parent) {
            debug!("lookup: {:?}, {:?}", parent_path, name);
            let path = Rc::new((*parent_path).clone().join(name));
            match self.target.lookup(req, Path::new(&*parent_path), name) {
                Ok((ref ttl, ref mut attr, generation)) => {
                    let ino = self.inodes.add_or_get(path.clone());
                    attr.ino = ino;
                    reply.entry(ttl, attr, generation);
                },
                Err(e) => reply.error(e),
            }
        } else {
            reply.error(libc::EINVAL);
        }
    }

    fn opendir(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        if let Some(path) = self.inodes.get_path(ino) {
            debug!("opendir: {:?}", path);
            match self.target.opendir(req, &path, flags) {
                Ok((fh, flags)) => reply.opened(fh, flags),
                Err(e) => reply.error(e),
            }
        } else {
            reply.error(libc::EINVAL);
        }
    }

    fn releasedir(&mut self, req: &Request, ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        if let Some(path) = self.inodes.get_path(ino) {
            debug!("releasedir: {:?}", path);
            match self.target.releasedir(req, &path, fh, flags) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        } else {
            reply.error(libc::EINVAL);
        }
    }

    fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if let Some(path) = self.inodes.get_path(ino) {
            debug!("readdir: {:?} @ {}", path, offset);
            match self.target.readdir(req, &path, fh, offset) {
                Ok(entries) => {
                    let parent_inode = if ino == 1 {
                        ino
                    } else {
                        let parent_path: &Path = path.parent().unwrap();
                        match self.inodes.get_inode(parent_path) {
                            Some(inode) => inode,
                            None => {
                                error!("readdir: unable to get inode for parent of {:?}", path);
                                reply.error(libc::EIO);
                                return;
                            }
                        }
                    };

                    let mut index = 0;
                    for entry in entries {
                        let entry_inode = if entry.name == Path::new(".") {
                            ino
                        } else if entry.name == Path::new("..") {
                            parent_inode
                        } else {
                            let path = Rc::new(path.clone().join(&entry.name));
                            self.inodes.add_or_get(path)
                        };

                        let buffer_full: bool = reply.add(
                            entry_inode,
                            index,
                            entry.kind,
                            entry.name.as_os_str());

                        if buffer_full {
                            debug!("readdir: reply buffer is full");
                            break;
                        }

                        index += 1;
                    }

                    reply.ok();
                },
                Err(e) => reply.error(e),
            }
        } else {
            reply.error(libc::EINVAL);
        }
    }
}
