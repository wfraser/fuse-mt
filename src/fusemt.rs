// FuseMT :: A wrapper around FUSE that presents paths instead of inodes and dispatches I/O
//           operations to multiple threads.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fuse::*;
use libc;
use threadpool::ThreadPool;
use time::Timespec;

use inode_table::*;

pub struct RequestInfo {
    pub unique: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

trait IntoRequestInfo {
    fn info(&self) -> RequestInfo;
}

impl<'a> IntoRequestInfo for Request<'a> {
    fn info(&self) -> RequestInfo {
        RequestInfo {
            unique: self.unique(),
            uid: self.uid(),
            gid: self.gid(),
            pid: self.pid(),
        }
    }
}

pub struct DirectoryEntry {
    pub name: PathBuf,
    pub kind: FileType,
}

pub struct Statfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

pub struct CreatedEntry {
    pub ttl: Timespec,
    pub attr: FileAttr,
    pub generation: u64,
    pub fh: u64,
    pub flags: u32,
}

pub enum Xattr {
    Size(u32),
    Data(Vec<u8>),
}

pub type ResultEmpty = Result<(), libc::c_int>;
pub type ResultGetattr = Result<(Timespec, FileAttr), libc::c_int>;
pub type ResultEntry = Result<(Timespec, FileAttr, u64), libc::c_int>;
pub type ResultOpen = Result<(u64, u32), libc::c_int>;
pub type ResultReaddir = Result<Vec<DirectoryEntry>, libc::c_int>;
pub type ResultData = Result<Vec<u8>, libc::c_int>;
pub type ResultWrite = Result<u32, libc::c_int>;
pub type ResultStatfs = Result<Statfs, libc::c_int>;
pub type ResultCreate = Result<CreatedEntry, libc::c_int>;
pub type ResultXattr = Result<Xattr, libc::c_int>;

pub trait FilesystemMT {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Err(0)
    }

    fn destroy(&self, _req: RequestInfo) {
        // Nothing.
    }

    fn lookup(&self, _req: RequestInfo, _parent: &Path, _name: &Path) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    fn getattr(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>) -> ResultGetattr {
        Err(libc::ENOSYS)
    }

    // The following operations in the FUSE C API are all one kernel call: setattr
    // We split them out to match the C API's behavior.

    fn chmod(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _mode: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn chown(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn truncate(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _size: u64) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn utimens(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn utimens_macos(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // END OF SETATTR FUNCTIONS

    fn readlink(&self, _req: RequestInfo, _path: &Path) -> ResultData {
        Err(libc::ENOSYS)
    }

    fn mknod(&self, _req: RequestInfo, _parent: &Path, _name: &Path, _mode: u32, _rdev: u32) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    fn mkdir(&self, _req: RequestInfo, _parent: &Path, _name: &Path, _mode: u32) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    fn unlink(&self, _req: RequestInfo, _parent: &Path, _name: &Path) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn rmdir(&self, _req: RequestInfo, _parent: &Path, _name: &Path) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn symlink(&self, _req: RequestInfo, _parent: &Path, _name: &Path, _target: &Path) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    fn rename(&self, _req: RequestInfo, _parent: &Path, _name: &Path, _newparent: &Path, _newname: &Path) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn link(&self, _req: RequestInfo, _path: &Path, _newparent: &Path, _newname: &Path) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    fn open(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Err(libc::ENOSYS)
    }

    fn read(&self, _req: RequestInfo, _path: &Path, _fh: u64, _offset: u64, _size: u32) -> ResultData {
        Err(libc::ENOSYS)
    }

    fn write(&self, _req: RequestInfo, _path: &Path, _fh: u64, _offset: u64, _data: &[u8], _flags: u32) -> ResultWrite {
        Err(libc::ENOSYS)
    }

    fn flush(&self, _req: RequestInfo, _path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn release(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn fsync(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn opendir(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Err(libc::ENOSYS)
    }

    fn readdir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _offset: u64) -> ResultReaddir {
        Err(libc::ENOSYS)
    }

    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn fsyncdir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
        Err(libc::ENOSYS)
    }

    fn setxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _value: &[u8], _flags: u32, _position: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    fn getxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _size: u32) -> ResultXattr {
        Err(libc::ENOSYS)
    }

    fn listxattr(&self, _req: RequestInfo, _path: &Path, _size: u32) -> ResultXattr {
        Err(libc::ENOSYS)
    }

    fn removexattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // access

    fn create(&self, _req: RequestInfo, _parent: &Path, _name: &Path, _mode: u32, _flags: u32) -> ResultCreate {
        Err(libc::ENOSYS)
    }

    // getlk

    // setlk

    // bmap
}

pub struct FuseMT<T> {
    target: Arc<T>,
    inodes: InodeTable,
    threads: ThreadPool,
}

impl<T: FilesystemMT + Sync + Send + 'static> FuseMT<T> {
    pub fn new(target_fs: T, num_threads: usize) -> FuseMT<T> {
        FuseMT {
            target: Arc::new(target_fs),
            inodes: InodeTable::new(),
            threads: ThreadPool::new(num_threads),
        }
    }
}

macro_rules! get_path {
    ($s:expr, $ino:expr, $reply:expr) => {
        if let Some(path) = $s.inodes.get_path($ino) {
            path
        } else {
            $reply.error(libc::EINVAL);
            return;
        }
    }
}

impl<T: FilesystemMT + Sync + Send + 'static> Filesystem for FuseMT<T> {
    fn init(&mut self, req: &Request) -> Result<(), libc::c_int> {
        debug!("init");
        self.target.init(req.info())
    }

    fn destroy(&mut self, req: &Request) {
        debug!("destroy");
        self.target.destroy(req.info());
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("lookup: {:?}, {:?}", parent_path, name);
        let path = Arc::new((*parent_path).clone().join(name));
        match self.target.lookup(req.info(), Path::new(&*parent_path), name) {
            Ok((ref ttl, ref mut attr, generation)) => {
                let ino = self.inodes.add_or_get(path.clone());
                self.inodes.lookup(ino);
                attr.ino = ino;
                reply.entry(ttl, attr, generation);
            },
            Err(e) => reply.error(e),
        }
    }

    fn forget(&mut self, _req: &Request, ino: u64, nlookup: u64) {
        let path = self.inodes.get_path(ino).unwrap();
        let lookups = self.inodes.forget(ino, nlookup);
        debug!("forget: inode {} ({:?}) now at {} lookups", ino, path, lookups);
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        let path = get_path!(self, ino, reply);
        debug!("getattr: {:?}", path);
        match self.target.getattr(req.info(), &path, None) {
            Ok((ref ttl, ref attr)) => reply.attr(ttl, attr),
            Err(e) => reply.error(e),
        }
    }

    fn setattr(&mut self,
               req: &Request,               // passed to all
               ino: u64,                    // translated to path; passed to all
               mode: Option<u32>,           // chmod
               uid: Option<u32>,            // chown
               gid: Option<u32>,            // chown
               size: Option<u64>,           // truncate
               atime: Option<Timespec>,     // utimens
               mtime: Option<Timespec>,     // utimens
               fh: Option<u64>,             // passed to all
               crtime: Option<Timespec>,    // utimens_osx  (OS X only)
               chgtime: Option<Timespec>,   // utimens_osx  (OS X only)
               bkuptime: Option<Timespec>,  // utimens_osx  (OS X only)
               flags: Option<u32>,          // utimens_osx  (OS X only)
               reply: ReplyAttr) {
        let path = get_path!(self, ino, reply);
        debug!("setattr: {:?}", path);

        debug!("\tino:\t{:?}", ino);
        debug!("\tmode:\t{:?}", mode);
        debug!("\tuid:\t{:?}", uid);
        debug!("\tgid:\t{:?}", gid);
        debug!("\tsize:\t{:?}", size);
        debug!("\tatime:\t{:?}", atime);
        debug!("\tmtime:\t{:?}", mtime);
        debug!("\tfh:\t{:?}", fh);

        // TODO: figure out what C FUSE does when only some of these are implemented.

        if mode.is_some() {
            if let Err(e) = self.target.chmod(req.info(), &path, fh, mode.unwrap()) {
                reply.error(e);
                return;
            }
        }

        if uid.is_some() || gid.is_some() {
            if let Err(e) = self.target.chown(req.info(), &path, fh, uid, gid) {
                reply.error(e);
                return;
            }
        }

        if size.is_some() {
            if let Err(e) = self.target.truncate(req.info(), &path, fh, size.unwrap()) {
                reply.error(e);
                return;
            }
        }

        if atime.is_some() || mtime.is_some() {
            if let Err(e) = self.target.utimens(req.info(), &path, fh, atime, mtime) {
                reply.error(e);
                return;
            }
        }

        if crtime.is_some() || chgtime.is_some() || bkuptime.is_some() || flags.is_some() {
            if let Err(e) = self.target.utimens_macos(req.info(), &path, fh, crtime, chgtime, bkuptime, flags) {
                reply.error(e);
                return
            }
        }

        match self.target.getattr(req.info(), &path, fh) {
            Ok((ref ttl, ref attr)) => reply.attr(ttl, attr),
            Err(e) => reply.error(e),
        }
   }

    fn readlink(&mut self, req: &Request, ino: u64, reply: ReplyData) {
        let path = get_path!(self, ino, reply);
        debug!("readlink: {:?}", path);
        match self.target.readlink(req.info(), &path) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn mknod(&mut self, req: &Request, parent: u64, name: &Path, mode: u32, rdev: u32, reply: ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mknod: {:?}/{:?}", parent_path, name);
        match self.target.mknod(req.info(), &parent_path, name, mode, rdev) {
            Ok((ref ttl, ref attr, generation)) => reply.entry(ttl, attr, generation),
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &Path, mode: u32, reply: ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mkdir: {:?}/{:?}", parent_path, name);
        match self.target.mkdir(req.info(), &parent_path, name, mode) {
            Ok((ref ttl, ref mut attr, generation)) => {
                let ino = self.inodes.add(Arc::new(parent_path.join(name)));
                attr.ino = ino;
                reply.entry(ttl, attr, generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &Path, reply: ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        debug!("unlink: {:?}/{:?}", parent_path, name);
        match self.target.unlink(req.info(), &parent_path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &Path, reply: ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        debug!("rmdir: {:?}/{:?}", parent_path, name);
        match self.target.rmdir(req.info(), &parent_path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&mut self, req: &Request, parent: u64, name: &Path, link: &Path, reply: ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("symlink: {:?}/{:?} -> {:?}", parent_path, name, link);
        match self.target.symlink(req.info(), &parent_path, name, link) {
            Ok((ref ttl, ref mut attr, generation)) => {
                let ino = self.inodes.add(Arc::new(parent_path.join(name)));
                attr.ino = ino;
                reply.entry(ttl, attr, generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn rename(&mut self, req: &Request, parent: u64, name: &Path, newparent: u64, newname: &Path, reply: ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        let newparent_path = get_path!(self, newparent, reply);
        debug!("rename: {:?}/{:?} -> {:?}/{:?}", parent_path, name, newparent_path, newname);
        match self.target.rename(req.info(), &parent_path, name, &newparent_path, newname) {
            Ok(()) => {
                self.inodes.rename(&parent_path.join(name), Arc::new(newparent_path.join(newname)));
                reply.ok()
            },
            Err(e) => reply.error(e),
        }
    }

    fn link(&mut self, req: &Request, ino: u64, newparent: u64, newname: &Path, reply: ReplyEntry) {
        let path = get_path!(self, ino, reply);
        let newparent_path = get_path!(self, newparent, reply);
        debug!("link: {:?} -> {:?}/{:?}", path, newparent_path, newname);
        match self.target.link(req.info(), &path, &newparent_path, newname) {
            Ok((ref ttl, ref mut attr, generation)) => {
                // NOTE: this results in the new link having a different inode from the original.
                // This is needed because our inode table is a 1:1 map between paths and inodes.
                let new_ino = self.inodes.add(Arc::new(newparent_path.join(newname)));
                attr.ino = new_ino;
                reply.entry(ttl, attr, generation);
            },
            Err(e) => reply.error(e),
        }
    }

    fn open(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let path = get_path!(self, ino, reply);
        debug!("open: {:?}", path);
        match self.target.open(req.info(), &path, flags) {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(e) => reply.error(e),
        }
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: u64, size: u32, reply: ReplyData) {
        let path = get_path!(self, ino, reply);
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let target = self.target.clone();
        let req_info = req.info();
        self.threads.execute(move|| {
            match target.read(req_info, &path, fh, offset, size) {
                Ok(ref data) => reply.data(data),
                Err(e) => reply.error(e),
            }
        });
    }

    fn write(&mut self, req: &Request, ino: u64, fh: u64, offset: u64, data: &[u8], flags: u32, reply: ReplyWrite) {
        let path = get_path!(self, ino, reply);
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let target = self.target.clone();
        let req_info = req.info();

        // TODO: it would be better if rust-fuse gave us the buffer by value so we could avoid this copy
        let data_buf = Vec::from(data);

        self.threads.execute(move|| {
            match target.write(req_info, &path, fh, offset, &data_buf, flags) {
                Ok(written) => reply.written(written),
                Err(e) => reply.error(e),
            }
        });
    }

    fn flush(&mut self, req: &Request, ino: u64, fh: u64, lock_owner: u64, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("flush: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threads.execute(move|| {
            match target.flush(req_info, &path, fh, lock_owner) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    fn release(&mut self, req: &Request, ino: u64, fh: u64, flags: u32, lock_owner: u64, flush: bool, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("release: {:?}", path);
        match self.target.release(req.info(), &path, fh, flags, lock_owner, flush) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn fsync(&mut self, req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("fsync: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threads.execute(move|| {
            match target.fsync(req_info, &path, fh, datasync) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    fn opendir(&mut self, req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let path = get_path!(self, ino, reply);
        debug!("opendir: {:?}", path);
        match self.target.opendir(req.info(), &path, flags) {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(e) => reply.error(e),
        }
    }

    fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: u64, mut reply: ReplyDirectory) {
        let path = get_path!(self, ino, reply);
        debug!("readdir: {:?} @ {}", path, offset);
        match self.target.readdir(req.info(), &path, fh, offset) {
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
                        let path = Arc::new(path.clone().join(&entry.name));
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
    }

    fn releasedir(&mut self, req: &Request, ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("releasedir: {:?}", path);
        match self.target.releasedir(req.info(), &path, fh, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn fsyncdir(&mut self, req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);
        match self.target.fsyncdir(req.info(), &path, fh, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn statfs(&mut self, req: &Request, ino: u64, reply: ReplyStatfs) {
        let path = if ino == 1 {
            Arc::new(PathBuf::from("/"))
        } else {
            get_path!(self, ino, reply)
        };

        debug!("statfs: {:?}", path);
        match self.target.statfs(req.info(), &path) {
            Ok(statfs) => reply.statfs(statfs.blocks,
                                       statfs.bfree,
                                       statfs.bavail,
                                       statfs.files,
                                       statfs.ffree,
                                       statfs.bsize,
                                       statfs.namelen,
                                       statfs.frsize),
            Err(e) => reply.error(e),
        }
    }

    // setxattr

    fn getxattr(&mut self, req: &Request, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
        let path = if ino == 1 {
            Arc::new(PathBuf::from("/"))
        } else {
            get_path!(self, ino, reply)
        };

        debug!("getxattr: {:?} {:?}", path, name);
        match self.target.getxattr(req.info(), &path, name, size) {
            Ok(Xattr::Size(size)) => {
                debug!("getxattr: sending size {}", size);
                reply.size(size)
            },
            Ok(Xattr::Data(vec)) => {
                debug!("getxattr: sending {} bytes", vec.len());
                reply.data(&vec)
            },
            Err(e) => {
                debug!("getxattr: error {}", e);
                reply.error(e)
            },
        }
    }

    fn listxattr(&mut self, req: &Request, ino: u64, size: u32, reply: ReplyXattr) {
        let path = if ino == 1 {
            Arc::new(PathBuf::from("/"))
        } else {
            get_path!(self, ino, reply)
        };

        debug!("listxattr: {:?}", path);
        match self.target.listxattr(req.info(), &path, size) {
            Ok(Xattr::Size(size)) => {
                debug!("listxattr: sending size {}", size);
                reply.size(size)
            },
            Ok(Xattr::Data(vec)) => {
                debug!("listxattr: sending {} bytes", vec.len());
                reply.data(&vec)
            }
            Err(e) => reply.error(e),
        }
    }

    // removexattr

    // access

    fn create(&mut self, req: &Request, parent: u64, name: &Path, mode: u32, flags: u32, reply: ReplyCreate) {
        let parent_path = get_path!(self, parent, reply);
        debug!("create: {:?}/{:?} (mode={:#o}, flags={:#x})", parent_path, name, mode, flags);
        match self.target.create(req.info(), &parent_path, name, mode, flags) {
            Ok(mut create) => {
                let ino = self.inodes.add(Arc::new(parent_path.join(name)));
                create.attr.ino = ino;
                reply.created(&create.ttl, &create.attr, create.generation, create.fh, create.flags);
            },
            Err(e) => reply.error(e),
        }
    }

    // getlk

    // setlk

    // bmap
}
