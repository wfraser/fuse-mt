// FuseMT :: A wrapper around FUSE that presents paths instead of inodes and dispatches I/O
//           operations to multiple threads.
//
// Copyright (c) 2016-2026 by William R. Fraser
//

use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

use fuser::{AccessFlags, BsdFileFlags, Errno, FileHandle, FopenFlags, INodeNo, LockOwner, OpenFlags, RenameFlags, TimeOrNow, WriteFlags};
use threadpool::ThreadPool;

use crate::directory_cache::*;
use crate::inode_table::*;
use crate::types::*;

trait IntoRequestInfo {
    fn info(&self) -> RequestInfo;
}

impl IntoRequestInfo for fuser::Request {
    fn info(&self) -> RequestInfo {
        RequestInfo {
            unique: self.unique().0,
            uid: self.uid(),
            gid: self.gid(),
            pid: self.pid(),
        }
    }
}

fn fuse_fileattr(attr: FileAttr, ino: INodeNo) -> fuser::FileAttr {
    fuser::FileAttr {
        ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: attr.kind,
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: 4096, // TODO
        flags: attr.flags,
    }
}

trait TimeOrNowExt {
    fn time(self) -> SystemTime;
}

impl TimeOrNowExt for TimeOrNow {
    fn time(self) -> SystemTime {
        match self {
            TimeOrNow::SpecificTime(t) => t,
            TimeOrNow::Now => SystemTime::now(),
        }
    }
}

#[derive(Debug)]
pub struct FuseMT<T> {
    target: Arc<T>,
    inodes: Arc<Mutex<InodeTable>>,
    threads: OnceLock<ThreadPool>,
    num_threads: usize,
    directory_cache: Arc<Mutex<DirectoryCache>>,
}

impl<T: FilesystemMT + Sync + Send + 'static> FuseMT<T> {
    pub fn new(target_fs: T, num_threads: usize) -> FuseMT<T> {
        FuseMT {
            target: Arc::new(target_fs),
            inodes: Arc::new(Mutex::new(InodeTable::new())),
            threads: OnceLock::new(),
            num_threads,
            directory_cache: Arc::new(Mutex::new(DirectoryCache::new())),
        }
    }

    fn threadpool_run<F: FnOnce() + Send + 'static>(&self, f: F) {
        if self.num_threads == 0 {
            f()
        } else {
            let threads = self.threads.get_or_init(|| {
                debug!("initializing threadpool with {} threads", self.num_threads);
                ThreadPool::new(self.num_threads)
            });
            threads.execute(f);
        }
    }
}

macro_rules! get_path {
    ($s:expr, $ino:expr, $reply:expr) => {
        if let Some(path) = $s.inodes.lock().unwrap().get_path($ino) {
            path
        } else {
            $reply.error(Errno::EINVAL);
            return;
        }
    }
}

impl<T: FilesystemMT + Sync + Send + 'static> fuser::Filesystem for FuseMT<T> {
    fn init(
        &mut self,
        req: &fuser::Request,
        _config: &mut fuser::KernelConfig, // TODO
    ) -> Result<(), std::io::Error> {
        debug!("init");
        self.target.init(req.info()).map_err(io::Error::from_raw_os_error)
    }

    fn destroy(&mut self) {
        debug!("destroy");
        self.target.destroy();
    }

    fn lookup(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("lookup: {:?}, {:?}", parent_path, name);
        let path = Arc::new((*parent_path).clone().join(name));
        match self.target.getattr(req.info(), &path, None) {
            Ok((ttl, attr)) => {
                let mut inodes = self.inodes.lock().unwrap();
                let (ino, generation) = inodes.add_or_get(path.clone());
                inodes.lookup(ino);
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation);
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn forget(
        &self,
        _req: &fuser::Request,
        ino: INodeNo,
        nlookup: u64,
    ) {
        let mut inodes = self.inodes.lock().unwrap();
        let path = inodes.get_path(ino).unwrap_or_else(|| {
            Arc::new(PathBuf::from("[unknown]"))
        });
        let lookups = inodes.forget(ino, nlookup);
        debug!("forget: inode {} ({:?}) now at {} lookups", ino, path, lookups);
    }

    fn getattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: Option<FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("getattr: {:?}", path);
        match self.target.getattr(req.info(), &path, fh.map(|h| h.0)) {
            Ok((ttl, attr)) => {
                reply.attr(&ttl, &fuse_fileattr(attr, ino))
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn setattr(
        &self,
        req: &fuser::Request,           // passed to all
        ino: INodeNo,                   // translated to path; passed to all
        mode: Option<u32>,              // chmod
        uid: Option<u32>,               // chown
        gid: Option<u32>,               // chown
        size: Option<u64>,              // truncate
        atime: Option<TimeOrNow>,       // utimens
        mtime: Option<TimeOrNow>,       // utimens
        _ctime: Option<SystemTime>,     // ? TODO
        fh: Option<FileHandle>,         // passed to all
        crtime: Option<SystemTime>,     // utimens_osx  (OS X only)
        chgtime: Option<SystemTime>,    // utimens_osx  (OS X only)
        bkuptime: Option<SystemTime>,   // utimens_osx  (OS X only)
        flags: Option<BsdFileFlags>,    // utimens_osx  (OS X only)
        reply: fuser::ReplyAttr,
    ) {
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

        let fh = fh.map(|h| h.0);

        if let Some(mode) = mode {
            if let Err(e) = self.target.chmod(req.info(), &path, fh, mode) {
                reply.error(Errno::from_i32(e));
                return;
            }
        }

        if uid.is_some() || gid.is_some() {
            if let Err(e) = self.target.chown(req.info(), &path, fh, uid, gid) {
                reply.error(Errno::from_i32(e));
                return;
            }
        }

        if let Some(size) = size {
            if let Err(e) = self.target.truncate(req.info(), &path, fh, size) {
                reply.error(Errno::from_i32(e));
                return;
            }
        }

        if atime.is_some() || mtime.is_some() {
            let atime = atime.map(TimeOrNowExt::time);
            let mtime = mtime.map(TimeOrNowExt::time);
            if let Err(e) = self.target.utimens(req.info(), &path, fh, atime, mtime) {
                reply.error(Errno::from_i32(e));
                return;
            }
        }

        if crtime.is_some() || chgtime.is_some() || bkuptime.is_some() || flags.is_some() {
            if let Err(e) = self.target.utimens_macos(req.info(), &path, fh, crtime, chgtime, bkuptime, flags.map(|f| f.bits())) {
                reply.error(Errno::from_i32(e));
                return
            }
        }

        match self.target.getattr(req.info(), &path, fh) {
            Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
   }

    fn readlink(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        reply: fuser::ReplyData,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("readlink: {:?}", path);
        match self.target.readlink(req.info(), &path) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn mknod(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mknod: {:?}/{:?}", parent_path, name);
        match self.target.mknod(req.info(), &parent_path, name, mode, rdev) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.lock().unwrap()
                    .add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn mkdir(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        reply: fuser::ReplyEntry,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mkdir: {:?}/{:?}", parent_path, name);
        match self.target.mkdir(req.info(), &parent_path, name, mode) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.lock().unwrap()
                    .add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn unlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("unlink: {:?}/{:?}", parent_path, name);
        match self.target.unlink(req.info(), &parent_path, name) {
            Ok(()) => {
                self.inodes.lock().unwrap().unlink(&parent_path.join(name));
                reply.ok()
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn rmdir(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("rmdir: {:?}/{:?}", parent_path, name);
        match self.target.rmdir(req.info(), &parent_path, name) {
            Ok(()) => {
                self.inodes.lock().unwrap().unlink(&parent_path.join(name));
                reply.ok()
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn symlink(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        link: &Path,
        reply: fuser::ReplyEntry,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("symlink: {:?}/{:?} -> {:?}", parent_path, name, link);
        match self.target.symlink(req.info(), &parent_path, name, link) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.lock().unwrap()
                    .add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn rename(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags, // TODO
        reply: fuser::ReplyEmpty,
    ) {
        let parent_path = get_path!(self, parent, reply);
        let newparent_path = get_path!(self, newparent, reply);
        debug!("rename: {:?}/{:?} -> {:?}/{:?}", parent_path, name, newparent_path, newname);
        match self.target.rename(req.info(), &parent_path, name, &newparent_path, newname) {
            Ok(()) => {
                self.inodes.lock().unwrap().rename(&parent_path.join(name), Arc::new(newparent_path.join(newname)));
                reply.ok()
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn link(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let path = get_path!(self, ino, reply);
        let newparent_path = get_path!(self, newparent, reply);
        debug!("link: {:?} -> {:?}/{:?}", path, newparent_path, newname);
        match self.target.link(req.info(), &path, &newparent_path, newname) {
            Ok((ttl, attr)) => {
                // NOTE: this results in the new link having a different inode from the original.
                // This is needed because our inode table is a 1:1 map between paths and inodes.
                let (new_ino, generation) = self.inodes.lock().unwrap()
                    .add(Arc::new(newparent_path.join(newname)));
                reply.entry(&ttl, &fuse_fileattr(attr, new_ino), generation);
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn open(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        flags: OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("open: {:?}", path);
        match self.target.open(req.info(), &path, flags.0 as u32) {
            Ok((fh, flags)) => reply.opened(FileHandle(fh), FopenFlags::from_bits_retain(flags)),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn read(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,                // TODO
        _lock_owner: Option<LockOwner>,   // TODO
        reply: fuser::ReplyData,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move || {
            target.read(req_info, &path, fh.0, offset, size, |result| {
                match result {
                    Ok(data) => reply.data(data),
                    Err(e) => reply.error(Errno::from_i32(e)),
                }
                CallbackResult {
                    _private: std::marker::PhantomData {},
                }
            });
        });
    }

    fn write(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,         // TODO
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,   // TODO
        reply: fuser::ReplyWrite,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        let target = self.target.clone();
        let req_info = req.info();

        // The data needs to be copied here before dispatching to the threadpool because it's a
        // slice of a single buffer that `fuser` re-uses for the entire session.
        let data_buf = Vec::from(data);

        self.threadpool_run(move|| {
            match target.write(req_info, &path, fh.0, offset, data_buf, flags.0 as u32) {
                Ok(written) => reply.written(written),
                Err(e) => reply.error(Errno::from_i32(e)),
            }
        });
    }

    fn flush(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        lock_owner: LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("flush: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move|| {
            match target.flush(req_info, &path, fh.0, lock_owner.0) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(Errno::from_i32(e)),
            }
        });
    }

    fn release(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("release: {:?}", path);
        match self.target.release(
            req.info(),
            &path,
            fh.0,
            flags.0 as u32,
            lock_owner.map(|o| o.0).unwrap_or(0) /* TODO */,
            flush,
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn fsync(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("fsync: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move|| {
            match target.fsync(req_info, &path, fh.0, datasync) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(Errno::from_i32(e)),
            }
        });
    }

    fn opendir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        flags: OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("opendir: {:?}", path);
        match self.target.opendir(req.info(), &path, flags.0 as u32) {
            Ok((fh, flags)) => {
                let dcache_key = self.directory_cache.lock().unwrap().new_entry(fh);
                reply.opened(FileHandle(dcache_key), FopenFlags::from_bits_retain(flags));
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn readdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("readdir: {:?} @ {}", path, offset);

        let mut dcache = self.directory_cache.lock().unwrap();
        let entries: &[DirectoryEntry] = {
            let dcache_entry = dcache.get_mut(fh.0);
            if let Some(ref entries) = dcache_entry.entries {
                entries
            } else {
                debug!("entries not yet fetched; requesting with fh {}", dcache_entry.fh);
                match self.target.readdir(req.info(), &path, dcache_entry.fh) {
                    Ok(entries) => {
                        dcache_entry.entries = Some(entries);
                        dcache_entry.entries.as_ref().unwrap()
                    },
                    Err(e) => {
                        reply.error(Errno::from_i32(e));
                        return;
                    }
                }
            }
        };

        let parent_inode = if ino == ROOT {
            ino
        } else {
            let parent_path: &Path = path.parent().unwrap();
            match self.inodes.lock().unwrap().get_inode(parent_path) {
                Some(inode) => inode,
                None => {
                    error!("readdir: unable to get inode for parent of {:?}", path);
                    reply.error(Errno::EIO);
                    return;
                }
            }
        };

        debug!("directory has {} entries", entries.len());

        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let entry_inode = if entry.name == Path::new(".") {
                ino
            } else if entry.name == Path::new("..") {
                parent_inode
            } else {
                // Don't bother looking in the inode table for the entry; FUSE doesn't pre-
                // populate its inode cache with this value, so subsequent access to these
                // files is going to involve it issuing a LOOKUP operation anyway.
                INodeNo(!1)
            };

            debug!("readdir: adding entry #{}, {:?}", offset + index as u64, entry.name);

            let buffer_full: bool = reply.add(
                entry_inode,
                offset + index as u64 + 1,
                entry.kind,
                entry.name.as_os_str());

            if buffer_full {
                debug!("readdir: reply buffer is full");
                break;
            }
        }

        reply.ok();
    }

    fn releasedir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        flags: OpenFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("releasedir: {:?}", path);
        let mut dcache = self.directory_cache.lock().unwrap();
        let real_fh = dcache.real_fh(fh.0);
        match self.target.releasedir(req.info(), &path, real_fh, flags.0 as u32) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
        dcache.delete(fh.0);
    }

    fn fsyncdir(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);
        let real_fh = self.directory_cache.lock().unwrap().real_fh(fh.0);
        match self.target.fsyncdir(req.info(), &path, real_fh, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn statfs(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        reply: fuser::ReplyStatfs,
    ) {
        let path = if ino == ROOT {
            Arc::new(PathBuf::from("/"))
        } else {
            get_path!(self, ino, reply)
        };

        debug!("statfs: {:?}", path);
        match self.target.statfs(req.info(), &path) {
            Ok(statfs) => reply.statfs(
                statfs.blocks,
                statfs.bfree,
                statfs.bavail,
                statfs.files,
                statfs.ffree,
                statfs.bsize,
                statfs.namelen,
                statfs.frsize),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn setxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("setxattr: {:?} {:?} ({} bytes, flags={:#x}, pos={:#x}",
            path, name, value.len(), flags, position);
        match self.target.setxattr(req.info(), &path, name, value, flags as u32, position) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn getxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        let path = get_path!(self, ino, reply);
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
                reply.error(Errno::from_i32(e))
            },
        }
    }

    fn listxattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        let path = get_path!(self, ino, reply);
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
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn removexattr(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("removexattr: {:?}, {:?}", path, name);
        match self.target.removexattr(req.info(), &path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn access(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        mask: AccessFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("access: {:?}, mask={:#o}", path, mask);
        match self.target.access(req.info(), &path, mask.bits() as u32) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    fn create(
        &self,
        req: &fuser::Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32, // TODO
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_path = get_path!(self, parent, reply);
        debug!("create: {:?}/{:?} (mode={:#o}, flags={:#x})", parent_path, name, mode, flags);
        match self.target.create(req.info(), &parent_path, name, mode, flags as u32) {
            Ok(create) => {
                let (ino, generation) = self.inodes.lock().unwrap().add(Arc::new(parent_path.join(name)));
                let attr = fuse_fileattr(create.attr, ino);
                reply.created(&create.ttl, &attr, generation, FileHandle(create.fh), FopenFlags::from_bits_retain(create.flags));
            },
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    // getlk

    // setlk

    // bmap

    #[cfg(target_os = "macos")]
    fn setvolname(
        &self,
        req: &fuser::Request,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        debug!("setvolname: {:?}", name);
        match self.target.setvolname(req.info(), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }

    // exchange (macOS only, undocumented)

    #[cfg(target_os = "macos")]
    fn getxtimes(
        &self,
        req: &fuser::Request,
        ino: INodeNo,
        reply: fuser::ReplyXTimes,
    ) {
        let path = get_path!(self, ino, reply);
        debug!("getxtimes: {:?}", path);
        match self.target.getxtimes(req.info(), &path) {
            Ok(xtimes) => {
                reply.xtimes(xtimes.bkuptime, xtimes.crtime);
            }
            Err(e) => reply.error(Errno::from_i32(e)),
        }
    }
}
