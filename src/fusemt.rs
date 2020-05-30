// FuseMT :: A wrapper around FUSE that presents paths instead of inodes and dispatches I/O
//           operations to multiple threads.
//
// Copyright (c) 2016-2019 by William R. Fraser
//

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use fuse;
use libc;
use threadpool::ThreadPool;

use directory_cache::*;
use inode_table::*;
use types::*;

trait IntoRequestInfo {
    fn info(&self) -> RequestInfo;
}

impl<'a> IntoRequestInfo for fuse::Request<'a> {
    fn info(&self) -> RequestInfo {
        RequestInfo {
            unique: self.unique(),
            uid: self.uid(),
            gid: self.gid(),
            pid: self.pid(),
        }
    }
}

fn fuse_fileattr(attr: FileAttr, ino: u64) -> fuse::FileAttr {
    fuse::FileAttr {
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
        flags: attr.flags,
    }
}

#[derive(Debug)]
pub struct FuseMT<T> {
    target: Arc<T>,
    inodes: InodeTable,
    threads: Option<ThreadPool>,
    num_threads: usize,
    directory_cache: DirectoryCache,
}

impl<T: FilesystemMT + Sync + Send + 'static> FuseMT<T> {
    pub fn new(target_fs: T, num_threads: usize) -> FuseMT<T> {
        FuseMT {
            target: Arc::new(target_fs),
            inodes: InodeTable::new(),
            threads: None,
            num_threads,
            directory_cache: DirectoryCache::new(),
        }
    }

    fn threadpool_run<F: FnOnce() + Send + 'static>(&mut self, f: F) {
        if self.num_threads == 0 {
            f()
        } else {
            if self.threads.is_none() {
                debug!("initializing threadpool with {} threads", self.num_threads);
                self.threads = Some(ThreadPool::new(self.num_threads));
            }
            self.threads.as_ref().unwrap().execute(f);
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

impl<T: FilesystemMT + Sync + Send + 'static> fuse::Filesystem for FuseMT<T> {
    fn init(&mut self, req: &fuse::Request) -> Result<(), libc::c_int> {
        debug!("init");
        self.target.init(req.info())
    }

    fn destroy(&mut self, req: &fuse::Request) {
        debug!("destroy");
        self.target.destroy(req.info());
    }

    fn lookup(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("lookup: {:?}, {:?}", parent_path, name);
        let path = Arc::new((*parent_path).clone().join(name));
        match self.target.getattr(req.info(), &path, None) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.add_or_get(path.clone());
                self.inodes.lookup(ino);
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation);
            },
            Err(e) => reply.error(e),
        }
    }

    fn forget(&mut self, _req: &fuse::Request, ino: u64, nlookup: u64) {
        let path = self.inodes.get_path(ino).unwrap_or_else(|| {
            Arc::new(PathBuf::from("[unknown]"))
        });
        let lookups = self.inodes.forget(ino, nlookup);
        debug!("forget: inode {} ({:?}) now at {} lookups", ino, path, lookups);
    }

    fn getattr(&mut self, req: &fuse::Request, ino: u64, reply: fuse::ReplyAttr) {
        let path = get_path!(self, ino, reply);
        debug!("getattr: {:?}", path);
        match self.target.getattr(req.info(), &path, None) {
            Ok((ttl, attr)) => {
                reply.attr(&ttl, &fuse_fileattr(attr, ino))
            },
            Err(e) => reply.error(e),
        }
    }

    fn setattr(&mut self,
               req: &fuse::Request,          // passed to all
               ino: u64,                     // translated to path; passed to all
               mode: Option<u32>,            // chmod
               uid: Option<u32>,             // chown
               gid: Option<u32>,             // chown
               size: Option<u64>,            // truncate
               atime: Option<SystemTime>,    // utimens
               mtime: Option<SystemTime>,    // utimens
               fh: Option<u64>,              // passed to all
               crtime: Option<SystemTime>,   // utimens_osx  (OS X only)
               chgtime: Option<SystemTime>,  // utimens_osx  (OS X only)
               bkuptime: Option<SystemTime>, // utimens_osx  (OS X only)
               flags: Option<u32>,           // utimens_osx  (OS X only)
               reply: fuse::ReplyAttr) {
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

        if let Some(mode) = mode {
            if let Err(e) = self.target.chmod(req.info(), &path, fh, mode) {
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

        if let Some(size) = size {
            if let Err(e) = self.target.truncate(req.info(), &path, fh, size) {
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
            Ok((ttl, attr)) => reply.attr(&ttl, &fuse_fileattr(attr, ino)),
            Err(e) => reply.error(e),
        }
   }

    fn readlink(&mut self, req: &fuse::Request, ino: u64, reply: fuse::ReplyData) {
        let path = get_path!(self, ino, reply);
        debug!("readlink: {:?}", path);
        match self.target.readlink(req.info(), &path) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(e),
        }
    }

    fn mknod(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, mode: u32, rdev: u32, reply: fuse::ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mknod: {:?}/{:?}", parent_path, name);
        match self.target.mknod(req.info(), &parent_path, name, mode, rdev) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, mode: u32, reply: fuse::ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mkdir: {:?}/{:?}", parent_path, name);
        match self.target.mkdir(req.info(), &parent_path, name, mode) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn unlink(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        debug!("unlink: {:?}/{:?}", parent_path, name);
        match self.target.unlink(req.info(), &parent_path, name) {
            Ok(()) => {
                self.inodes.unlink(&parent_path.join(name));
                reply.ok()
            },
            Err(e) => reply.error(e),
        }
    }

    fn rmdir(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, reply: fuse::ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        debug!("rmdir: {:?}/{:?}", parent_path, name);
        match self.target.rmdir(req.info(), &parent_path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, link: &Path, reply: fuse::ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("symlink: {:?}/{:?} -> {:?}", parent_path, name, link);
        match self.target.symlink(req.info(), &parent_path, name, link) {
            Ok((ttl, attr)) => {
                let (ino, generation) = self.inodes.add(Arc::new(parent_path.join(name)));
                reply.entry(&ttl, &fuse_fileattr(attr, ino), generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn rename(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, reply: fuse::ReplyEmpty) {
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

    fn link(&mut self, req: &fuse::Request, ino: u64, newparent: u64, newname: &OsStr, reply: fuse::ReplyEntry) {
        let path = get_path!(self, ino, reply);
        let newparent_path = get_path!(self, newparent, reply);
        debug!("link: {:?} -> {:?}/{:?}", path, newparent_path, newname);
        match self.target.link(req.info(), &path, &newparent_path, newname) {
            Ok((ttl, attr)) => {
                // NOTE: this results in the new link having a different inode from the original.
                // This is needed because our inode table is a 1:1 map between paths and inodes.
                let (new_ino, generation) = self.inodes.add(Arc::new(newparent_path.join(newname)));
                reply.entry(&ttl, &fuse_fileattr(attr, new_ino), generation);
            },
            Err(e) => reply.error(e),
        }
    }

    fn open(&mut self, req: &fuse::Request, ino: u64, flags: u32, reply: fuse::ReplyOpen) {
        let path = get_path!(self, ino, reply);
        debug!("open: {:?}", path);
        match self.target.open(req.info(), &path, flags) {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(e) => reply.error(e),
        }
    }

    fn read(&mut self, req: &fuse::Request, ino: u64, fh: u64, offset: i64, size: u32, reply: fuse::ReplyData) {
        let path = get_path!(self, ino, reply);
        debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
        if offset < 0 {
            error!("read called with a negative offset");
            reply.error(libc::EINVAL);
            return;
        }
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move || {
            target.read(req_info, &path, fh, offset as u64, size, |result| {
                match result {
                    Ok(data) => reply.data(data),
                    Err(e) => reply.error(e),
                }
            });
        });
    }

    fn write(&mut self, req: &fuse::Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: fuse::ReplyWrite) {
        let path = get_path!(self, ino, reply);
        debug!("write: {:?} {:#x} @ {:#x}", path, data.len(), offset);
        if offset < 0 {
            error!("write called with a negative offset");
            reply.error(libc::EINVAL);
            return;
        }
        let target = self.target.clone();
        let req_info = req.info();

        // The data needs to be copied here before dispatching to the threadpool because it's a
        // slice of a single buffer that `rust-fuse` re-uses for the entire session.
        let data_buf = Vec::from(data);

        self.threadpool_run(move|| {
            match target.write(req_info, &path, fh, offset as u64, data_buf, flags) {
                Ok(written) => reply.written(written),
                Err(e) => reply.error(e),
            }
        });
    }

    fn flush(&mut self, req: &fuse::Request, ino: u64, fh: u64, lock_owner: u64, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("flush: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move|| {
            match target.flush(req_info, &path, fh, lock_owner) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    fn release(&mut self, req: &fuse::Request, ino: u64, fh: u64, flags: u32, lock_owner: u64, flush: bool, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("release: {:?}", path);
        match self.target.release(req.info(), &path, fh, flags, lock_owner, flush) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn fsync(&mut self, req: &fuse::Request, ino: u64, fh: u64, datasync: bool, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("fsync: {:?}", path);
        let target = self.target.clone();
        let req_info = req.info();
        self.threadpool_run(move|| {
            match target.fsync(req_info, &path, fh, datasync) {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(e),
            }
        });
    }

    fn opendir(&mut self, req: &fuse::Request, ino: u64, flags: u32, reply: fuse::ReplyOpen) {
        let path = get_path!(self, ino, reply);
        debug!("opendir: {:?}", path);
        match self.target.opendir(req.info(), &path, flags) {
            Ok((fh, flags)) => {
                let dcache_key = self.directory_cache.new_entry(fh);
                reply.opened(dcache_key, flags);
            },
            Err(e) => reply.error(e),
        }
    }

    fn readdir(&mut self, req: &fuse::Request, ino: u64, fh: u64, offset: i64, mut reply: fuse::ReplyDirectory) {
        let path = get_path!(self, ino, reply);
        debug!("readdir: {:?} @ {}", path, offset);

        if offset < 0 {
            error!("readdir called with a negative offset");
            reply.error(libc::EINVAL);
            return;
        }

        let entries: &[DirectoryEntry] = {
            let dcache_entry = self.directory_cache.get_mut(fh);
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
                        reply.error(e);
                        return;
                    }
                }
            }
        };

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
                !(1 as Inode)
            };

            debug!("readdir: adding entry #{}, {:?}", offset + index as i64, entry.name);

            let buffer_full: bool = reply.add(
                entry_inode,
                offset + index as i64 + 1,
                entry.kind,
                entry.name.as_os_str());

            if buffer_full {
                debug!("readdir: reply buffer is full");
                break;
            }
        }

        reply.ok();
    }

    fn releasedir(&mut self, req: &fuse::Request, ino: u64, fh: u64, flags: u32, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("releasedir: {:?}", path);
        let real_fh = self.directory_cache.real_fh(fh);
        match self.target.releasedir(req.info(), &path, real_fh, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
        self.directory_cache.delete(fh);
    }

    fn fsyncdir(&mut self, req: &fuse::Request, ino: u64, fh: u64, datasync: bool, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);
        let real_fh = self.directory_cache.real_fh(fh);
        match self.target.fsyncdir(req.info(), &path, real_fh, datasync) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn statfs(&mut self, req: &fuse::Request, ino: u64, reply: fuse::ReplyStatfs) {
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

    fn setxattr(&mut self, req: &fuse::Request, ino: u64, name: &OsStr, value: &[u8], flags: u32, position: u32, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("setxattr: {:?} {:?} ({} bytes, flags={:#x}, pos={:#x}", path, name, value.len(), flags, position);
        match self.target.setxattr(req.info(), &path, name, value, flags, position) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn getxattr(&mut self, req: &fuse::Request, ino: u64, name: &OsStr, size: u32, reply: fuse::ReplyXattr) {
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
                reply.error(e)
            },
        }
    }

    fn listxattr(&mut self, req: &fuse::Request, ino: u64, size: u32, reply: fuse::ReplyXattr) {
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
            Err(e) => reply.error(e),
        }
    }

    fn removexattr(&mut self, req: &fuse::Request, ino: u64, name: &OsStr, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("removexattr: {:?}, {:?}", path, name);
        match self.target.removexattr(req.info(), &path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn access(&mut self, req: &fuse::Request, ino: u64, mask: u32, reply: fuse::ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("access: {:?}, mask={:#o}", path, mask);
        match self.target.access(req.info(), &path, mask) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn create(&mut self, req: &fuse::Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: fuse::ReplyCreate) {
        let parent_path = get_path!(self, parent, reply);
        debug!("create: {:?}/{:?} (mode={:#o}, flags={:#x})", parent_path, name, mode, flags);
        match self.target.create(req.info(), &parent_path, name, mode, flags) {
            Ok(create) => {
                let (ino, generation) = self.inodes.add(Arc::new(parent_path.join(name)));
                let attr = fuse_fileattr(create.attr, ino);
                reply.created(&create.ttl, &attr, generation, create.fh, create.flags);
            },
            Err(e) => reply.error(e),
        }
    }

    // getlk

    // setlk

    // bmap

    #[cfg(target_os = "macos")]
    fn setvolname(&mut self, req: &fuse::Request, name: &OsStr, reply: fuse::ReplyEmpty) {
        debug!("setvolname: {:?}", name);
        match self.target.setvolname(req.info(), name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    // exchange (macOS only, undocumented)

    #[cfg(target_os = "macos")]
    fn getxtimes(&mut self, req: &fuse::Request, ino: u64, reply: fuse::ReplyXTimes) {
        let path = get_path!(self, ino, reply);
        debug!("getxtimes: {:?}", path);
        match self.target.getxtimes(req.info(), &path) {
            Ok(xtimes) => {
                reply.xtimes(xtimes.bkuptime, xtimes.crtime);
            }
            Err(e) => reply.error(e),
        }
    }
}
