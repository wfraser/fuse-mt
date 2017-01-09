// FuseMT :: A wrapper around FUSE that presents paths instead of inodes and dispatches I/O
//           operations to multiple threads.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fuse::*;
use libc;
use threadpool::ThreadPool;
use time::Timespec;

use directory_cache::*;
use inode_table::*;

/// Info about a request:
///
/// * `unique`: the unique ID assigned to this request by FUSE.
/// * `uid`: the user ID of the process making the request.
/// * `gid`: the group ID of the process making the request.
/// * `pid`: the process ID of the process making the request.
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

/// A directory entry.
///
/// * `name`: the name of the entry
/// * `kind`:
pub struct DirectoryEntry {
    pub name: OsString,
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

/// The return value for `create`: contains info on the newly-created file, as well as a handle to
/// the opened file.
pub struct CreatedEntry {
    pub ttl: Timespec,
    pub attr: FileAttr,
    pub generation: u64,
    pub fh: u64,
    pub flags: u32,
}

/// Represents the return value from the `listxattr` and `getxattr` calls, which can be either a
/// size or contain data, depending on how they are called.
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

/// This trait must be implemented to implement a filesystem with FuseMT.
pub trait FilesystemMT {
    /// Called on mount, before any other function.
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Err(0)
    }

    /// Called on filesystem unmount.
    fn destroy(&self, _req: RequestInfo) {
        // Nothing.
    }

    /// Look up a filesystem entry and get its attributes.
    ///
    /// * `parent`: path to the parent of the entry being looked up
    /// * `name`: the name of the entry (under `parent`) being looked up.
    fn lookup(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    /// Get the attributes of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    fn getattr(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>) -> ResultGetattr {
        Err(libc::ENOSYS)
    }

    // The following operations in the FUSE C API are all one kernel call: setattr
    // We split them out to match the C API's behavior.

    /// Change the mode of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `mode`: the mode to change the file to.
    fn chmod(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _mode: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Change the owner UID and/or group GID of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `uid`: user ID to change the file's owner to. If `None`, leave the UID unchanged.
    /// * `gid`: group ID to change the file's group to. If `None`, leave the GID unchanged.
    fn chown(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set the length of a file.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `size`: size in bytes to set as the file's length.
    fn truncate(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _size: u64) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set timestamps of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `atime`: the time of last access.
    /// * `mtime`: the time of last modification.
    fn utimens(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set timestamps of a filesystem entry (with extra options only used on MacOS).
    #[allow(unknown_lints, too_many_arguments)]
    fn utimens_macos(&self, _req: RequestInfo, _path: &Path, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // END OF SETATTR FUNCTIONS

    /// Read a symbolic link.
    fn readlink(&self, _req: RequestInfo, _path: &Path) -> ResultData {
        Err(libc::ENOSYS)
    }

    /// Create a special file.
    ///
    /// * `parent`: path to the directory to make the entry under.
    /// * `name`: name of the entry.
    /// * `mode`: mode for the new entry.
    /// * `rdev`: if mode has the bits `S_IFCHR` or `S_IFBLK` set, this is the major and minor numbers for the device file. Otherwise it should be ignored.
    fn mknod(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32, _rdev: u32) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    /// Create a directory.
    ///
    /// * `parent`: path to the directory to make the directory under.
    /// * `name`: name of the directory.
    /// * `mode`: permissions for the new directory.
    fn mkdir(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    /// Remove a file.
    ///
    /// * `parent`: path to the directory containing the file to delete.
    /// * `name`: name of the file to delete.
    fn unlink(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Remove a directory.
    ///
    /// * `parent`: path to the directory containing the directory to delete.
    /// * `name`: name of the directory to delete.
    fn rmdir(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Create a symbolic link.
    ///
    /// * `parent`: path to the directory to make the link in.
    /// * `name`: name of the symbolic link.
    /// * `target`: path (may be relative or absolute) to the target of the link.
    fn symlink(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _target: &Path) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    /// Rename a filesystem entry.
    ///
    /// * `parent`: path to the directory containing the existing entry.
    /// * `name`: name of the existing entry.
    /// * `newparent`: path to the directory it should be renamed into (may be the same as `parent`).
    /// * `newname`: name of the new entry.
    fn rename(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _newparent: &Path, _newname: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Create a hard link.
    ///
    /// * `path`: path to an existing file.
    /// * `newparent`: path to the directory for the new link.
    /// * `newname`: name for the new link.
    fn link(&self, _req: RequestInfo, _path: &Path, _newparent: &Path, _newname: &OsStr) -> ResultEntry {
        Err(libc::ENOSYS)
    }

    /// Open a file.
    ///
    /// * `path`: path to the file.
    /// * `flags`: one of `O_RDONLY`, `O_WRONLY`, or `O_RDWR`, plus maybe additional flags.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the file, and can be any value you choose, though it should allow
    /// your filesystem to identify the file opened even without any path info.
    fn open(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Err(libc::ENOSYS)
    }

    /// Read from a file.
    ///
    /// Note that it is not an error for this call to request to read past the end of the file, and
    /// you should only return data up to the end of the file (i.e. the number of bytes returned
    /// will be fewer than requested; possibly even zero). Do not extend the file in this case.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `offset`: offset into the file to start reading.
    /// * `size`: number of bytes to read.
    ///
    /// Return the bytes read.
    fn read(&self, _req: RequestInfo, _path: &Path, _fh: u64, _offset: u64, _size: u32) -> ResultData {
        Err(libc::ENOSYS)
    }

    /// Write to a file.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `offset`: offset into the file to start writing.
    /// * `data`: the data to write
    /// * `flags`:
    ///
    /// Return the number of bytes written.
    fn write(&self, _req: RequestInfo, _path: &Path, _fh: u64, _offset: u64, _data: Vec<u8>, _flags: u32) -> ResultWrite {
        Err(libc::ENOSYS)
    }

    /// Called each time a program calls `close` on an open file.
    ///
    /// Note that because file descriptors can be duplicated (by `dup`, `dup2`, `fork`) this may be
    /// called multiple times for a given file handle. The main use of this function is if the
    /// filesystem would like to return an error to the `close` call. Note that most programs
    /// ignore the return value of `close`, though.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `lock_owner`: if the filesystem supports locking (`setlk`, `getlk`), remove all locks
    ///   belonging to this lock owner.
    fn flush(&self, _req: RequestInfo, _path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Called when an open file is closed.
    ///
    /// There will be one of these for each `open` call. After `release`, no more calls will be
    /// made with the given file handle.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `flags`: the flags passed when the file was opened.
    /// * `lock_owner`: if the filesystem supports locking (`setlk`, `getlk`), remove all locks
    ///   belonging to this lock owner.
    /// * `flush`: whether pending data must be flushed or not.
    fn release(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Write out any pending changes of a file.
    ///
    /// When this returns, data should be written to persistent storage.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `datasync`: if `false`, just write metadata, otherwise also write file data.
    fn fsync(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Open a directory.
    ///
    /// Analogous to the `opend` call.
    ///
    /// * `path`: path to the directory.
    /// * `flags`: file access flags. Will contain `O_DIRECTORY` at least.
    ///
    /// Return a tuple of (file handle, flags). The file handle will be passed to any subsequent
    /// calls that operate on the directory, and can be any value you choose, though it should
    /// allow your filesystem to identify the directory opened even without any path info.
    fn opendir(&self, _req: RequestInfo, _path: &Path, _flags: u32) -> ResultOpen {
        Err(libc::ENOSYS)
    }

    /// Get the entries of a directory.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    ///
    /// Return all the entries of the directory.
    fn readdir(&self, _req: RequestInfo, _path: &Path, _fh: u64) -> ResultReaddir {
        Err(libc::ENOSYS)
    }

    /// Close an open directory.
    ///
    /// This will be called exactly once for each `opendir` call.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    /// * `flags`: the file access flags passed to the `opendir` call.
    fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Write out any pending changes to a directory.
    ///
    /// Analogous to the `fsync` call.
    fn fsyncdir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _datasync: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Get filesystem statistics.
    ///
    /// * `path`: path to some folder in the filesystem.
    ///
    /// See the `Statfs` struct for more details.
    fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
        Err(libc::ENOSYS)
    }

    /// Set a file extended attribute.
    ///
    /// * `path`: path to the file.
    /// * `name`: attribute name.
    /// * `value`: the data to set the value to.
    /// * `flags`: can be either `XATTR_CREATE` or `XATTR_REPLACE`.
    /// * `position`: offset into the attribute value to write data.
    fn setxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _value: &[u8], _flags: u32, _position: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Get a file extended attribute.
    ///
    /// * `path`: path to the file
    /// * `name`: attribute name.
    /// * `size`: the maximum number of bytes to read.
    ///
    /// If `size` is 0, return `Xattr::Size(n)` where `n` is the size of the attribute data.
    /// Otherwise, return `Xattr::Data(data)` with the requested data.
    fn getxattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr, _size: u32) -> ResultXattr {
        Err(libc::ENOSYS)
    }

    /// List extended attributes for a file.
    ///
    /// * `path`: path to the file.
    /// * `size`: maximum number of bytes to return.
    ///
    /// If `size` is 0, return `Xattr::Size(n)` where `n` is the size required for the list of
    /// attribute names.
    /// Otherwise, return `Xattr::Data(data)` where `data` is all the null-terminated attribute
    /// names.
    fn listxattr(&self, _req: RequestInfo, _path: &Path, _size: u32) -> ResultXattr {
        Err(libc::ENOSYS)
    }

    /// Remove an extended attribute for a file.
    ///
    /// * `path`: path to the file.
    /// * `name`: name of the attribute to remove.
    fn removexattr(&self, _req: RequestInfo, _path: &Path, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // access

    /// Create and open a new file.
    ///
    /// * `parent`: path to the directory to create the file in.
    /// * `name`: name of the file to be created.
    /// * `mode`: the mode to set on the new file.
    /// * `flags`: flags like would be passed to `open`.
    ///
    /// Return a `CreatedEntry` (which contains the new file's attributes as well as a file handle
    /// -- see documentation on `open` for more info on that).
    fn create(&self, _req: RequestInfo, _parent: &Path, _name: &OsStr, _mode: u32, _flags: u32) -> ResultCreate {
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
    directory_cache: DirectoryCache,
}

impl<T: FilesystemMT + Sync + Send + 'static> FuseMT<T> {
    pub fn new(target_fs: T, num_threads: usize) -> FuseMT<T> {
        FuseMT {
            target: Arc::new(target_fs),
            inodes: InodeTable::new(),
            threads: ThreadPool::new(num_threads),
            directory_cache: DirectoryCache::new(),
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

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
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
            Ok((ref ttl, ref mut attr)) => {
                attr.ino = ino;
                reply.attr(ttl, attr)
            },
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

    fn mknod(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, rdev: u32, reply: ReplyEntry) {
        let parent_path = get_path!(self, parent, reply);
        debug!("mknod: {:?}/{:?}", parent_path, name);
        match self.target.mknod(req.info(), &parent_path, name, mode, rdev) {
            Ok((ref ttl, ref mut attr, generation)) => {
                let ino = self.inodes.add(Arc::new(parent_path.join(name)));
                attr.ino = ino;
                reply.entry(ttl, attr, generation)
            },
            Err(e) => reply.error(e),
        }
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
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

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
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

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = get_path!(self, parent, reply);
        debug!("rmdir: {:?}/{:?}", parent_path, name);
        match self.target.rmdir(req.info(), &parent_path, name) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
    }

    fn symlink(&mut self, req: &Request, parent: u64, name: &OsStr, link: &Path, reply: ReplyEntry) {
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

    fn rename(&mut self, req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, reply: ReplyEmpty) {
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

    fn link(&mut self, req: &Request, ino: u64, newparent: u64, newname: &OsStr, reply: ReplyEntry) {
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

        // The data needs to be copied here before dispatching to the threadpool because it's a
        // slice of a single buffer that `rust-fuse` re-uses for the entire session.
        let data_buf = Vec::from(data);

        self.threads.execute(move|| {
            match target.write(req_info, &path, fh, offset, data_buf, flags) {
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
            Ok((fh, flags)) => {
                let dcache_key = self.directory_cache.new_entry(fh);
                reply.opened(dcache_key, flags);
            },
            Err(e) => reply.error(e),
        }
    }

    fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: u64, mut reply: ReplyDirectory) {
        let path = get_path!(self, ino, reply);
        debug!("readdir: {:?} @ {}", path, offset);

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

    fn releasedir(&mut self, req: &Request, ino: u64, fh: u64, flags: u32, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("releasedir: {:?}", path);
        let real_fh = self.directory_cache.real_fh(fh);
        match self.target.releasedir(req.info(), &path, real_fh, flags) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e),
        }
        self.directory_cache.delete(fh);
    }

    fn fsyncdir(&mut self, req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let path = get_path!(self, ino, reply);
        debug!("fsyncdir: {:?} (datasync: {:?})", path, datasync);
        let real_fh = self.directory_cache.real_fh(fh);
        match self.target.fsyncdir(req.info(), &path, real_fh, datasync) {
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

    fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: ReplyCreate) {
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
