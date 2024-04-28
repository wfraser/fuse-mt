// Public types exported by FuseMT.
//
// Copyright (c) 2016-2022 by William R. Fraser
//

use std::ffi::{OsStr, OsString};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::time::{Duration, SystemTime};
pub use crate::inode_table::Inode;

/// Info about a request.
#[derive(Clone, Copy, Debug)]
pub struct RequestInfo {
    /// The unique ID assigned to this request by FUSE.
    pub unique: u64,
    /// The user ID of the process making the request.
    pub uid: u32,
    /// The group ID of the process making the request.
    pub gid: u32,
    /// The process ID of the process making the request.
    pub pid: u32,
}

/// A directory entry.
#[derive(Clone, Debug)]
pub struct DirectoryEntry {
    /// Name of the entry
    pub name: OsString,
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: crate::FileType,
}

/// Filesystem statistics.
#[derive(Clone, Copy, Debug)]
pub struct Statfs {
    /// Total data blocks in the filesystem
    pub blocks: u64,
    /// Free blocks in filesystem
    pub bfree: u64,
    /// Free blocks available to unprivileged user
    pub bavail: u64,
    /// Total file nodes in filesystem
    pub files: u64,
    /// Free file nodes in filesystem
    pub ffree: u64,
    /// Optimal transfer block size
    pub bsize: u32,
    /// Maximum length of filenames
    pub namelen: u32,
    /// Fragment size
    pub frsize: u32,
}

/// File attributes.
#[derive(Clone, Copy, Debug)]
pub struct FileAttr {
    /// Size in bytes
    pub size: u64,
    /// Size in blocks
    pub blocks: u64,
    /// Time of last access
    pub atime: SystemTime,
    /// Time of last modification
    pub mtime: SystemTime,
    /// Time of last metadata change
    pub ctime: SystemTime,
    /// Time of creation (macOS only)
    pub crtime: SystemTime,
    /// Kind of file (directory, file, pipe, etc.)
    pub kind: crate::FileType,
    /// Permissions
    pub perm: u16,
    /// Number of hard links
    pub nlink: u32,
    /// User ID
    pub uid: u32,
    /// Group ID
    pub gid: u32,
    /// Device ID (if special file)
    pub rdev: u32,
    /// Flags (macOS only; see chflags(2))
    pub flags: u32,
}

/// File attributes with inode and generation
/// This implements DerefMut<Target=FileAttr> to not break the API
#[derive(Clone, Copy, Debug)]
pub struct RawFileAttr {
    /// inode
    pub inode: libc::ino_t,
    pub generation: u64,
    pub attr: FileAttr
}

impl Deref for RawFileAttr {
    type Target = FileAttr;

    fn deref(&self) -> &Self::Target {
        &self.attr
    }
}

// TODO: Is this a good choice for the API?
impl DerefMut for RawFileAttr {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.attr
    }
}

impl From<RawFileAttr> for FileAttr {
    fn from(value: RawFileAttr) -> Self {
        value.attr
    }
}

impl FileAttr {
    /// Convert this `FileAttr` instance to an instance of `RawFileAttr`
    /// by adding an inode and its generation
    pub fn as_raw(self, inode: Inode, generation: u64) -> RawFileAttr {
        RawFileAttr {
            inode,
            generation,
            attr: self
        }
    }
}

/// The return value for `create`: contains info on the newly-created file, as well as a handle to
/// the opened file.
#[derive(Clone, Debug)]
pub struct CreatedEntry<Attr = FileAttr> where Attr: Copy + Clone {
    pub ttl: Duration,
    pub attr: Attr,
    pub fh: u64,
    pub flags: u32,
}

/// Represents the return value from the `listxattr` and `getxattr` calls, which can be either a
/// size or contain data, depending on how they are called.
#[derive(Clone, Debug)]
pub enum Xattr {
    Size(u32),
    Data(Vec<u8>),
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug)]
pub struct XTimes {
    pub bkuptime: SystemTime,
    pub crtime: SystemTime,
}



pub type ResultEmpty = Result<(), libc::c_int>;
pub type ResultEntry<Attr = FileAttr> = Result<(Duration, Attr), libc::c_int>;
pub type ResultOpen = Result<(u64, u32), libc::c_int>;
pub type ResultReaddir = Result<Vec<DirectoryEntry>, libc::c_int>;
pub type ResultData = Result<Vec<u8>, libc::c_int>;
pub type ResultSlice<'a> = Result<&'a [u8], libc::c_int>;
pub type ResultWrite = Result<u32, libc::c_int>;
pub type ResultStatfs = Result<Statfs, libc::c_int>;
pub type ResultCreate<Attr = FileAttr> = Result<CreatedEntry<Attr>, libc::c_int>;
pub type ResultXattr = Result<Xattr, libc::c_int>;
pub type ResultInode = Result<Inode, libc::c_int>;

#[cfg(target_os = "macos")]
pub type ResultXTimes = Result<XTimes, libc::c_int>;

#[deprecated(since = "0.3.0", note = "use ResultEntry instead")]
pub type ResultGetattr = ResultEntry;

/// Dummy struct returned by the callback in the `read()` method. Cannot be constructed outside
/// this crate, `read()` requires you to return it, thus ensuring that you don't forget to call the
/// callback.
pub struct CallbackResult {
    pub(crate) _private: std::marker::PhantomData<()>,
}

/// This trait must be implemented to implement a filesystem with FuseMT.
pub trait FilesystemMT<'a, T = &'a Path, Attr = FileAttr> where Attr: Copy + Clone  {
    /// Called on mount, before any other function.
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        Ok(())
    }

    /// Called on filesystem unmount.
    fn destroy(&self) {
        // Nothing.
    }

    /// Get the attributes of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    fn getattr(&self, _req: RequestInfo, _path: T, _fh: Option<u64>) -> ResultEntry<Attr> {
        Err(libc::ENOSYS)
    }

    // The following operations in the FUSE C API are all one kernel call: setattr
    // We split them out to match the C API's behavior.

    /// Change the mode of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `mode`: the mode to change the file to.
    fn chmod(&self, _req: RequestInfo, _path: T, _fh: Option<u64>, _mode: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Change the owner UID and/or group GID of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `uid`: user ID to change the file's owner to. If `None`, leave the UID unchanged.
    /// * `gid`: group ID to change the file's group to. If `None`, leave the GID unchanged.
    fn chown(&self, _req: RequestInfo, _path: T, _fh: Option<u64>, _uid: Option<u32>, _gid: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set the length of a file.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `size`: size in bytes to set as the file's length.
    fn truncate(&self, _req: RequestInfo, _path: T, _fh: Option<u64>, _size: u64) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set timestamps of a filesystem entry.
    ///
    /// * `fh`: a file handle if this is called on an open file.
    /// * `atime`: the time of last access.
    /// * `mtime`: the time of last modification.
    fn utimens(&self, _req: RequestInfo, _path: T, _fh: Option<u64>, _atime: Option<SystemTime>, _mtime: Option<SystemTime>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Set timestamps of a filesystem entry (with extra options only used on MacOS).
    #[allow(clippy::too_many_arguments)]
    fn utimens_macos(&self, _req: RequestInfo, _path: T, _fh: Option<u64>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<u32>) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // END OF SETATTR FUNCTIONS

    /// Read a symbolic link.
    fn readlink(&self, _req: RequestInfo, _path: T) -> ResultData {
        Err(libc::ENOSYS)
    }

    /// Create a special file.
    ///
    /// * `parent`: path to the directory to make the entry under.
    /// * `name`: name of the entry.
    /// * `mode`: mode for the new entry.
    /// * `rdev`: if mode has the bits `S_IFCHR` or `S_IFBLK` set, this is the major and minor numbers for the device file. Otherwise it should be ignored.
    fn mknod(&self, _req: RequestInfo, _parent: T, _name: &OsStr, _mode: u32, _rdev: u32) -> ResultEntry<Attr> {
        Err(libc::ENOSYS)
    }

    /// Create a directory.
    ///
    /// * `parent`: path to the directory to make the directory under.
    /// * `name`: name of the directory.
    /// * `mode`: permissions for the new directory.
    fn mkdir(&self, _req: RequestInfo, _parent: T, _name: &OsStr, _mode: u32) -> ResultEntry<Attr> {
        Err(libc::ENOSYS)
    }

    /// Remove a file.
    ///
    /// * `parent`: path to the directory containing the file to delete.
    /// * `name`: name of the file to delete.
    fn unlink(&self, _req: RequestInfo, _parent: T, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Remove a directory.
    ///
    /// * `parent`: path to the directory containing the directory to delete.
    /// * `name`: name of the directory to delete.
    fn rmdir(&self, _req: RequestInfo, _parent: T, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Create a symbolic link.
    ///
    /// * `parent`: path to the directory to make the link in.
    /// * `name`: name of the symbolic link.
    /// * `target`: path (may be relative or absolute) to the target of the link.
    fn symlink(&self, _req: RequestInfo, _parent: T, _name: &OsStr, _target: &Path) -> ResultEntry<Attr> {
        Err(libc::ENOSYS)
    }

    /// Rename a filesystem entry.
    ///
    /// * `parent`: path to the directory containing the existing entry.
    /// * `name`: name of the existing entry.
    /// * `newparent`: path to the directory it should be renamed into (may be the same as `parent`).
    /// * `newname`: name of the new entry.
    fn rename(&self, _req: RequestInfo, _parent: T, _name: &OsStr, _newparent: T, _newname: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Create a hard link.
    ///
    /// * `path`: path to an existing file.
    /// * `newparent`: path to the directory for the new link.
    /// * `newname`: name for the new link.
    fn link(&self, _req: RequestInfo, _path: T, _newparent: T, _newname: &OsStr) -> ResultEntry<Attr> {
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
    fn open(&self, _req: RequestInfo, _path: T, _flags: u32) -> ResultOpen {
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
    /// * `callback`: a callback that must be invoked to return the result of the operation: either
    ///    the result data as a slice, or an error code.
    ///
    /// Return the return value from the `callback` function.
    fn read(&self, _req: RequestInfo, _path: T, _fh: u64, _offset: u64, _size: u32, callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult) -> CallbackResult {
        callback(Err(libc::ENOSYS))
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
    fn write(&self, _req: RequestInfo, _path: T, _fh: u64, _offset: u64, _data: Vec<u8>, _flags: u32) -> ResultWrite {
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
    fn flush(&self, _req: RequestInfo, _path: T, _fh: u64, _lock_owner: u64) -> ResultEmpty {
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
    fn release(&self, _req: RequestInfo, _path: T, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Write out any pending changes of a file.
    ///
    /// When this returns, data should be written to persistent storage.
    ///
    /// * `path`: path to the file.
    /// * `fh`: file handle returned from the `open` call.
    /// * `datasync`: if `false`, also write metadata, otherwise just write file data.
    fn fsync(&self, _req: RequestInfo, _path: T, _fh: u64, _datasync: bool) -> ResultEmpty {
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
    fn opendir(&self, _req: RequestInfo, _path: T, _flags: u32) -> ResultOpen {
        Err(libc::ENOSYS)
    }

    /// Get the entries of a directory.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    ///
    /// Return all the entries of the directory.
    fn readdir(&self, _req: RequestInfo, _path: T, _fh: u64) -> ResultReaddir {
        Err(libc::ENOSYS)
    }

    /// Close an open directory.
    ///
    /// This will be called exactly once for each `opendir` call.
    ///
    /// * `path`: path to the directory.
    /// * `fh`: file handle returned from the `opendir` call.
    /// * `flags`: the file access flags passed to the `opendir` call.
    fn releasedir(&self, _req: RequestInfo, _path: T, _fh: u64, _flags: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Write out any pending changes to a directory.
    ///
    /// Analogous to the `fsync` call.
    fn fsyncdir(&self, _req: RequestInfo, _path: T, _fh: u64, _datasync: bool) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Get filesystem statistics.
    ///
    /// * `path`: path to some folder in the filesystem.
    ///
    /// See the `Statfs` struct for more details.
    fn statfs(&self, _req: RequestInfo, _path: T) -> ResultStatfs {
        Err(libc::ENOSYS)
    }

    /// Set a file extended attribute.
    ///
    /// * `path`: path to the file.
    /// * `name`: attribute name.
    /// * `value`: the data to set the value to.
    /// * `flags`: can be either `XATTR_CREATE` or `XATTR_REPLACE`.
    /// * `position`: offset into the attribute value to write data.
    fn setxattr(&self, _req: RequestInfo, _path: T, _name: &OsStr, _value: &[u8], _flags: u32, _position: u32) -> ResultEmpty {
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
    fn getxattr(&self, _req: RequestInfo, _path: T, _name: &OsStr, _size: u32) -> ResultXattr {
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
    fn listxattr(&self, _req: RequestInfo, _path: T, _size: u32) -> ResultXattr {
        Err(libc::ENOSYS)
    }

    /// Remove an extended attribute for a file.
    ///
    /// * `path`: path to the file.
    /// * `name`: name of the attribute to remove.
    fn removexattr(&self, _req: RequestInfo, _path: T, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Check for access to a file.
    ///
    /// * `path`: path to the file.
    /// * `mask`: mode bits to check for access to.
    ///
    /// Return `Ok(())` if all requested permissions are allowed, otherwise return `Err(EACCES)`
    /// or other error code as appropriate (e.g. `ENOENT` if the file doesn't exist).
    fn access(&self, _req: RequestInfo, _path: T, _mask: u32) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    /// Create and open a new file.
    ///
    /// * `parent`: path to the directory to create the file in.
    /// * `name`: name of the file to be created.
    /// * `mode`: the mode to set on the new file.
    /// * `flags`: flags like would be passed to `open`.
    ///
    /// Return a `CreatedEntry` (which contains the new file's attributes as well as a file handle
    /// -- see documentation on `open` for more info on that).
    fn create(&self, _req: RequestInfo, _parent: T, _name: &OsStr, _mode: u32, _flags: u32) -> ResultCreate<Attr> {
        Err(libc::ENOSYS)
    }

    // getlk

    // setlk

    // bmap

    /// macOS only: Rename the volume.
    ///
    /// * `name`: new name for the volume
    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: RequestInfo, _name: &OsStr) -> ResultEmpty {
        Err(libc::ENOSYS)
    }

    // exchange (macOS only, undocumented)

    /// macOS only: Query extended times (bkuptime and crtime).
    ///
    /// * `path`: path to the file to get the times for.
    ///
    /// Return an `XTimes` struct with the times, or other error code as appropriate.
    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: RequestInfo, _path: T) -> ResultXTimes {
        Err(libc::ENOSYS)
    }
}

/// Extension trait for `FilesystemMT` to allow for unmanaged file systems where fuse-mt
/// does not handle inode allocation and management.
///
/// This trait provides methods for filesystem operations that are not automatically
/// managed by fuse-mt, such as inode lookup, forgetting inodes, and retrieving parent
/// inodes. Implementing this trait allows for more control over the filesystem's behavior
/// in scenarios where manual inode management is necessary.
pub trait RawFilesystemMT: for <'a> FilesystemMT<'a, Inode, RawFileAttr> {
    /// Performs a lookup operation for a given file name within a parent inode.
    ///
    /// This method is used to find a file or directory by its name within a parent directory.
    /// It returns the attributes of the found file or directory, or an error if the lookup fails.
    ///
    /// # Arguments
    ///
    /// * `_req` - The request information.
    /// * `_parent` - The parent inode.
    /// * `_name` - The name of the file or directory to look up.
    ///
    /// # Returns
    ///
    /// * `ResultEntry<RawFileAttr>` - The result of the lookup operation, containing the
    ///   attributes of the found file or directory, or an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `fs` is an instance of a struct implementing `RawFilesystemMT`
    /// let req = RequestInfo::new(); // Example request info
    /// let parent_inode = Inode::new(1); // Example parent inode
    /// let name = OsStr::new("example.txt"); // Example file name
    ///
    /// let result = fs.lookup(req, parent_inode, name);
    /// match result {
    ///     Ok(entry) => println!("Found file with attributes: {:?}", entry.attrs),
    ///     Err(e) => println!("Lookup failed: {:?}", e),
    /// }
    /// ```
    fn lookup(&self, _req: RequestInfo, _parent: Inode, _name: &OsStr) -> ResultEntry<RawFileAttr>;

    /// Forgets a previously looked-up inode.
    ///
    /// This method is used to inform the filesystem that a previously looked-up inode is no
    /// longer needed. This can be used to free up resources associated with the inode.
    ///
    /// # Arguments
    ///
    /// * `_req` - The request information.
    /// * `_path` - The inode to forget.
    /// * `_nlookup` - The number of lookups to forget.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `fs` is an instance of a struct implementing `RawFilesystemMT`
    /// let req = RequestInfo::new(); // Example request info
    /// let inode_to_forget = Inode::new(1); // Example inode to forget
    /// let nlookup = 1; // Number of lookups to forget
    ///
    /// fs.forget(req, inode_to_forget, nlookup);
    /// ```
    fn forget(&self, _req: RequestInfo, _path: Inode, _nlookup: u64);

    /// Retrieves the parent inode of a given inode.
    ///
    /// This method is used to find the parent directory of a given inode. It returns the
    /// parent inode, or an error if the operation fails.
    ///
    /// This is only ever really used for readdir() at the moment.
    ///
    /// # Arguments
    ///
    /// * `_req` - The request information.
    /// * `_path` - The inode for which to find the parent.
    ///
    /// # Returns
    ///
    /// * `ResultInode` - The result of the operation, containing the parent inode, or an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Assuming `fs` is an instance of a struct implementing `RawFilesystemMT`
    /// let req = RequestInfo::new(); // Example request info
    /// let inode = Inode::new(1); // Example inode
    ///
    /// let result = fs.parent(req, inode);
    /// match result {
    ///     Ok(parent_inode) => println!("Parent inode: {:?}", parent_inode),
    ///     Err(e) => println!("Failed to find parent: {:?}", e),
    /// }
    /// ```
    fn parent(&self, _req: RequestInfo, _path: Inode) -> ResultInode;
}