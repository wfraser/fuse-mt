//! FuseMT -- A higher-level FUSE (Filesystem in Userspace) interface and wrapper around the
//! low-level `fuser` library that makes implementing a filesystem a bit easier.
//!
//! FuseMT translates inodes to paths and dispatches I/O operations to multiple threads, and
//! simplifies some details of filesystem implementation, for example: splitting the `setattr` call
//! into multiple separate operations, and simplifying the `readdir` call so that filesystems don't
//! need to deal with pagination.
//!
//! To implement a filesystem, implement the `FilesystemMT` trait. Not all functions in it need to
//! be implemented -- the default behavior is to return `ENOSYS` ("Function not implemented"). For
//! example, a read-only filesystem can skip implementing the `write` call and many others.

//
// Copyright (c) 2016-2022 by William R. Fraser
//

#![deny(rust_2018_idioms)]

#[macro_use]
extern crate log;

mod directory_cache;
mod fusemt;
mod inode_table;
mod types;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub use fuser::FileType;
pub use crate::fusemt::*;
pub use crate::types::*;

// Forward to similarly-named fuser functions to work around deprecation for now.
// When these are removed, we'll have to either reimplement or break reverse compat.
// Keep the doc comments in sync with those in fuser.

use std::ffi::OsStr;
use std::io;
use std::path::Path;

include!(concat!(env!("OUT_DIR"), "/ver.include"));

// Previous implementations passed the options strings directly to fuse, which would print help and
// version messages when appropriate options were given. Currently fuser parses the options to
// symbolic constants first, which doesn't support this behavior. This cuntion exists to print that
// text ourselves and retain compatibility.
fn check_options(opts: &[&OsStr]) -> bool {
    let mut skip = false;
    for opt in opts {
        match opt.to_str() {
            _ if skip => {
                skip = false;
            }
            Some("-h") | Some("--help") => {
                // This was the help message from fusermount:
                eprintln!("    -o allow_other         allow access to other users");
                eprintln!("    -o allow_root          allow access to root");
                eprintln!("    -o auto_unmount        auto unmount on process termination");
                eprintln!("    -o nonempty            allow mounts on non-empty file/dir");
                eprintln!("    -o default_permissions enable permission checking by kernel");
                eprintln!("    -o fsname=NAME         set filesystem name");
                eprintln!("    -o subtype=NAME        set filesystem type");
                eprintln!("    -o large_read          issue large read requests (2.4 only)");
                eprintln!("    -o max_read=N          set maximum size of read requests");

                // Additional options recognized by fuser:
                eprintln!("    -o [no]dev             [dis]allow special character and block devices");
                eprintln!("    -o [no]suid            [dis]allow set-user-id and set-group-id bits on files");
                eprintln!("    -o ro                  read-only filesystem");
                eprintln!("    -o rw                  read-write filesystem");
                eprintln!("    -o [no]exec            [dis]allow execution of binaries");
                eprintln!("    -o [no]atime           enable/disable inode access time");
                eprintln!("    -o dirsync             all modifications to directories will be done synchronously");
                eprintln!("    -o sync                all I/O will be done synchronously");
                eprintln!("    -o async               all I/O will be done asynchronously");
                return false;
            }
            Some("-V") | Some("--version") => {
                eprintln!("FuseMT version {}", env!("CARGO_PKG_VERSION"));
                eprintln!("fuser version {}", FUSER_VER);
                return false;
            }
            Some("--") => {
                // stop looking
                return true;
            }
            Some("-o") => {
                skip = true;
            }
            _ => (),
        }
    }
    true
}

/// Mount the given filesystem to the given mountpoint. This function will not return until the
/// filesystem is unmounted.
#[inline(always)]
pub fn mount<FS: fuser::Filesystem, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[&OsStr],
) -> io::Result<()> {
    if !check_options(options) {
        return Ok(());
    }

    #[allow(deprecated)]
    fuser::mount(fs, mountpoint, options)
}

/// Mount the given filesystem to the given mountpoint. This function spawns a background thread to
/// handle filesystem operations while being mounted and therefore returns immediately. The
/// returned handle should be stored to reference the mounted filesystem. If it's dropped, the
/// filesystem will be unmounted.
#[inline(always)]
pub fn spawn_mount<FS: fuser::Filesystem + Send + 'static, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[&OsStr],
) -> io::Result<fuser::BackgroundSession> {
    check_options(options);
    // need to return a BackgroundSession, so attempt the mount regardless of what check_options
    // returns.

    #[allow(deprecated)]
    fuser::spawn_mount(fs, mountpoint, options)
}
