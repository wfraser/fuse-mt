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

/// Mount the given filesystem to the given mountpoint. This function will not return until the
/// filesystem is unmounted.
#[inline(always)]
pub fn mount<FS: fuser::Filesystem, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[&OsStr],
) -> io::Result<()> {
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
    #[allow(deprecated)]
    fuser::spawn_mount(fs, mountpoint, options)
}
