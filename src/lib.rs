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
// Copyright (c) 2016-2020 by William R. Fraser
//

#![deny(rust_2018_idioms)]

#[macro_use]
extern crate libc;


mod directory_cache;
mod fusemt;
mod inode_table;
mod types;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::ffi::OsStr;
use std::io;

pub use fuser::{FileType, mount2, spawn_mount, MountOption};
pub use crate::fusemt::*;
pub use crate::types::*;


/// Mounts a filesystem
// A wrapper around fuser::mount, since fuser::mount is deprecated
// and we will wrap it ourselves later, we don't want it bleeding out.
#[inline]
pub fn mount<FS, P>(filesystem: FS, mount_point: P, options: &[&OsStr]) -> io::Result<()>
where
    FS: fuser::Filesystem,
    P: AsRef<std::path::Path> {
		#[allow(deprecated)]
		fuser::mount(filesystem, mount_point, options)
}
