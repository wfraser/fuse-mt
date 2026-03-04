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
//!
//! # Mounting
//!
//! Use [`mount`] for a simple blocking mount with legacy `&[&OsStr]` options, or
//! [`mount_with_config`] / [`spawn_mount_with_config`] for full access to the
//! `fuser` 0.17+ [`fuser::Config`] API (multi-threaded event loops, ACL policy, etc.).
//!
//! **Note**: [`fuser::BackgroundSession::join`] has been renamed to
//! `umount_and_join` in `fuser` 0.17. Update any downstream code that calls `.join()` on the
//! session handle returned by [`spawn_mount`] or [`spawn_mount_with_config`].

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

use std::ffi::OsStr;
use std::io;
use std::path::Path;

// ---------------------------------------------------------------------------
// Legacy convenience wrappers (parse the traditional "-o key=value" options)
// ---------------------------------------------------------------------------

/// Build a [`fuser::Config`] from a traditional `&[&OsStr]` options slice.
///
/// Recognises common `-o key[=value]` entries such as `fsname=…`, `allow_other`,
/// `allow_root`, `auto_unmount`, `default_permissions`, `dev`, `nodev`, `suid`,
/// `nosuid`, `ro`, `rw`, `exec`, `noexec`, `atime`, `noatime`, `dirsync`,
/// and `sync`. Unknown options are silently ignored.
fn config_from_legacy_options(options: &[&OsStr]) -> fuser::Config {
    use fuser::{MountOption, SessionACL};

    let mut config = fuser::Config::default();

    let mut i = 0;
    while i < options.len() {
        let opt = options[i];
        if opt == OsStr::new("-o") {
            i += 1;
            if i >= options.len() {
                break;
            }
            let val = options[i].to_string_lossy();
            for part in val.split(',') {
                let part = part.trim();
                if let Some(name) = part.strip_prefix("fsname=") {
                    config.mount_options.push(MountOption::FSName(name.to_owned()));
                } else {
                    match part {
                        // allow_other / allow_root are now ACL policy in fuser 0.17
                        "allow_other" => { config.acl = SessionACL::All; }
                        "allow_root" => { config.acl = SessionACL::RootAndOwner; }
                        "auto_unmount" => { config.mount_options.push(MountOption::AutoUnmount); }
                        "default_permissions" => { config.mount_options.push(MountOption::DefaultPermissions); }
                        "dev" => { config.mount_options.push(MountOption::Dev); }
                        "nodev" => { config.mount_options.push(MountOption::NoDev); }
                        "suid" => { config.mount_options.push(MountOption::Suid); }
                        "nosuid" => { config.mount_options.push(MountOption::NoSuid); }
                        "ro" => { config.mount_options.push(MountOption::RO); }
                        "rw" => { config.mount_options.push(MountOption::RW); }
                        "exec" => { config.mount_options.push(MountOption::Exec); }
                        "noexec" => { config.mount_options.push(MountOption::NoExec); }
                        "atime" => { config.mount_options.push(MountOption::Atime); }
                        "noatime" => { config.mount_options.push(MountOption::NoAtime); }
                        "dirsync" => { config.mount_options.push(MountOption::DirSync); }
                        "sync" => { config.mount_options.push(MountOption::Sync); }
                        _ => {}
                    }
                }
            }
        }
        i += 1;
    }

    config
}

/// Mount the given filesystem to the given mountpoint. This function will not return until the
/// filesystem is unmounted.
///
/// This is a legacy convenience wrapper. Options are specified as a slice of `&OsStr` using the
/// traditional `-o key[=value]` format. For full control over mount configuration use
/// [`mount_with_config`] instead.
#[inline(always)]
pub fn mount<FS: fuser::Filesystem, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[&OsStr],
) -> io::Result<()> {
    let config = config_from_legacy_options(options);
    fuser::mount2(fs, mountpoint, &config)
}

/// Mount the given filesystem to the given mountpoint. This function spawns a background thread to
/// handle filesystem operations while being mounted and therefore returns immediately. The
/// returned handle should be stored to reference the mounted filesystem. If it's dropped, the
/// filesystem will be unmounted.
///
/// This is a legacy convenience wrapper. Options are specified as a slice of `&OsStr` using the
/// traditional `-o key[=value]` format. For full control over mount configuration use
/// [`spawn_mount_with_config`] instead.
///
/// **Note**: [`fuser::BackgroundSession::join`] has been renamed to `umount_and_join` in
/// `fuser` 0.17. Update any code that calls `.join()` on the returned session.
#[inline(always)]
pub fn spawn_mount<FS: fuser::Filesystem + Send + 'static, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    options: &[&OsStr],
) -> io::Result<fuser::BackgroundSession> {
    let config = config_from_legacy_options(options);
    fuser::spawn_mount2(fs, mountpoint, &config)
}

// ---------------------------------------------------------------------------
// Modern Config-based mount API
// ---------------------------------------------------------------------------

/// Mount the given filesystem to the given mountpoint using a [`fuser::Config`].
///
/// This function will not return until the filesystem is unmounted. Use a [`fuser::Config`]
/// to opt into `fuser` 0.17+ features such as multi-threaded event loops
/// (`Config::n_threads`), ACL policy (`Config::acl`), and `clone_fd` support.
#[inline(always)]
pub fn mount_with_config<FS: fuser::Filesystem, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    config: &fuser::Config,
) -> io::Result<()> {
    fuser::mount2(fs, mountpoint, config)
}

/// Mount the given filesystem to the given mountpoint using a [`fuser::Config`], spawning a
/// background thread to handle requests. Returns immediately; drop the handle to unmount.
///
/// Use a [`fuser::Config`] to opt into `fuser` 0.17+ features such as multi-threaded event
/// loops (`Config::n_threads`), ACL policy (`Config::acl`), and `clone_fd` support.
///
/// **Note**: [`fuser::BackgroundSession::join`] has been renamed to `umount_and_join` in
/// `fuser` 0.17. Update any code that calls `.join()` on the returned session.
#[inline(always)]
pub fn spawn_mount_with_config<FS: fuser::Filesystem + Send + 'static, P: AsRef<Path>>(
    fs: FS,
    mountpoint: P,
    config: &fuser::Config,
) -> io::Result<fuser::BackgroundSession> {
    fuser::spawn_mount2(fs, mountpoint, config)
}
