# FUSE-MT

![Build Status](https://github.com/wfraser/fuse-mt/workflows/Cargo%20Checks/badge.svg)
[![Crates.io](https://img.shields.io/crates/v/fuse_mt.svg)](https://crates.io/crates/fuse_mt)

[Documentation](https://docs.rs/fuse_mt)

This code is a wrapper on top of the Rust FUSE crate with the following additions:
* Dispatch system calls on multiple threads, so that e.g. I/O doesn't block directory listing.
* Translate inodes into paths, to simplify filesystem implementation.

The `fuser` crate provides a minimal, low-level access to the FUSE kernel API, whereas this crate is more high-level, like the FUSE C API.

It includes a sample filesystem that uses the crate to pass all system calls through to another filesystem at any arbitrary path.

This is a work-in-progress. Bug reports, pull requests, and other feedback are welcome!

Some random notes on the implementation:
* The trait that filesystems will implement is called `FilesystemMT`, and instead of the FUSE crate's convention of having methods return void and including a "reply" parameter, the methods return their values. This feels more idiomatic to me. They also take `&Path` arguments instead of inode numbers.
* Currently, only the following calls are dispatched to other threads:
    * read
    * write
    * flush
    * fsync
    * ioctl
* Other calls run synchronously on the main thread because either it is expected that they will complete quickly and/or they require mutating internal state of the InodeTranslator and I want to avoid needing locking in there.
* The inode/path translation is always done on the main thread.
* It might be a good idea to limit the number of concurrent read and write operations in flight. I'm not sure yet how many outstanding read/write requests FUSE will issue though, so it might be a non-issue.
