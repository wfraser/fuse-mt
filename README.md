# FUSE-MT

This code is a wrapper on top of the Rust FUSE crate that aims to:
* Dispatch system calls on multiple threads, so that e.g. I/O doesn't block directory listing.
* Translate inodes into paths, to simplify filesystem implementation.

This is a work-in-progress. I'm going to get the inode/path translation working first, then make it multithreaded.

Some random notes on the implementation:
* The trait that filesystems will implement is called `PathFilesystem`, and instead of the FUSE crate's convention of having methods return void and including a "reply" parameter, the methods return their values. This feels more idiomatic to me.
* The inode/path translation is going to be done on the main thread, so no locking should be needed in inode_translator.
