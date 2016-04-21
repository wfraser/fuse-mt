This is a sample program that uses fuse_mt.

It implements a filesystem that forwards all requests to another filesystem at any arbitrary location.

To use it and test fuse_mt, run:

    cargo run <path to filesystem> <mount point>

Unmount it with `fusermount -u <mount point>` or just CTRL-C the running program.
