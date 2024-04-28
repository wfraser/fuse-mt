A simple test filesystem showcasing the unmanaged capabilities of fuse-mt.

It implements a filesystem that serves a single file: `hello.txt`.

Just enough fuse operations are implemented to interact with the file system using `ls`, `cat` and the likes.

To use it and test fuse_mt, run:

    cargo run <mount point>