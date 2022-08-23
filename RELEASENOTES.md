v0.6.0: 2022-07-12
  * Changed underlying FUSE crate to [`fuser`](https://github.com/cberner/fuser) v0.11
    * Note that fuser has some additional capabilities which are not yet being exposed in this
      crate's API. These may be added in future releases.
  * breaking change: replaced `time` crate's `Timespec` with
    `std::time::SystemTime` and `std::time::Duration`.
  * breaking change: destroy() no longer takes an argument.

v0.5.1: 2020-08-16
  * Changed FilesystemMT::init default impl to succeed instead of error.

v0.5.0: 2019-05-11
  * Changed read() to use a callback instead of having implementations return data by value.
    * This allows implementations to avoid allocating memory in some cases.

v0.4.4: 2018-02-18
  * Implemented `getxtimes` and `setvolname` for macOS

v0.4.3: 2017-11-08
  * Implemented socket file type support from rust-fuse.
  * u64 -> i64 offset type changed in rust-fuse; fuse-mt's type is unchanged.

v0.4.2: 2017-10-30
  * Fixed a bug that caused 'forget' log messages on stdout.

v0.4.1: 2017-06-06
  * Added basic derives (Clone, Copy, Debug) for types as appropriate.

v0.4.0: 2017-05-29
  * Removed `FilesystemMT::lookup`. See #10.
  * Removed the `ino` field of `FileAttr`. See #12.

v0.3.0: 2017-02-01
  * Merged the `generation-managed` branch.
      * The inode table now keeps track of when it re-uses an inode.
      * This is a breaking change because the type signature of `ResultEntry` was changed to not
        have a `generation` member. This affects the `lookup`, `mknod`, `mkdir`, `symlink`,
        `link`, and `create` calls.
  * Added some tests for the inode table.

v0.2.2: 2017-01-13
  * fixed a build error on 32-bit Linux.
  * added a `VERSION` public const string with the fuse_mt package version.

v0.2.1: 2017-01-09
  * Added lots of documentation.
  * Implemented `access`, `setxattr`
  * Delay threadpool creation until it is actually used.
  * Added `setxattr`, `removexattr` in passthrufs.
  * Build fixes for MacOS.

v0.2.0: 2017-01-06
  * Merged the `lookup-refcount` branch.
      * The inode table no longer grows without bound. :)
  * Fixed readdir() so that filesystems don't need to handle the `offset` parameter at all.

v0.1.2: 2017-01-06
  * Fixed a bug in mknod(): the inode was not set in the response, nor was it added to the inode
    table.
  * Updated to rust-fuse v0.3.0
  * First release on crates.io.

v0.1.1: 2017-01-06
  * (accidental release of experimental branch; yanked)

v0.1.0: 2017-01-04
  * initial release, not yet on crates.io
