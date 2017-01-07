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
