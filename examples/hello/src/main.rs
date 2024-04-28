use std::env;
use std::ffi::{OsStr, OsString};
use std::time::{Duration, SystemTime};

use fuse_mt::{CallbackResult, DirectoryEntry, FileAttr, FilesystemMT, FileType, Inode, RawFileAttr, RawFilesystemMT, RequestInfo, ResultEmpty, ResultEntry, ResultInode, ResultOpen, ResultReaddir, ResultSlice, ResultStatfs, Statfs};

#[derive(Debug)]
struct HelloFS {
    ttl: Duration,
    bootup: SystemTime
}

impl HelloFS {
    const ROOT_INODE: Inode = 1;
    const HELLO_INODE: Inode = 2;
    const HELLO_NAME: &'static str = "hello.txt";
    const HELLO_CONTENT: &'static [u8] = b"Hello World, this is fuse-mt!\x0a";

    fn root_attr(&self, uid: u32, gid: u32) -> FileAttr {
        FileAttr {
            size: 0,
            blocks: 0,
            atime: self.bootup,
            mtime: self.bootup,
            ctime: self.bootup,
            crtime: self.bootup,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid,
            gid,
            rdev: 0,
            flags: 0,
        }
    }

    fn hello_attr(&self, uid: u32, gid: u32) -> FileAttr {
        FileAttr {
            size: Self::HELLO_CONTENT.len() as u64,
            blocks: 1,
            atime: self.bootup,
            mtime: self.bootup,
            ctime: self.bootup,
            crtime: self.bootup,
            kind: FileType::RegularFile,
            perm: 0o755,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            flags: 0,
        }
    }
}

impl FilesystemMT<'_, Inode, RawFileAttr> for HelloFS {
    fn getattr(&self, req: RequestInfo, path: Inode, _fh: Option<u64>) -> ResultEntry<RawFileAttr> {
        match path {
            Self::ROOT_INODE => Ok((self.ttl, self.root_attr(req.uid, req.gid).as_raw(Self::ROOT_INODE, 0))),
            Self::HELLO_INODE => Ok((self.ttl, self.hello_attr(req.uid, req.gid).as_raw(Self::HELLO_INODE, 0))),
            _ => Err(libc::ENOENT)
        }
    }

    fn open(&self, _req: RequestInfo, path: Inode, _flags: u32) -> ResultOpen {
        match path {
            Self::HELLO_INODE => Ok((2, 0)),
            _ => Err(libc::ENOENT)
        }
    }

    fn read(&self, _req: RequestInfo, path: Inode, _fh: u64, offset: u64, size: u32, callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult) -> CallbackResult {
        let result = if path == Self::HELLO_INODE {
            let start = offset as usize;
            let end = start + size as usize;

            Ok(&Self::HELLO_CONTENT[start..end])
        } else {
            Err(libc::ENOENT)
        };

        callback(result)
    }

    fn opendir(&self, _req: RequestInfo, path: Inode, _flags: u32) -> ResultOpen {
        match path {
            Self::ROOT_INODE => Ok((1, 0)),
            _ => Err(libc::ENOTDIR)
        }
    }

    fn readdir(&self, _req: RequestInfo, path: Inode, _fh: u64) -> ResultReaddir {
        if path != 1 {
            return Err(libc::ENOENT);
        }

         Ok(vec![
             DirectoryEntry {
                 name: ".".into(),
                 kind: FileType::Directory,
             },
             DirectoryEntry {
                 name: "..".into(),
                 kind: FileType::Directory,
             },
             DirectoryEntry {
                 name: "hello.txt".into(),
                 kind: FileType::Directory,
             }
        ])
    }

    fn releasedir(&self, _req: RequestInfo, _path: Inode, _fh: u64, _flags: u32) -> ResultEmpty {
        Ok(())
    }

    fn statfs(&self, _req: RequestInfo, path: Inode) -> ResultStatfs {
        if path != Self::ROOT_INODE {
            return Err(libc::ENOENT);
        }

        Ok(Statfs {
            blocks: 1,
            bfree: 0,
            bavail: 0,
            files: 2,
            ffree: 0,
            bsize: 512,
            namelen: 512,
            frsize: 512,
        })
    }
}

impl RawFilesystemMT for HelloFS {
    fn lookup(&self, req: RequestInfo, parent: Inode, name: &OsStr) -> ResultEntry<RawFileAttr> {
        if parent != Self::ROOT_INODE {
            return Err(libc::ENOENT);
        }

        if let Some(Self::HELLO_NAME) = name.to_str() {
            let attr: RawFileAttr = self.hello_attr(req.uid, req.gid).as_raw(Self::HELLO_INODE, 0);

            Ok((self.ttl, attr))
        } else {
            Err(libc::ENOENT)
        }
    }

    fn forget(&self, _req: RequestInfo, _path: Inode, _nlookup: u64) {
    }

    fn parent(&self, _req: RequestInfo, path: Inode) -> ResultInode {
        match path {
            Self::ROOT_INODE | Self::HELLO_INODE => Ok(Self::ROOT_INODE),
            _ => Err(libc::ENOENT)
        }
    }
}

struct ConsoleLogger;

impl log::Log for ConsoleLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        println!("{}: {}: {}", record.target(), record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: ConsoleLogger = ConsoleLogger;

fn main() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Debug);

    let args: Vec<OsString> = env::args_os().collect();

    if args.len() != 2 {
        println!("usage: {} <mountpoint>", &env::args().next().unwrap());
        std::process::exit(-1);
    }

    let filesystem = HelloFS {
        ttl: Duration::from_secs(1),
        bootup: SystemTime::now()
    };

    let fuse_args = [OsStr::new("-o"), OsStr::new("fsname=hellofs"), OsStr::new("-o"), OsStr::new("ro")];

    fuse_mt::mount(fuse_mt::RawFuseMT::new(filesystem, 1), &args[1], &fuse_args[..]).unwrap();
}
