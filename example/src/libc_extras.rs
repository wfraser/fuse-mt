// libc_extras :: Functions missing from the libc crate and wrappers for better cross-platform
//                compatibility.
//
// Copyright (c) 2016 by William R. Fraser
//

pub mod libc {
    #![allow(non_camel_case_types)]

    pub use ::libc::*;

    // stuff missing from the libc crate.
    extern "system" {
        // Specified by POSIX.1-2008; not sure why this is missing.
        pub fn fchown(fd: c_int, uid: uid_t, gid: gid_t) -> c_int;

        // On Mac OS X, off_t is always 64 bits.
        // https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/64bitPorting/transition/transition.html
        #[cfg(target_os = "macos")]
        pub fn truncate(path: *const c_char, size: off_t) -> c_int;

        // On Linux, off_t is architecture-dependent, and this is provided for 32-bit systems:
        #[cfg(target_os = "linux")]
        pub fn truncate64(path: *const c_char, size: off64_t) -> c_int;

        // These XATTR functions are missing from the libc crate on Darwin for some reason.
        #[cfg(target_os = "macos")]
        pub fn listxattr(path: *const c_char, list: *mut c_char, size: size_t, options: c_int) -> ssize_t;

        #[cfg(target_os = "macos")]
        pub fn getxattr(path: *const c_char, name: *const c_char, value: *mut c_void, size: size_t, position: u32, options: c_int) -> ssize_t;

        #[cfg(target_os = "macos")]
        pub fn setxattr(path: *const c_char, name: *const c_char, value: *const c_void, size: size_t, flags: c_int, position: u32) -> c_int;

        #[cfg(target_os = "macos")]
        pub fn removexattr(path: *const c_char, name: *const c_char, flags: c_int) -> c_int;
    }

    //
    // Mac-Linux 64-bit compat
    //

    #[cfg(target_os = "macos")]
    pub type stat64 = stat;

    #[cfg(target_os = "macos")]
    pub unsafe fn lstat64(path: *const c_char, stat: *mut stat64) -> c_int {
        lstat(path, stat)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn fstat64(fd: c_int, stat: *mut stat64) -> c_int {
        fstat(fd, stat)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn ftruncate64(fd: c_int, length: i64) -> c_int {
        ftruncate(fd, length as off_t)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn truncate64(path: *const c_char, size: off_t) -> c_int {
        truncate(path, size)
    }

    #[cfg(target_os = "macos")]
    fn timespec_to_timeval(timespec: &timespec) -> timeval {
        timeval {
            tv_sec: timespec.tv_sec,
            tv_usec: timespec.tv_nsec as suseconds_t * 1000,
        }
    }

    pub const UTIME_OMIT: time_t = ((11 << 30) - 21);

    // Mac OS X does not support futimens; map it to futimes with lower precision.
    #[cfg(target_os = "macos")]
    pub unsafe fn futimens(fd: c_int, times: *const timespec) -> c_int {
        use super::super::libc_wrappers;
        let mut times_osx = [timespec_to_timeval(&*times),
                             timespec_to_timeval(&*times.offset(1))];

        let mut stat: Option<stat> = None;

        if (*times).tv_nsec == UTIME_OMIT {
            // atime is unspecified

            stat = match libc_wrappers::fstat(fd as u64) {
                Ok(s) => Some(s),
                Err(e) => return e,
            };

            times_osx[0].tv_sec = stat.unwrap().st_atime;
            times_osx[0].tv_usec = stat.unwrap().st_atime_nsec as suseconds_t * 1000;
        }

        if (*times.offset(1)).tv_nsec == UTIME_OMIT {
            // mtime is unspecified

            if stat.is_none() {
                stat = match libc_wrappers::fstat(fd as u64) {
                    Ok(s) => Some(s),
                    Err(e) => return e,
                };
            }

            times_osx[1].tv_sec = stat.unwrap().st_mtime;
            times_osx[1].tv_usec = stat.unwrap().st_mtime_nsec as suseconds_t * 1000;
        }

        futimes(fd, &times_osx as *const timeval)
    }

    // Mac OS X does not support utimensat; map it to lutimes with lower precision.
    // The relative path feature of utimensat is not supported by this workaround.
    #[cfg(target_os = "macos")]
    pub fn utimensat(_dirfd_ignored: c_int, path: *const c_char, times: *const timespec,
                     _flag_ignored: c_int) -> c_int {
        use super::super::libc_wrappers;
        unsafe {
            assert_eq!(*path, b'/' as c_char); // relative paths are not supported here!
            let mut times_osx = [timespec_to_timeval(&*times),
                                 timespec_to_timeval(&*times.offset(1))];

            let mut stat: Option<stat> = None;
            fn stat_if_needed(path: *const c_char, stat: &mut Option<stat>) -> Result<(), c_int> {
                use std::ffi::{CStr, OsString};
                use std::os::unix::ffi::OsStringExt;
                if stat.is_none() {
                    let path_c = unsafe { CStr::from_ptr(path) } .to_owned();
                    let path_os = OsString::from_vec(path_c.into_bytes());
                    *stat = Some(try!(libc_wrappers::lstat(path_os)));
                }
                Ok(())
            }

            if (*times).tv_nsec == UTIME_OMIT {
                // atime is unspecified

                if let Err(e) = stat_if_needed(path, &mut stat) {
                    return e;
                }

                times_osx[0].tv_sec = stat.unwrap().st_atime;
                times_osx[0].tv_usec = stat.unwrap().st_atime_nsec as suseconds_t * 1000;
            }

            if (*times.offset(1)).tv_nsec == UTIME_OMIT {
                // mtime is unspecified

                if stat.is_none() {
                    if let Err(e) = stat_if_needed(path, &mut stat) {
                        return e;
                    }
                }
                times_osx[1].tv_sec = stat.unwrap().st_mtime;
                times_osx[1].tv_usec = stat.unwrap().st_mtime_nsec as suseconds_t * 1000;
            }

            lutimes(path, &times_osx as *const timeval)
        }
    }

    // the value is ignored; this is for OS X compat
    #[cfg(target_os = "macos")]
    pub const AT_FDCWD: c_int = -100;

    // the value is ignored; this is for OS X compat
    #[cfg(target_os = "macos")]
    pub const AT_SYMLINK_NOFOLLOW: c_int = 0x400;

    #[cfg(target_os = "macos")]
    pub const XATTR_NOFOLLOW: c_int = 1;

    #[cfg(target_os = "macos")]
    pub unsafe fn llistxattr(path: *const c_char, namebuf: *mut c_char, size: size_t) -> ssize_t {
        listxattr(path, namebuf, size, XATTR_NOFOLLOW)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn lgetxattr(path: *const c_char, name: *const c_char, value: *mut c_void, size: size_t) -> ssize_t {
        getxattr(path, name, value, size, 0, XATTR_NOFOLLOW)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn lsetxattr(path: *const c_char, name: *const c_char, value: *const c_void, size: size_t, flags: c_int, position: u32) -> c_int {
        setxattr(path, name, value, size, flags | XATTR_NOFOLLOW, position)
    }

    #[cfg(target_os = "macos")]
    pub unsafe fn lremovexattr(path: *const c_char, name: *const c_char) -> c_int {
        removexattr(path, name, XATTR_NOFOLLOW)
    }
}
