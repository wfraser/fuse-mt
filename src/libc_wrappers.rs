// Libc Wrappers :: Safe wrappers around system calls.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::ffi::{CString, OsString};
use std::io;
use std::mem;
use std::ptr;
use std::os::unix::ffi::OsStringExt;
use libc;

pub fn opendir(path: OsString) -> Result<usize, libc::c_int> {
    let path_c = match CString::new(path.into_vec()) {
        Ok(s) => s,
        Err(e) => {
            error!("opendir: path {:?} contains interior NUL byte",
                   OsString::from_vec(e.into_vec()));
            return Err(libc::EINVAL);
        }
    };

    let dir: *mut libc::DIR = unsafe { libc::opendir(mem::transmute(path_c.as_ptr())) };
    if dir.is_null() {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(dir as usize)
}

pub fn readdir(fh: usize) -> Result<Option<libc::dirent>, libc::c_int> {
    let dir: *mut libc::DIR = unsafe { mem::transmute(fh) };
    let mut entry: libc::dirent = unsafe { mem::zeroed() };
    let mut result: *mut libc::dirent = ptr::null_mut();

    let error: i32 = unsafe { libc::readdir_r(dir, &mut entry, &mut result) };
    if error != 0 {
        return Err(error);
    }

    if result.is_null() {
        return Ok(None);
    }

    Ok(Some(entry))
}

pub fn closedir(fh: usize) -> Result<(), libc::c_int> {
    let dir: *mut libc::DIR = unsafe { mem::transmute(fh) };
    if -1 == unsafe { libc::closedir(dir) } {
        Err(io::Error::last_os_error().raw_os_error().unwrap())
    } else {
        Ok(())
    }
}

pub fn open(path: OsString, flags: libc::c_int) -> Result<usize, libc::c_int> {
    let path_c = match CString::new(path.into_vec()) {
        Ok(s) => s,
        Err(e) => {
            error!("open: path {:?} contains interior NUL byte",
                   OsString::from_vec(e.into_vec()));
            return Err(libc::EINVAL);
        }
    };

    let fd: libc::c_int = unsafe { libc::open(mem::transmute(path_c.as_ptr()), flags) };
    if fd == -1 {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(fd as usize)
}

pub fn close(fh: usize) -> Result<(), libc::c_int> {
    let fd = fh as libc::c_int;
    if -1 == unsafe { libc::close(fd) } {
        Err(io::Error::last_os_error().raw_os_error().unwrap())
    } else {
        Ok(())
    }
}

pub fn lstat(path: OsString) -> Result<libc::stat64, libc::c_int> {
    let path_c = match CString::new(path.into_vec()) {
        Ok(s) => s,
        Err(e) => {
            error!("lstat: path {:?} contains interior NUL byte",
                   OsString::from_vec(e.into_vec()));
            return Err(libc::EINVAL);
        }
    };

    let mut buf: libc::stat64 = unsafe { mem::zeroed() };
    if -1 == unsafe { libc::lstat64(mem::transmute(path_c.as_ptr()), &mut buf) } {
        return Err(io::Error::last_os_error().raw_os_error().unwrap());
    }

    Ok(buf)
}
