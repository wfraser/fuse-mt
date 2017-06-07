// InodeTable :: a bi-directional map of paths to inodes.
//
// Copyright (c) 2016-2017 by William R. Fraser
//

use std::borrow::Borrow;
use std::cmp::{Eq, PartialEq};
use std::collections::{HashMap, VecDeque};
use std::collections::hash_map::Entry::*;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub type Inode = u64;
pub type Generation = u64;
pub type LookupCount = u64;

#[derive(Debug)]
struct InodeTableEntry {
    path: Option<Arc<PathBuf>>,
    lookups: LookupCount,
    generation: Generation,
}

/// A data structure for mapping paths to inodes and vice versa.
#[derive(Debug)]
pub struct InodeTable {
    table: Vec<InodeTableEntry>,
    free_list: VecDeque<usize>,
    by_path: HashMap<Arc<PathBuf>, usize>,
}

impl InodeTable {
    /// Create a new inode table.
    ///
    /// inode table entries have a limited lifetime, controlled by a 'lookup count', which is
    /// manipulated with the `lookup` and `forget` functions.
    ///
    /// The table initially contains just the root directory ("/"), mapped to inode 1.
    /// inode 1 is special: it cannot be forgotten.
    pub fn new() -> InodeTable {
        let mut inode_table = InodeTable {
            table: Vec::new(),
            free_list: VecDeque::new(),
            by_path: HashMap::new()
        };
        let root = Arc::new(PathBuf::from("/"));
        inode_table.table.push(InodeTableEntry {
            path: Some(root.clone()),
            lookups: 0, // not used for this entry; root is always present.
            generation: 0,
        });
        inode_table.by_path.insert(root, 0);
        inode_table
    }

    /// Add a path to the inode table.
    ///
    /// Returns the inode number the path is now mapped to.
    /// The returned inode number may be a re-used number formerly assigned to a now-forgotten
    /// path.
    ///
    /// The path is added with an initial lookup count of 1.
    ///
    /// This operation runs in O(log n) time.
    pub fn add(&mut self, path: Arc<PathBuf>) -> (Inode, Generation) {
        let (inode, generation) = {
            let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
            entry.path = Some(path.clone());
            entry.lookups = 1;
            (inode, entry.generation)
        };
        debug!("explicitly adding {} -> {:?} with 1 lookups", inode, path);
        let previous = self.by_path.insert(path, inode as usize - 1);
        if previous.is_some() {
            error!("inode table buggered: {:?}", self);
            panic!("attempted to insert duplicate path into inode table: {:?}", previous.unwrap());
        }
        (inode, generation)
    }

    /// Add a path to the inode table if it does not yet exist.
    ///
    /// Returns the inode number the path is now mapped to.
    ///
    /// If the path was not in the table, it is added with an initial lookup count of 0.
    ///
    /// This operation runs in O(log n) time.
    pub fn add_or_get(&mut self, path: Arc<PathBuf>) -> (Inode, Generation) {
        match self.by_path.entry(path.clone()) {
            Vacant(path_entry) => {
                let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
                debug!("adding {} -> {:?} with 0 lookups", inode, path);
                entry.path = Some(path);
                path_entry.insert(inode as usize - 1);
                (inode, entry.generation)
            },
            Occupied(path_entry) => {
                let idx = path_entry.get();
                ((idx + 1) as Inode, self.table[*idx].generation)
            }
        }
    }

    /// Get the path that corresponds to an inode, if there is one, or None, if it is not in the
    /// table.
    /// Note that the file could be unlinked but still open, in which case it's not actually
    /// reachable from the path returned.
    ///
    /// This operation runs in O(1) time.
    pub fn get_path(&self, inode: Inode) -> Option<Arc<PathBuf>> {
        let idx = inode as usize - 1;
        match self.table[idx].path {
            Some(ref path) => Some(path.clone()),
            None => None,
        }
    }

    /// Get the inode that corresponds to a path, if there is one, or None, if it is not in the
    /// table.
    ///
    /// This operation runs in O(log n) time.
    pub fn get_inode(&mut self, path: &Path) -> Option<Inode> {
        match self.by_path.get(Pathish::new(path)) {
            Some(idx) => Some((idx + 1) as Inode),
            None => None,
        }
    }

    /// Increment the lookup count on a given inode.
    ///
    /// Calling this on an invalid inode will result in a panic.
    ///
    /// This operation runs in O(1) time.
    pub fn lookup(&mut self, inode: Inode) {
        if inode == 1 {
            return;
        }

        let entry = &mut self.table[inode as usize - 1];
        entry.lookups += 1;
        debug!("lookups on {} -> {:?} now {}", inode, entry.path, entry.lookups);
    }

    /// Decrement the lookup count on a given inode by the given number.
    ///
    /// If the lookup count reaches 0, the path is removed from the table, and the inode number
    /// is eligible to be re-used.
    ///
    /// Returns the new lookup count of the inode. (If it returns 0, that means the inode was
    /// deleted.)
    ///
    /// Calling this on an invalid inode will result in a panic.
    ///
    /// This operation runs in O(1) time normally, or O(log n) time if the inode is deleted.
    pub fn forget(&mut self, inode: Inode, n: LookupCount) -> LookupCount {
        if inode == 1 {
            return 1;
        }

        let mut delete = false;
        let lookups: LookupCount;
        let idx = inode as usize - 1;

        {
            let entry = &mut self.table[idx];
            println!("forget entry {:?}", entry);
            assert!(n <= entry.lookups);
            entry.lookups -= n;
            lookups = entry.lookups;
            if lookups == 0 {
                delete = true;
                self.by_path.remove(&*entry.path.as_ref().unwrap());
            }
        }

        if delete {
            self.table[idx].path = None;
            self.free_list.push_back(idx);
        }

        lookups
    }

    /// Change an inode's path to a different one, without changing the inode number.
    /// Lookup counts remain unchanged, even if this is replacing another file.
    pub fn rename(&mut self, oldpath: &Path, newpath: Arc<PathBuf>) {
        let idx = self.by_path.remove(Pathish::new(oldpath)).unwrap();
        self.table[idx].path = Some(newpath.clone());
        self.by_path.insert(newpath, idx); // this can replace a path with a new inode
    }

    /// Remove the path->inode mapping for a given path, but keep the inode around.
    pub fn unlink(&mut self, path: &Path) {
        self.by_path.remove(Pathish::new(path));
        // Note that the inode->path mapping remains.
    }

    /// Get a free indode table entry and its number, either by allocating a new one, or re-using
    /// one that had its lookup count previously go to zero.
    ///
    /// 1st arg should be `&mut self.free_list`; 2nd arg should be `&mut self.table`.
    /// This function's signature is like this instead of taking &mut self so that it can avoid
    /// mutably borrowing *all* fields of self when we only need those two.
    fn get_inode_entry<'a>(free_list: &mut VecDeque<usize>, table: &'a mut Vec<InodeTableEntry>)
            -> (Inode, &'a mut InodeTableEntry) {
        let idx = match free_list.pop_front() {
            Some(idx) => {
                debug!("re-using inode {}", idx + 1);
                table[idx].generation += 1;
                idx
            },
            None => {
                table.push(InodeTableEntry {
                    path: None,
                    lookups: 0,
                    generation: 0,
                });
                table.len() - 1
            }
        };
        ((idx + 1) as Inode, &mut table[idx])
    }
}

/// Facilitates comparing Rc<PathBuf> and &Path
#[derive(Debug)]
struct Pathish {
    inner: Path,
}

impl Pathish {
    pub fn new(p: &Path) -> &Pathish {
        unsafe { ::std::mem::transmute(p) }
    }
}

impl Borrow<Pathish> for Arc<PathBuf> {
    fn borrow(&self) -> &Pathish {
        Pathish::new(self.as_path())
    }
}

impl Hash for Pathish {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl Eq for Pathish {
}

impl PartialEq for Pathish {
    fn eq(&self, other: &Pathish) -> bool {
        self.inner.eq(&other.inner)
    }
}

#[test]
fn test_inode_reuse() {
    let mut table = InodeTable::new();
    let path1 = Arc::new(PathBuf::from("/foo/a"));
    let path2 = Arc::new(PathBuf::from("/foo/b"));

    // Add a path.
    let inode1 = table.add(path1.clone()).0;
    assert!(inode1 != 1);
    assert_eq!(*path1, *table.get_path(inode1).unwrap());

    // Add a second path; verify that the inode number is different.
    let inode2 = table.add(path2.clone()).0;
    assert!(inode2 != inode1);
    assert!(inode2 != 1);
    assert_eq!(*path2, *table.get_path(inode2).unwrap());

    // Forget the first inode; verify that lookups on it fail.
    assert_eq!(0, table.forget(inode1, 1));
    assert!(table.get_path(inode1).is_none());

    // Add a third path; verify that the inode is reused.
    let (inode3, generation3) = table.add(Arc::new(PathBuf::from("/foo/c")));
    assert_eq!(inode1, inode3);
    assert_eq!(1, generation3);

    // Check that lookups on the third path succeed.
    assert_eq!(Path::new("/foo/c"), *table.get_path(inode3).unwrap());
}

#[test]
fn test_add_or_get() {
    let mut table = InodeTable::new();
    let path1 = Arc::new(PathBuf::from("/foo/a"));
    let path2 = Arc::new(PathBuf::from("/foo/b"));

    // add_or_get() a path and verify that get by inode works before lookup() is done.
    let inode1 = table.add_or_get(path1.clone()).0;
    assert_eq!(*path1, *table.get_path(inode1).unwrap());
    table.lookup(inode1);

    // add() a second path and verify that get by path and inode work.
    let inode2 = table.add(path2.clone()).0;
    assert_eq!(*path2, *table.get_path(inode2).unwrap());
    assert_eq!(inode2, table.add_or_get(path2.clone()).0);
    table.lookup(inode2);

    // Check the ref counts by doing a single forget.
    assert_eq!(0, table.forget(inode1, 1));
    assert_eq!(1, table.forget(inode2, 1));
}

#[test]
fn test_inode_rename() {
    let mut table = InodeTable::new();
    let path1 = Arc::new(PathBuf::from("/foo/a"));
    let path2 = Arc::new(PathBuf::from("/foo/b"));

    // Add a path; verify that get by path and inode work.
    let inode = table.add(path1.clone()).0;
    assert_eq!(*path1, *table.get_path(inode).unwrap());
    assert_eq!(inode, table.get_inode(&*path1).unwrap());

    // Rename the inode; verify that get by the new path works and old path doesn't, and get by
    // inode still works.
    table.rename(&*path1, path2.clone());
    assert!(table.get_inode(&*path1).is_none());
    assert_eq!(inode, table.get_inode(&*path2).unwrap());
    assert_eq!(*path2, *table.get_path(inode).unwrap());
}

#[test]
fn test_unlink() {
    let mut table = InodeTable::new();
    let path = Arc::new(PathBuf::from("/foo/bar"));

    // Add a path.
    let inode = table.add(path.clone()).0;

    // Unlink it and verify that get by path fails.
    table.unlink(&*path);
    assert!(table.get_inode(&*path).is_none());

    // Getting the path for the inode should still return the path.
    assert_eq!(*path, *table.get_path(inode).unwrap());

    // Verify that forgetting it once drops the refcount to zero and then lookups by inode fail.
    assert_eq!(0, table.forget(inode, 1));
    assert!(table.get_path(inode).is_none());
}
