// InodeTable :: a bi-directional map of paths to inodes.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::{BTreeMap, VecDeque};
use std::collections::btree_map::Entry::*;
use std::sync::Arc;
use std::path::{Path, PathBuf};

pub type Inode = u64;
pub type LookupCount = u64;

#[derive(Debug)]
struct InodeTableEntry {
    path: Arc<PathBuf>,
    lookups: LookupCount,
}

/// A data structure for mapping paths to inodes and vice versa.
#[derive(Debug)]
pub struct InodeTable {
    table: Vec<Option<InodeTableEntry>>,
    free_list: VecDeque<usize>,
    by_path: BTreeMap<Arc<PathBuf>, usize>,
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
            by_path: BTreeMap::new()
        };
        let root = Arc::new(PathBuf::from("/"));
        inode_table.table.push(Some(InodeTableEntry {
            path: root.clone(),
            lookups: 0, // not used for this entry; root is always present.
        }));
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
    pub fn add(&mut self, path: Arc<PathBuf>) -> Inode {
        let idx = self.free_list.pop_front().unwrap_or_else(|| {
            self.table.push(None);
            self.table.len() - 1
        });
        self.table[idx] = Some(InodeTableEntry {
            path: path.clone(),
            lookups: 0,
        });
        let previous = self.by_path.insert(path, idx);
        if previous.is_some() {
            error!("inode table buggered: {:?}", self);
            panic!("attempted to insert duplicate path into inode table: {:?}", previous.unwrap());
        }
        (idx + 1) as Inode
    }

    /// Add a path to the inode table if it does not yet exist.
    ///
    /// Returns the inode number the path is now mapped to.
    ///
    /// If the path was not in the table, it is added with an initial lookup count of 0.
    ///
    /// This operation runs in O(log n) time.
    pub fn add_or_get(&mut self, path: Arc<PathBuf>) -> Inode {
        match self.by_path.entry(path.clone()) {
            Vacant(entry) => {
                let table_ref = &mut self.table;
                let idx = self.free_list.pop_front().unwrap_or_else(|| {
                    table_ref.push(None);
                    table_ref.len() - 1
                });
                table_ref[idx] = Some(InodeTableEntry {
                    path: path,
                    lookups: 0,    // lookup must be done later
                });
                entry.insert(idx);
                (idx + 1) as Inode
            },
            Occupied(entry) => (entry.get() + 1) as Inode
        }
    }

    /// Get the path that corresponds to an inode, if there is one, or None, if it is not in the
    /// table.
    ///
    /// This operation runs in O(1) time.
    pub fn get_path(&self, inode: Inode) -> Option<Arc<PathBuf>> {
        let idx = inode as usize - 1;
        match self.table[idx] {
            Some(ref entry) => Some(entry.path.clone()),
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

        self.table[inode as usize - 1].as_mut().unwrap().lookups += 1;
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
            let entry = self.table[idx].as_mut().unwrap();
            assert!(n <= entry.lookups);
            entry.lookups -= n;
            lookups = entry.lookups;
            if lookups == 0 {
                delete = true;
                self.by_path.remove(&*entry.path);
            }
        }

        if delete {
            self.table[idx] = None;
            self.free_list.push_back(idx);
        }

        lookups
    }

    /// Change an inode's path to a different one, without changing the inode number.
    pub fn rename(&mut self, oldpath: &Path, newpath: Arc<PathBuf>) {
        let idx = self.by_path.remove(Pathish::new(oldpath)).unwrap();
        self.table[idx].as_mut().unwrap().path = newpath.clone();
        self.by_path.insert(newpath, idx); // this can replace a path with a new inode
    }

    pub fn unlink(&mut self, path: &Path) {
        // The inode is now unreachable by this name, but the inode->path mapping remains.
        self.path_to_inode.remove(Pathish::new(path));
    }
}

// Facilitates comparing Rc<PathBuf> and &Path
struct Pathish {
    inner: Path,
}

impl Pathish {
    pub fn new(p: &Path) -> &Pathish {
        unsafe { ::std::mem::transmute(p) }
    }
}

impl ::std::borrow::Borrow<Pathish> for Arc<PathBuf> {
    fn borrow(&self) -> &Pathish {
        Pathish::new(self.as_path())
    }
}

impl ::std::cmp::Ord for Pathish {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl ::std::cmp::PartialOrd for Pathish {
    fn partial_cmp(&self, other: &Pathish) -> Option<::std::cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl ::std::cmp::Eq for Pathish {
}

impl ::std::cmp::PartialEq for Pathish {
    fn eq(&self, other: &Pathish) -> bool {
        self.inner.eq(&other.inner)
    }
}
