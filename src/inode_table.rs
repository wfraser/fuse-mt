// InodeTable :: a bi-directional map of paths to inodes.
//
// Copyright (c) 2016-2026 by William R. Fraser
//

use std::borrow::Borrow;
use std::cmp::{Eq, Ordering, PartialEq};
use std::collections::btree_map::Entry::*;
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use fuser::{Generation, INodeNo};

pub type LookupCount = u64;

pub const ROOT: INodeNo = INodeNo(1);

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
            by_path: BTreeMap::new(),
        };
        let root = Arc::new(PathBuf::from("/"));
        inode_table.table.push(InodeTableEntry {
            path: Some(root.clone()),
            lookups: 0, // not used for this entry; root is always present.
            generation: Generation(0),
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
    pub fn add(&mut self, path: Arc<PathBuf>) -> (INodeNo, Generation) {
        let (inode, generation) = {
            let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
            entry.path = Some(path.clone());
            entry.lookups = 1;
            (inode, entry.generation)
        };
        debug!("explicitly adding {} -> {:?} with 1 lookups", inode, path);
        let previous = self.by_path.insert(path, inode.0 as usize - 1);
        if let Some(previous) = previous {
            error!("inode table buggered: {:?}", self);
            panic!("attempted to insert duplicate path into inode table: {:?}", previous);
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
    pub fn add_or_get(&mut self, path: Arc<PathBuf>) -> (INodeNo, Generation) {
        match self.by_path.entry(path.clone()) {
            Vacant(path_entry) => {
                let (inode, entry) = Self::get_inode_entry(&mut self.free_list, &mut self.table);
                debug!("adding {} -> {:?} with 0 lookups", inode, path);
                entry.path = Some(path);
                path_entry.insert(inode.0 as usize - 1);
                (inode, entry.generation)
            },
            Occupied(path_entry) => {
                let idx = path_entry.get();
                (INodeNo((idx + 1) as u64), self.table[*idx].generation)
            }
        }
    }

    /// Get the path that corresponds to an inode, if there is one, or None, if it is not in the
    /// table.
    /// Note that the file could be unlinked but still open, in which case it's not actually
    /// reachable from the path returned.
    ///
    /// This operation runs in O(1) time.
    pub fn get_path(&self, inode: INodeNo) -> Option<Arc<PathBuf>> {
        self.table[inode.0 as usize - 1].path.clone()
    }

    /// Get the inode that corresponds to a path, if there is one, or None, if it is not in the
    /// table.
    ///
    /// This operation runs in O(log n) time.
    pub fn get_inode(&mut self, path: &Path) -> Option<INodeNo> {
        self.by_path
            .get(Pathish::new(path))
            .map(|idx| INodeNo((idx + 1) as u64))
    }

    /// Increment the lookup count on a given inode.
    ///
    /// Calling this on an invalid inode will result in a panic.
    ///
    /// This operation runs in O(1) time.
    pub fn lookup(&mut self, inode: INodeNo) {
        if inode == ROOT {
            return;
        }

        let entry = &mut self.table[inode.0 as usize - 1];
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
    pub fn forget(&mut self, inode: INodeNo, n: LookupCount) -> LookupCount {
        if inode == ROOT {
            return 1;
        }

        let idx = inode.0 as usize - 1;

        let entry = &mut self.table[idx];
        debug!("forget entry {:?}", entry);

        entry.lookups = entry.lookups.saturating_sub(n);
        let lookups = entry.lookups; // unborrow

        if lookups == 0 {
            self.by_path.remove(entry.path.as_ref().unwrap());
            self.table[idx].path = None;
            self.free_list.push_back(idx);
        }

        lookups
    }

    /// Change an inode's path to a different one, without changing the inode number.
    /// Lookup counts remain unchanged, even if this is replacing another file.
    pub fn rename(&mut self, oldpath: &Path, newpath: Arc<PathBuf>) {
        // Look for children of the path being renamed and fix them up too.
        // Note that we use range() to find the bounds of the map first, and only then use
        // extract_if to remove that range, because:
        // 1. extract_if does not provide a way to stop iterating early
        // 2. extract_if does not utilize Borrow in its range, so here we have to clone the
        //    start and end paths because the range type must match the key type exactly.
        if let Some((last_child, _)) = self
            .by_path
            .range::<Pathish, _>((Excluded(Pathish::new(oldpath)), Unbounded))
            .take_while(|(path, _)| path.starts_with(oldpath))
            .last()
        {
            let mut new_entries = vec![];
            for (path, idx) in self.by_path.extract_if(
                (
                    Included(Arc::new(oldpath.to_owned())),
                    Included(Arc::clone(last_child)),
                ),
                |_, _| true,
            ) {
                let suffix = path.strip_prefix(oldpath).unwrap();
                let new_entry_path = if suffix.as_os_str().is_empty() {
                    // this is the entry for parent path itself
                    Arc::clone(&newpath)
                } else {
                    Arc::new(newpath.as_path().join(suffix))
                };
                self.table[idx].path = Some(Arc::clone(&new_entry_path));
                new_entries.push((new_entry_path, idx));
            }
            self.by_path.extend(new_entries);
        } else {
            let idx = self.by_path.remove(Pathish::new(oldpath)).unwrap();
            self.table[idx].path = Some(Arc::clone(&newpath));
            self.by_path.insert(newpath, idx); // this can replace a path with a new inode
        }
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
            -> (INodeNo, &'a mut InodeTableEntry) {
        let idx = match free_list.pop_front() {
            Some(idx) => {
                debug!("re-using inode {}", idx + 1);
                table[idx].generation.0 += 1;
                idx
            },
            None => {
                table.push(InodeTableEntry {
                    path: None,
                    lookups: 0,
                    generation: Generation(0),
                });
                table.len() - 1
            }
        };
        (INodeNo((idx + 1) as u64), &mut table[idx])
    }
}

/// Facilitates comparing Arc<PathBuf> and &Path.
///
/// We can't implement arbitrary traits like Borrow<Path> on Arc<PathBuf>, but we can invent our
/// own type like this that's identical to Path, and use that instead.
#[derive(Debug)]
#[repr(transparent)]
struct Pathish(Path);

impl Pathish {
    pub fn new(p: &Path) -> &Pathish {
        // safe because Path and Pathish are identical, guarenteed by #[repr(transparent)]
        unsafe { std::mem::transmute(p) }
    }
}

impl Borrow<Pathish> for Arc<PathBuf> {
    fn borrow(&self) -> &Pathish {
        Pathish::new(self.as_path())
    }
}

impl PartialOrd<Self> for Pathish {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Pathish {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl Eq for Pathish {}

impl PartialEq for Pathish {
    fn eq(&self, other: &Pathish) -> bool {
        self.0.eq(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_reuse() {
        let mut table = InodeTable::new();
        let path1 = Arc::new(PathBuf::from("/foo/a"));
        let path2 = Arc::new(PathBuf::from("/foo/b"));

        // Add a path.
        let inode1 = table.add(path1.clone()).0;
        assert_ne!(inode1.0, 1);
        assert_eq!(*path1, *table.get_path(inode1).unwrap());

        // Add a second path; verify that the inode number is different.
        let inode2 = table.add(path2.clone()).0;
        assert_ne!(inode2, inode1);
        assert_ne!(inode2.0, 1);
        assert_eq!(*path2, *table.get_path(inode2).unwrap());

        // Forget the first inode; verify that lookups on it fail.
        assert_eq!(0, table.forget(inode1, 1));
        assert!(table.get_path(inode1).is_none());

        // Add a third path; verify that the inode is reused.
        let (inode3, generation3) = table.add(Arc::new(PathBuf::from("/foo/c")));
        assert_eq!(inode1, inode3);
        assert_eq!(Generation(1), generation3);

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
        assert_eq!(inode2, table.add_or_get(path2).0);
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
        assert_eq!(inode, table.get_inode(&path1).unwrap());

        // Rename the inode; verify that get by the new path works and old path doesn't, and get by
        // inode still works.
        table.rename(&path1, path2.clone());
        assert!(table.get_inode(&path1).is_none());
        assert_eq!(inode, table.get_inode(&path2).unwrap());
        assert_eq!(*path2, *table.get_path(inode).unwrap());
    }

    #[test]
    fn test_unlink() {
        let mut table = InodeTable::new();
        let path = Arc::new(PathBuf::from("/foo/bar"));

        // Add a path.
        let inode = table.add(path.clone()).0;

        // Unlink it and verify that get by path fails.
        table.unlink(&path);
        assert!(table.get_inode(&path).is_none());

        // Getting the path for the inode should still return the path.
        assert_eq!(*path, *table.get_path(inode).unwrap());

        // Verify that forgetting it once drops the refcount to zero and then lookups by inode fail.
        assert_eq!(0, table.forget(inode, 1));
        assert!(table.get_path(inode).is_none());
    }

    #[test]
    fn test_rename_directory() {
        let mut table = InodeTable::new();
        let a = table.add(Arc::new(PathBuf::from("/a_file")));
        let d = table.add(Arc::new(PathBuf::from("/directory")));
        let x = table.add(Arc::new(PathBuf::from("/directory.x"))); // '.' sorts before '/' naïvely!
        let d_f1 = table.add(Arc::new(PathBuf::from("/directory/file1")));
        let d_f2 = table.add(Arc::new(PathBuf::from("/directory/file2")));
        let z = table.add(Arc::new(PathBuf::from("/z_file")));

        table.rename(Path::new("/a_file"), Arc::new(PathBuf::from("/a_file_renamed")));
        assert_eq!(table.get_inode(Path::new("/a_file")), None);
        assert_eq!(table.get_inode(Path::new("/a_file_renamed")), Some(a.0));

        table.rename(Path::new("/directory"), Arc::new(PathBuf::from("/new_directory")));

        // paths which should be unaffected
        assert_eq!(table.get_inode(Path::new("/a_file_renamed")), Some(a.0));
        assert_eq!(table.get_inode(Path::new("/z_file")), Some(z.0));
        assert_eq!(table.get_inode(Path::new("/directory.x")), Some(x.0));

        // paths which should have been renamed and return the same inode as original
        assert_eq!(table.get_inode(Path::new("/new_directory")), Some(d.0));
        assert_eq!(table.get_inode(Path::new("/new_directory/file1")), Some(d_f1.0));
        assert_eq!(table.get_inode(Path::new("/new_directory/file2")), Some(d_f2.0));

        // paths which should no longer exist
        assert_eq!(table.get_inode(Path::new("/directory")), None);
        assert_eq!(table.get_inode(Path::new("/directory/file1")), None);
        assert_eq!(table.get_inode(Path::new("/directory/file2")), None);
    }
}
