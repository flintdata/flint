use std::io::{self, Result as IoResult};
use crate::storage::base::TuplePointer;
use crate::storage::files::IndexFile;
use crate::storage::base::PageId;
use super::page::{IndexEntry, IndexPage, IndexPageHeader, NodeType};

/// Represents a split result when a node overflows
#[derive(Debug)]
pub struct SplitResult {
    /// The key that was promoted to the parent
    pub promoted_key: u64,
    /// The right sibling after split
    pub right_page: IndexPage,
}

/// B+ Tree with root page tracking
/// Stores root page ID and loads/saves pages via IndexDiskManager
#[derive(Debug, Clone)]
pub struct BTree {
    root_page_id: Option<PageId>,
}

impl BTree {
    /// Create a new BTree with optional root page ID
    pub fn new(root_page_id: Option<PageId>) -> Self {
        BTree { root_page_id }
    }

    /// Get the root page ID (if exists)
    pub fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    /// Insert a key-value pair into a page, handling splits if necessary
    /// Returns None if no split occurred, Some(SplitResult) if the page split
    pub fn insert_into_page(
        page: &mut IndexPage,
        key: u64,
        tuple_ptr: TuplePointer,
    ) -> IoResult<Option<SplitResult>> {
        let (found, pos) = page.binary_search(key)?;

        // If key already exists, update it (replace old value)
        if found {
            let entry = IndexEntry::new(key, tuple_ptr);
            let header_size = std::mem::size_of::<IndexPageHeader>();
            let entry_size = std::mem::size_of::<IndexEntry>();
            let offset = header_size + pos * entry_size;

            let entry_bytes = unsafe {
                std::slice::from_raw_parts(
                    &entry as *const IndexEntry as *const u8,
                    entry_size,
                )
            };
            page.data[offset..offset + entry_size].copy_from_slice(entry_bytes);
            return Ok(None);
        }

        // Try to insert at position
        let entry = IndexEntry::new(key, tuple_ptr);
        match page.insert_at(pos, entry) {
            Ok(()) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::Other => {
                // Page is full, need to split
                Self::split_page(page, pos, entry)
            }
            Err(e) => Err(e),
        }
    }

    /// Split a full page into two pages
    /// Returns the promoted key and the right sibling page
    fn split_page(
        page: &mut IndexPage,
        insert_pos: usize,
        new_entry: IndexEntry,
    ) -> IoResult<Option<SplitResult>> {
        // Get all current entries
        let mut entries = page.entries()?;

        // Insert the new entry into the collection
        entries.insert(insert_pos, new_entry);

        // Get page info
        let header = page.header()?;
        let is_leaf = header.is_leaf();

        // Calculate split point (roughly middle)
        let split_point = entries.len() / 2;

        // Split entries
        let right_entries: Vec<_> = entries.drain(split_point..).collect();
        let promoted_key = right_entries[0].key;

        // Left page keeps the lower keys
        let node_type_left = if is_leaf { NodeType::Leaf } else { NodeType::Internal };
        page.set_entries(node_type_left, entries)?;

        // Right page gets the higher keys
        let node_type = if is_leaf { NodeType::Leaf } else { NodeType::Internal };
        let mut right_page = IndexPage::new(node_type);
        right_page.set_entries(node_type, right_entries)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
    }

    /// Find a value by key in a page (returns Option<TuplePointer> if leaf)
    pub fn search_page(page: &IndexPage, key: u64) -> IoResult<Option<TuplePointer>> {
        let (found, pos) = page.binary_search(key)?;

        if !found {
            return Ok(None);
        }

        let entry = page.get_entry(pos)?;
        Ok(Some(entry.as_tuple_pointer()))
    }

    /// Range scan in a leaf page - get all entries in [start_key, end_key]
    pub fn range_scan_page(
        page: &IndexPage,
        start_key: u64,
        end_key: u64,
    ) -> IoResult<Vec<(u64, TuplePointer)>> {
        let header = page.header()?;
        let mut results = Vec::new();

        for i in 0..header.num_keys as usize {
            let entry = page.get_entry(i)?;
            if entry.key >= start_key && entry.key <= end_key {
                results.push((entry.key, entry.as_tuple_pointer()));
            }
        }

        Ok(results)
    }

    /// Get all entries from a leaf page (for full scan)
    pub fn scan_page(page: &IndexPage) -> IoResult<Vec<(u64, TuplePointer)>> {
        let entries = page.entries()?;
        Ok(entries
            .into_iter()
            .map(|e| (e.key, e.as_tuple_pointer()))
            .collect())
    }

    /// Find the leaf page containing a given key by traversing internal nodes
    fn find_leaf_page(
        &self,
        key: u64,
        disk_mgr: &IndexFile,
    ) -> IoResult<IndexPage> {
        let mut current_page_id = match self.root_page_id {
            None => return Err(io::Error::new(io::ErrorKind::NotFound, "No root page")),
            Some(id) => id,
        };

        loop {
            let page_data = disk_mgr.read_page(current_page_id)?;
            let current_page = IndexPage { data: page_data };
            let header = current_page.header()?;

            if header.is_leaf() {
                return Ok(current_page);
            }

            // Internal node: find child to traverse
            let (found, pos) = current_page.binary_search(key)?;

            // For internal nodes, position tells us which child to follow
            // If found, child is at pos; if not found, child is at pos
            let child_index = if found { pos } else { pos };

            // Get the entry at child_index to find next page
            let entry = if child_index < header.num_keys as usize {
                current_page.get_entry(child_index)?
            } else {
                // Key is greater than all keys in this node, use rightmost child
                if header.num_keys == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Internal node has no keys",
                    ));
                }
                current_page.get_entry((header.num_keys - 1) as usize)?
            };

            current_page_id = entry.as_child_page_id();
        }
    }
}

impl super::Index for BTree {
    fn index_type(&self) -> &str {
        "btree"
    }

    fn insert(
        &mut self,
        key: u64,
        pointer: TuplePointer,
        disk_mgr: &IndexFile,
    ) -> IoResult<Option<super::IndexSplit>> {
        // Read root page
        let root_id = self.root_page_id.unwrap();
        let page_data = disk_mgr.read_page(root_id)?;
        let mut root_page = super::page::IndexPage { data: page_data };

        // Insert into root
        match Self::insert_into_page(&mut root_page, key, pointer)? {
            None => {
                // No split, just write back
                disk_mgr.write_page(root_id, &root_page.data)?;
                Ok(None)
            }
            Some(split) => {
                // Root split - create new root
                // Write left page (current root becomes left child)
                disk_mgr.write_page(root_id, &root_page.data)?;

                // Allocate right sibling
                let right_id = disk_mgr.allocate_page()?;
                disk_mgr.write_page(right_id, &split.right_page.data)?;

                // For now, signal split to caller
                // Full B+ tree would create new parent here
                Ok(Some(super::IndexSplit {
                    promoted_key: split.promoted_key,
                    right_sibling_data: split.right_page.data.to_vec(),
                }))
            }
        }
    }

    fn search(
        &self,
        key: u64,
        disk_mgr: &IndexFile,
    ) -> IoResult<Option<TuplePointer>> {
        // Find the leaf page containing the key
        let leaf_page = self.find_leaf_page(key, disk_mgr)?;
        Self::search_page(&leaf_page, key)
    }
}

impl super::OrderedIndex for BTree {
    fn range_scan(
        &self,
        start_key: u64,
        end_key: u64,
        disk_mgr: &IndexFile,
    ) -> IoResult<Vec<(u64, TuplePointer)>> {
        // Find the leftmost leaf containing start_key
        let leaf_page = self.find_leaf_page(start_key, disk_mgr)?;
        Self::range_scan_page(&leaf_page, start_key, end_key)
    }

    fn full_scan(&self, disk_mgr: &IndexFile) -> IoResult<Vec<(u64, TuplePointer)>> {
        // Find the leftmost leaf by searching for key 0
        let leaf_page = self.find_leaf_page(0, disk_mgr)?;
        Self::scan_page(&leaf_page)
        // NOTE: Without sibling pointers, we only scan the first leaf found.
        // Full implementation would need B+ tree sibling links to scan all leaves.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_creation_empty() {
        let btree = BTree::new(None);
        assert_eq!(btree.root_page_id(), None);
    }

    #[test]
    fn test_btree_creation_with_root() {
        let page_id = PageId::new(0, 0);
        let btree = BTree::new(Some(page_id));
        assert_eq!(btree.root_page_id(), Some(page_id));
    }
}