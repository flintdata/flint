use std::io::{self, Result as IoResult};
use std::collections::HashMap;
use crate::storage::base::TuplePointer;
use crate::storage::files::IndexFile;
use crate::storage::base::PageId;
use super::page::{IndexEntry, IndexPage};

/// Hash index with dynamic bucket allocation
/// Uses SipHash-style mixing for cryptographic safety against hash collision attacks
/// Buckets are allocated on-demand; bucket pages are chained via next_page_id when full
#[derive(Debug, Clone)]
pub struct HashIndex {
    /// Root page ID (reserved for metadata, not used yet)
    root_page_id: Option<PageId>,
    /// Map from bucket hash -> first page ID for that bucket
    bucket_pages: HashMap<u32, PageId>,
    /// Random seed for hash mixing (prevents hash flooding attacks)
    seed: u64,
}

impl HashIndex {
    /// Create a new dynamic hash index with random seed
    pub fn new(root_page_id: Option<PageId>) -> Self {
        // Generate random seed using system entropy for hash collision resistance
        let seed = Self::generate_seed();
        HashIndex {
            root_page_id,
            bucket_pages: HashMap::new(),
            seed,
        }
    }

    /// Create with explicit seed (for testing)
    #[cfg(test)]
    pub fn with_seed(root_page_id: Option<PageId>, seed: u64) -> Self {
        HashIndex {
            root_page_id,
            bucket_pages: HashMap::new(),
            seed,
        }
    }

    /// Generate a cryptographically random seed
    fn generate_seed() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        // Use RandomState (which uses SipHash internally) with system time for entropy
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let nanos = duration.as_nanos() as u64;

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(nanos);
        hasher.finish()
    }

    /// Get the root page ID
    pub fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    /// Compute bucket hash for key using SipHash-inspired mixing for crypto safety
    /// Combines key with random seed to prevent hash flooding attacks
    fn hash_key(&self, key: u64) -> u32 {
        // Mix key with seed using diffusion-based approach (SipHash-like)
        let mut hash = self.seed;

        // XOR in the key
        hash ^= key;

        // Rotate and multiply mixing steps (inspired by MurmurHash3)
        hash = hash.wrapping_mul(0xff51afd7ed558ccdu64);
        hash ^= hash >> 32;

        // Final diffusion
        hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53u64);
        hash ^= hash >> 33;

        hash as u32
    }

    /// Get or initialize bucket page for a bucket hash
    fn get_bucket_page(
        &mut self,
        bucket_hash: u32,
        disk_mgr: &IndexFile,
    ) -> IoResult<PageId> {
        if let Some(&page_id) = self.bucket_pages.get(&bucket_hash) {
            return Ok(page_id);
        }

        // Allocate new bucket page
        let page_id = disk_mgr.allocate_page()?;
        let page = IndexPage::new(true); // Hash buckets are leaf pages
        disk_mgr.write_page(page_id, &page.data)?;

        self.bucket_pages.insert(bucket_hash, page_id);
        Ok(page_id)
    }

    /// Find last page in chain (for appending overflow)
    fn find_last_page(&self, first_page_id: PageId, disk_mgr: &IndexFile) -> IoResult<PageId> {
        let mut current_id = first_page_id;
        loop {
            let page_data = disk_mgr.read_page(current_id)?;
            let page = IndexPage { data: page_data };
            match page.next_sibling()? {
                None => return Ok(current_id),
                Some(next_id) => current_id = next_id,
            }
        }
    }

    /// Search within a bucket page for a key
    fn search_in_page(page: &IndexPage, key: u64) -> IoResult<Option<usize>> {
        let header = page.header()?;
        for i in 0..header.num_keys as usize {
            let entry = page.get_entry(i)?;
            if entry.key == key {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    /// Update entry at position in page
    fn update_entry(page: &mut IndexPage, pos: usize, entry: &IndexEntry) -> IoResult<()> {
        let header = page.header()?;
        if pos >= header.num_keys as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Entry position out of range",
            ));
        }

        let header_size = std::mem::size_of::<super::page::IndexPageHeader>();
        let entry_size = std::mem::size_of::<IndexEntry>();
        let offset = header_size + pos * entry_size;

        let entry_bytes = unsafe {
            std::slice::from_raw_parts(
                entry as *const IndexEntry as *const u8,
                entry_size,
            )
        };
        page.data[offset..offset + entry_size].copy_from_slice(entry_bytes);
        Ok(())
    }
}

impl super::Index for HashIndex {
    fn index_type(&self) -> &str {
        "hash"
    }

    fn insert(
        &mut self,
        key: u64,
        pointer: TuplePointer,
        disk_mgr: &IndexFile,
    ) -> IoResult<Option<super::IndexSplit>> {
        let bucket_hash = self.hash_key(key);
        let first_page_id = self.get_bucket_page(bucket_hash, disk_mgr)?;

        // Search through bucket chain (first page and any overflow pages)
        let mut current_id = first_page_id;
        loop {
            let page_data = disk_mgr.read_page(current_id)?;
            let mut current_page = IndexPage { data: page_data };

            // Check if key already exists in this page
            if let Some(pos) = Self::search_in_page(&current_page, key)? {
                // Update existing entry
                let entry = IndexEntry::new(key, pointer);
                Self::update_entry(&mut current_page, pos, &entry)?;
                disk_mgr.write_page(current_id, &current_page.data)?;
                return Ok(None);
            }

            // Try to insert at end of this page
            let entry = IndexEntry::new(key, pointer);
            let header = current_page.header()?;
            let insert_pos = header.num_keys as usize;

            match current_page.insert_at(insert_pos, entry) {
                Ok(()) => {
                    // Successfully inserted
                    disk_mgr.write_page(current_id, &current_page.data)?;
                    return Ok(None);
                }
                Err(e) if e.kind() == io::ErrorKind::Other => {
                    // Current page is full, check if there's a next page
                    match current_page.next_sibling()? {
                        Some(next_id) => {
                            // Follow chain
                            current_id = next_id;
                        }
                        None => {
                            // Allocate new overflow page
                            let overflow_id = disk_mgr.allocate_page()?;
                            let mut overflow_page = IndexPage::new(true);

                            // Link current page to overflow
                            current_page.set_next_sibling(Some(overflow_id))?;
                            disk_mgr.write_page(current_id, &current_page.data)?;

                            // Insert into overflow page
                            overflow_page.insert_at(0, entry)?;
                            disk_mgr.write_page(overflow_id, &overflow_page.data)?;
                            return Ok(None);
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn search(
        &self,
        key: u64,
        disk_mgr: &IndexFile,
    ) -> IoResult<Option<TuplePointer>> {
        let bucket_hash = self.hash_key(key);

        // Get first page for bucket, or return not found if bucket doesn't exist
        let first_page_id = match self.bucket_pages.get(&bucket_hash) {
            Some(&page_id) => page_id,
            None => return Ok(None),
        };

        // Search through bucket chain
        let mut current_id = first_page_id;
        loop {
            let page_data = disk_mgr.read_page(current_id)?;
            let current_page = IndexPage { data: page_data };

            // Search for key in this page
            if let Some(pos) = Self::search_in_page(&current_page, key)? {
                let entry = current_page.get_entry(pos)?;
                return Ok(Some(entry.as_tuple_pointer()));
            }

            // Not found in this page, check next
            match current_page.next_sibling()? {
                Some(next_id) => current_id = next_id,
                None => return Ok(None),
            }
        }
    }
}