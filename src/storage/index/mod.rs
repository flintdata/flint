use std::io;
use crate::storage::base::TuplePointer;
use crate::storage::files::IndexFile;

pub mod page;
pub mod btree;
pub mod hash;

pub use page::IndexPage;

/// Represents a split result when a node overflows
#[derive(Debug, Clone)]
pub struct IndexSplit {
    /// The key that was promoted to the parent
    pub promoted_key: u64,
    /// Serialized data for the right sibling page
    pub right_sibling_data: Vec<u8>,
}

/// Base trait for all index types - supports point lookups and insertions
pub trait Index: Send + Sync {
    /// Return the type name of this index
    fn index_type(&self) -> &str;

    /// Insert a key-value pair into the index
    /// Returns None if no split occurred, Some(IndexSplit) if the index node split
    fn insert(&mut self, key: u64, pointer: TuplePointer, disk_mgr: &IndexFile) -> io::Result<Option<IndexSplit>>;

    /// Search for a value by key
    fn search(&self, key: u64, disk_mgr: &IndexFile) -> io::Result<Option<TuplePointer>>;
}

/// Extended trait for indexes that support ordered operations and range scans
pub trait OrderedIndex: Index {
    /// Range scan - return all entries in [start_key, end_key] inclusive
    fn range_scan(&self, start_key: u64, end_key: u64, disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>>;

    /// Full scan - return all entries in the index
    fn full_scan(&self, disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>>;
}