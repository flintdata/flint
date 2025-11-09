use std::io;
use crate::storage::base::{TuplePointer, PageId};
use crate::storage::files::IndexFile;

pub mod page;
pub mod btree;
pub mod hash;

/// Index capability classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCapability {
    /// Supports only point lookups and inserts
    PointOnly,
    /// Supports ordered operations including range scans
    Ordered,
}

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

    /// Return the capability classification of this index
    /// Default: PointOnly (indexes must override if they support OrderedIndex)
    fn capability(&self) -> IndexCapability {
        IndexCapability::PointOnly
    }

    /// Insert a key-value pair into the index
    /// Returns None if no split occurred, Some(IndexSplit) if the index node split
    fn insert(&mut self, key: u64, pointer: TuplePointer, disk_mgr: &IndexFile) -> io::Result<Option<IndexSplit>>;

    /// Search for a value by key
    fn search(&self, key: u64, disk_mgr: &IndexFile) -> io::Result<Option<TuplePointer>>;

    /// Range scan - return all entries in [start_key, end_key] inclusive
    /// Default implementation: returns empty vec (override for ordered indexes)
    fn range_scan(&self, _start_key: u64, _end_key: u64, _disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>> {
        Ok(Vec::new())
    }

    /// Full scan - return all entries in the index
    /// Default implementation: returns empty vec (override for ordered indexes)
    fn full_scan(&self, _disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>> {
        Ok(Vec::new())
    }
}

/// Extended trait for indexes that support ordered operations and range scans
pub trait OrderedIndex: Index {
    /// Return the capability classification of this index
    /// Overrides parent trait to declare Ordered capability
    fn capability(&self) -> IndexCapability {
        IndexCapability::Ordered
    }

    /// Range scan - return all entries in [start_key, end_key] inclusive
    fn range_scan(&self, start_key: u64, end_key: u64, disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>>;

    /// Full scan - return all entries in the index
    fn full_scan(&self, disk_mgr: &IndexFile) -> io::Result<Vec<(u64, TuplePointer)>>;
}

/// Factory trait for creating index instances
pub trait IndexBuilder: Send + Sync {
    /// Create a new index instance with optional root page ID
    fn create(&self, root_page_id: Option<PageId>) -> Box<dyn Index>;

    /// Return the type name of this index builder (e.g., "btree", "hash")
    fn type_name(&self) -> &str;
}

/// Registry for discovering and instantiating index types
pub struct IndexBuilderRegistry {
    builders: std::collections::HashMap<String, Box<dyn IndexBuilder>>,
}

impl IndexBuilderRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        IndexBuilderRegistry {
            builders: std::collections::HashMap::new(),
        }
    }

    /// Register an index builder
    pub fn register(&mut self, type_name: &str, builder: Box<dyn IndexBuilder>) {
        self.builders.insert(type_name.to_string(), builder);
    }

    /// Get a builder by type name and create an index instance
    pub fn create_index(&self, type_name: &str, root_page_id: Option<PageId>) -> Option<Box<dyn Index>> {
        self.builders
            .get(type_name)
            .map(|builder| builder.create(root_page_id))
    }

    /// List all available index types
    pub fn available_types(&self) -> Vec<String> {
        self.builders.keys().cloned().collect()
    }
}