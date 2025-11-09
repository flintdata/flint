mod io;
mod base;
mod internal;
pub mod index;
pub mod files;
pub mod catalog;
pub mod wal;

// Re-export for extension types
pub use self::base::TuplePointer;
pub use base::PageId;

use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU8, Ordering}};
use std::path::PathBuf;
use parking_lot::Mutex;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use crate::types::{Row, Schema};
use crate::config::Config;
#[cfg(feature = "extensions")]
use crate::extensions::registry::{TypeRegistry, OperatorRegistry, FunctionRegistry, IndexBuilderRegistry};
use self::files::TableFile;
use self::catalog::Catalog;

pub type Result<T> = std::result::Result<T, String>;

/// Compute simple checksum for metadata validation
fn compute_checksum(data: &[u8]) -> u64 {
    data.iter().fold(0u64, |acc, &byte| {
        acc.wrapping_mul(31).wrapping_add(byte as u64)
    })
}

/// Catalog header for metadata persistence
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct CatalogHeader {
    pub catalog_version: u32,
    pub num_tables: u32,
    pub table_offsets: Vec<(String, u32)>, // (table_name, byte_offset)
    pub checksum: u64,
}

impl CatalogHeader {
    pub fn new() -> Self {
        CatalogHeader {
            catalog_version: 1,
            num_tables: 0,
            table_offsets: Vec::new(),
            checksum: 0,
        }
    }
}

/// Metadata manager tracks active metadata segment (0 or 1) with atomic flip
struct MetadataManager {
    active_segment: AtomicU8,
}

impl MetadataManager {
    fn new() -> Self {
        MetadataManager {
            active_segment: AtomicU8::new(0),
        }
    }

    fn get_active(&self) -> u32 {
        self.active_segment.load(Ordering::SeqCst) as u32
    }

    fn get_inactive(&self) -> u32 {
        let active = self.active_segment.load(Ordering::SeqCst);
        (1 - active) as u32
    }

    fn flip(&self) {
        let current = self.active_segment.load(Ordering::SeqCst);
        self.active_segment.store(1 - current, Ordering::SeqCst);
    }
}

/// Runtime table metadata (file paths + schema)
#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub file_path: PathBuf,
    pub schema: Schema,
    /// Primary key index root page ID (None if no PK defined)
    pub primary_index_root: Option<PageId>,
    /// Secondary indexes: (index_name -> root page ID)
    pub secondary_indexes: Vec<(String, PageId)>,
}

/// Database with per-table file storage
pub struct Database {
    /// Per-table file handles
    table_files: HashMap<String, Arc<TableFile>>,
    /// Runtime metadata (paths + schemas)
    tables: HashMap<String, TableMetadata>,
    /// Global catalog metadata
    catalog: Catalog,
    /// Per-table latches for serializing index modifications
    /// TODO implement per-page latching for proper implementation
    index_latches: HashMap<String, Arc<Mutex<()>>>,
    /// Extension registries for types, operators, functions, indexes
    #[cfg(feature = "extensions")]
    pub type_registry: Arc<TypeRegistry>,
    #[cfg(feature = "extensions")]
    pub operator_registry: Arc<OperatorRegistry>,
    #[cfg(feature = "extensions")]
    pub function_registry: Arc<FunctionRegistry>,
    #[cfg(feature = "extensions")]
    pub index_builder_registry: Arc<IndexBuilderRegistry>,
}

impl Database {
    pub fn new(config: &Config) -> Self {
        // Initialize global catalog from catalog.db or create new
        let catalog = Catalog::new();

        #[cfg(feature = "extensions")]
        let mut db = {
            // Initialize registries with built-in types
            let mut type_registry = TypeRegistry::new();
            crate::extensions::builtin::register_builtin_types(&mut type_registry);

            let mut operator_registry = OperatorRegistry::new();
            let mut function_registry = FunctionRegistry::new();
            let index_builder_registry = IndexBuilderRegistry::new();

            // Load extensions based on config
            let enabled_extensions = if config.load_all_extensions {
                None // None means load all extensions
            } else {
                Some(config.enabled_extensions.as_slice()) // Load only specified extensions
            };

            // Load all registered extensions (auto-discovered via inventory)
            crate::extensions::loader::load_all_extensions(
                &mut type_registry,
                &mut operator_registry,
                &mut function_registry,
                enabled_extensions,
            );

            Database {
                table_files: HashMap::new(),
                tables: HashMap::new(),
                catalog,
                index_latches: HashMap::new(),
                type_registry: Arc::new(type_registry),
                operator_registry: Arc::new(operator_registry),
                function_registry: Arc::new(function_registry),
                index_builder_registry: Arc::new(index_builder_registry),
            }
        };

        #[cfg(not(feature = "extensions"))]
        let mut db = Database {
            table_files: HashMap::new(),
            tables: HashMap::new(),
            catalog,
            index_latches: HashMap::new(),
        };

        // Try to load catalog from disk (TODO: implement catalog.db disk I/O)
        let _ = db.load_catalog_from_disk();

        db
    }

    /// Load catalog from catalog.db file (TODO: implement disk I/O)
    fn load_catalog_from_disk(&mut self) -> Result<()> {
        // TODO: Load catalog from catalog.db using dual-segment atomic writes
        // For now, start with empty catalog
        Ok(())
    }

    /// Save catalog to catalog.db file (TODO: implement disk I/O)
    fn save_catalog_to_disk(&mut self) -> Result<()> {
        // TODO: Serialize catalog and write to catalog.db with atomic flip
        Ok(())
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table already exists: {}", name));
        }

        // Create file path: table_<name>.tbl
        let file_path = PathBuf::from(format!("table_{}.tbl", name));

        // Open/create the per-table file
        let table_file = TableFile::open(&file_path)
            .map_err(|e| format!("Failed to open table file: {}", e))?;

        // Allocate first segment (segment 0 contains table header)
        let segment_id = table_file.allocate_segment()
            .map_err(|e| format!("Failed to allocate segment: {}", e))?;

        // Create runtime metadata
        let metadata = TableMetadata {
            name: name.clone(),
            file_path: file_path.clone(),
            schema,
            primary_index_root: None,
            secondary_indexes: Vec::new(),
        };

        // Insert into runtime tables
        // TODO check or mutex to prevent duplicate tables
        self.tables.insert(name.clone(), metadata);
        self.table_files.insert(name.clone(), Arc::new(table_file));

        // TODO: Update catalog and persist to catalog.db
        self.save_catalog_to_disk()?;

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableMetadata> {
        self.tables
            .get(name)
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<()> {
        let table_file = self.table_files.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?
            .clone();

        let metadata = self.tables.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;

        // Validate row against schema
        if row.len() != metadata.schema.len() {
            return Err(format!(
                "Row has {} columns but schema expects {}",
                row.len(),
                metadata.schema.len()
            ));
        }

        // Serialize row to bytes
        let row_bytes = bincode::encode_to_vec(&row, bincode::config::standard())
            .map_err(|e| format!("Serialization error: {}", e))?;

        // Try to allocate block in segment 0 (first segment)
        let segment_id = 0u32;
        let block_id = table_file.allocate_block(segment_id)
            .map_err(|e| format!("Failed to allocate block: {}", e))?
            .ok_or_else(|| "Segment full - need to allocate new segment".to_string())?;

        // Read block, append tuple, write back
        let mut block = table_file.read_block(segment_id, block_id)
            .map_err(|e| format!("Failed to read block: {}", e))?;

        let slot_id = block.append_tuple(&row_bytes)
            .ok_or_else(|| "Block full".to_string())?;

        table_file.write_block(segment_id, block_id, &block)
            .map_err(|e| format!("Failed to write block: {}", e))?;

        // TODO: Update primary key index if table has one
        let _tuple_ptr = TuplePointer::new(segment_id, block_id, slot_id);

        Ok(())
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let table_file = self.table_files.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;

        let mut rows = Vec::new();

        // Scan segment 0 (first segment allocated)
        let segment_id = 0u32;
        let header = table_file.read_segment_header(segment_id)
            .map_err(|e| format!("Failed to read segment header: {}", e))?;

        // Scan all used blocks
        for block_id in 0..base::BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            if !header.is_block_free(block_id) {
                let block = table_file.read_block(segment_id, block_id)
                    .map_err(|e| format!("Failed to read block: {}", e))?;

                // Read all slots in block
                let slot_count = block.header().slot_count;
                for slot_id in 0..slot_count {
                    if let Some(tuple_bytes) = block.read_tuple(slot_id) {
                        let (row, _): (Row, usize) = bincode::decode_from_slice(tuple_bytes, bincode::config::standard())
                            .map_err(|e| format!("Deserialization error: {}", e))?;
                        rows.push(row);
                    }
                }
            }
        }

        Ok(rows)
    }

    pub fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let metadata = self.get_table(table_name)?;
        Ok(metadata.schema.clone())
    }

    /// Read a block from storage (for index/executor use)
    pub fn read_block(&self, _segment_id: u32, _block_id: u8) -> Result<base::Block> {
        // TODO: Implement per-table access to blocks
        Err("read_block not yet implemented with per-file architecture".to_string())
    }

    /// Update primary key index when a row is inserted (STUB)
    fn update_primary_index(&mut self, _table_name: &str, _key: u64, _tuple_ptr: TuplePointer) -> Result<()> {
        // TODO: Implement index updates when we wire up IndexFile
        Ok(())
    }

    /// Point lookup using primary index (STUB)
    pub fn get_by_key(&self, _table_name: &str, _key: u64) -> Result<Option<TuplePointer>> {
        // TODO: Implement once IndexDiskManager is wired up
        Ok(None)
    }

    /// Range scan using primary index (STUB)
    pub fn range_scan_index(&self, _table_name: &str, _start_key: u64, _end_key: u64) -> Result<Vec<TuplePointer>> {
        // TODO: Implement once IndexDiskManager is wired up
        Ok(Vec::new())
    }
}