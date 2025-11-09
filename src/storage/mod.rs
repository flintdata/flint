mod io;
pub mod base;
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
use parking_lot::{Mutex, RwLock};
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use tracing::debug;
use crate::types::{Row, Schema};
use crate::config::Config;
#[cfg(feature = "extensions")]
use crate::extensions::registry::{TypeRegistry, OperatorRegistry, FunctionRegistry};
use self::index::IndexBuilderRegistry;
use self::files::{TableFile, IndexFile};
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

/// Index metadata - wraps the actual index instance
pub struct IndexMetadata {
    pub name: String,
    pub column: String,
    pub index_type: String,
    /// The actual index instance (manages its own root page ID)
    /// TODO replace Mutex with lockless pattern
    pub index: Arc<Mutex<Box<dyn index::Index>>>,
}

/// Runtime table metadata (file paths + schema)
pub struct TableMetadata {
    pub name: String,
    pub file_path: PathBuf,
    pub schema: Schema,
    /// Primary key index (None if no PK defined)
    pub primary_index: Option<IndexMetadata>,
    /// Secondary indexes
    pub secondary_indexes: Vec<IndexMetadata>,
}

/// Database with per-table file storage
pub struct Database {
    /// Per-table file handles
    table_files: HashMap<String, Arc<TableFile>>,
    /// Per-table primary index file handles
    index_files: HashMap<String, Arc<IndexFile>>,
    /// Runtime metadata (paths + schemas, wrapped for concurrent access)
    /// Both primary and secondary indexes are stored in TableMetadata
    tables: HashMap<String, Arc<RwLock<TableMetadata>>>,
    /// Global catalog metadata
    catalog: Catalog,
    /// Index builder registry (always available with builtins)
    pub index_builder_registry: Arc<IndexBuilderRegistry>,
    /// Extension registries for types, operators, functions
    #[cfg(feature = "extensions")]
    pub type_registry: Arc<TypeRegistry>,
    #[cfg(feature = "extensions")]
    pub operator_registry: Arc<OperatorRegistry>,
    #[cfg(feature = "extensions")]
    pub function_registry: Arc<FunctionRegistry>,
}

impl Database {
    pub fn new(config: &Config) -> Self {
        // Initialize global catalog from catalog.db or create new
        let catalog = Catalog::new();

        // Always initialize index_builder_registry with builtins
        let mut index_builder_registry = IndexBuilderRegistry::new();
        crate::extensions::builtin::register_builtin_indexes(&mut index_builder_registry);

        #[cfg(feature = "extensions")]
        let mut db = {
            // Initialize registries with built-in types
            let mut type_registry = TypeRegistry::new();
            crate::extensions::builtin::register_builtin_types(&mut type_registry);

            let mut operator_registry = OperatorRegistry::new();
            let mut function_registry = FunctionRegistry::new();

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
                index_files: HashMap::new(),
                tables: HashMap::new(),
                catalog,
                type_registry: Arc::new(type_registry),
                operator_registry: Arc::new(operator_registry),
                function_registry: Arc::new(function_registry),
                index_builder_registry: Arc::new(index_builder_registry),
            }
        };

        #[cfg(not(feature = "extensions"))]
        let mut db = Database {
            table_files: HashMap::new(),
            index_files: HashMap::new(),
            tables: HashMap::new(),
            catalog,
            index_builder_registry: Arc::new(index_builder_registry),
        };

        // Try to load catalog from disk (TODO: implement catalog.db disk I/O)
        let _ = db.load_catalog_from_disk();

        db
    }

    /// Load catalog from catalog.db file
    fn load_catalog_from_disk(&mut self) -> Result<()> {
        use std::fs;

        // Try to load from active segment (0 or 1)
        let active_seg = self.catalog.active_segment();
        let catalog_path = format!("catalog_{}.db", active_seg);

        let data = match fs::read(&catalog_path) {
            Ok(data) => data,
            Err(_) => return Ok(()), // No catalog file yet, start with empty
        };

        match catalog::Catalog::deserialize(&data) {
            Ok(loaded_catalog) => {
                // Replace catalog with loaded version
                self.catalog = loaded_catalog;

                // Reconstruct runtime metadata and indexes from catalog
                for table_meta in self.catalog.all_tables() {
                    // Open table file
                    let table_path = PathBuf::from(&table_meta.file_path);
                    let table_file = TableFile::open(&table_path)
                        .map_err(|e| format!("Failed to open table file during recovery: {}", e))?;

                    // Reconstruct primary index if it exists
                    let primary_index = if let Some(index_meta) = &table_meta.primary_index {
                        let index_path = PathBuf::from(&index_meta.file_path);
                        let index_file = IndexFile::open(&index_path)
                            .map_err(|e| format!("Failed to open index file during recovery: {}", e))?;

                        let root_page_id = base::PageId::new(index_meta.root_page_segment, index_meta.root_page_offset);
                        let index = self.index_builder_registry.create_index(&index_meta.index_type, Some(root_page_id))
                            .ok_or_else(|| format!("Failed to create {} index during recovery", index_meta.index_type))?;

                        self.index_files.insert(table_meta.name.clone(), Arc::new(index_file));

                        // Get primary key column from schema
                        let pk_column = table_meta.schema.columns.iter()
                            .find(|col| col.is_primary_key)
                            .or_else(|| table_meta.schema.columns.first())
                            .map(|col| col.name.clone())
                            .unwrap_or_else(|| "".to_string());

                        Some(IndexMetadata {
                            name: index_meta.name.clone(),
                            column: pk_column,
                            index_type: index_meta.index_type.clone(),
                            index: Arc::new(Mutex::new(index)),
                        })
                    } else {
                        None
                    };

                    // Build runtime table metadata
                    let runtime_meta = TableMetadata {
                        name: table_meta.name.clone(),
                        file_path: table_path,
                        schema: table_meta.schema.clone(),
                        primary_index,
                        secondary_indexes: Vec::new(),
                    };

                    self.tables.insert(table_meta.name.clone(), Arc::new(RwLock::new(runtime_meta)));
                    self.table_files.insert(table_meta.name.clone(), Arc::new(table_file));
                }

                Ok(())
            }
            Err(_) => {
                // Corruption in active segment, try inactive
                let inactive_seg = self.catalog.inactive_segment();
                let fallback_path = format!("catalog_{}.db", inactive_seg);

                let fallback_data = fs::read(&fallback_path)
                    .map_err(|_| "Failed to load catalog from either segment".to_string())?;

                let fallback_catalog = catalog::Catalog::deserialize(&fallback_data)
                    .map_err(|e| format!("Failed to deserialize fallback catalog: {}", e))?;

                // Use fallback catalog and flip segment
                self.catalog = fallback_catalog;
                self.catalog.flip_segment();

                // Recursively load with fallback catalog
                self.load_catalog_from_disk()
            }
        }
    }

    /// Save catalog to catalog.db file with atomic flip
    fn save_catalog_to_disk(&mut self) -> Result<()> {
        use std::fs;
        use std::io::Write;

        // Get inactive segment to write to
        let inactive_seg = self.catalog.inactive_segment();
        let temp_path = format!("catalog_{}.tmp", inactive_seg);
        let final_path = format!("catalog_{}.db", inactive_seg);

        // Serialize catalog
        let data = self.catalog.serialize()
            .map_err(|e| format!("Failed to serialize catalog: {}", e))?;

        // Write to temp file first
        let mut temp_file = fs::File::create(&temp_path)
            .map_err(|e| format!("Failed to create temp catalog file: {}", e))?;

        temp_file.write_all(&data)
            .map_err(|e| format!("Failed to write catalog file: {}", e))?;

        temp_file.sync_all()
            .map_err(|e| format!("Failed to sync catalog file: {}", e))?;

        // Atomic rename
        fs::rename(&temp_path, &final_path)
            .map_err(|e| format!("Failed to rename catalog file: {}", e))?;

        // Flip segment
        self.catalog.flip_segment();

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
        let _segment_id = table_file.allocate_segment()
            .map_err(|e| format!("Failed to allocate segment: {}", e))?;

        // Create and initialize primary index
        let index_file_path = PathBuf::from(format!("index_{}_{}.idx", name, "pk"));
        let index_file = IndexFile::open(&index_file_path)
            .map_err(|e| format!("Failed to open index file: {}", e))?;

        // Allocate root page for the primary index
        let root_page_id = index_file.allocate_page()
            .map_err(|e| format!("Failed to allocate index root page: {}", e))?;

        // Create BTree index via registry
        let index = self.index_builder_registry.create_index("btree", Some(root_page_id))
            .ok_or_else(|| "Failed to create btree index".to_string())?;

        let primary_index = Some(IndexMetadata {
            name: "pk".to_string(),
            column: "".to_string(), // Primary key column determined by schema
            index_type: "btree".to_string(),
            index: Arc::new(Mutex::new(index)),
        });

        // Create runtime metadata
        let metadata_schema = schema.clone();
        let metadata = TableMetadata {
            name: name.clone(),
            file_path: file_path.clone(),
            schema,
            primary_index,
            secondary_indexes: Vec::new(),
        };

        // Insert into runtime tables (wrapped in Arc<RwLock<>>)
        // TODO check or mutex to prevent duplicate tables
        self.tables.insert(name.clone(), Arc::new(RwLock::new(metadata)));
        self.table_files.insert(name.clone(), Arc::new(table_file));
        self.index_files.insert(name.clone(), Arc::new(index_file));

        // Build and save metadata to catalog
        let primary_index_meta = catalog::IndexFileMetadata {
            name: "pk".to_string(),
            index_type: "btree".to_string(),
            file_path: index_file_path.to_string_lossy().to_string(),
            root_page_segment: root_page_id.segment_id(),
            root_page_offset: root_page_id.page_offset(),
        };

        let table_meta = catalog::TableFileMetadata {
            name: name.clone(),
            file_path: file_path.to_string_lossy().to_string(),
            schema: metadata_schema,
            next_segment_id: 1, // We allocated segment 0
            primary_index: Some(primary_index_meta),
            secondary_indexes: Vec::new(),
        };

        self.catalog.add_table(table_meta)
            .map_err(|e| format!("Failed to add table to catalog: {}", e))?;

        self.save_catalog_to_disk()?;

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<Arc<RwLock<TableMetadata>>> {
        self.tables
            .get(name)
            .cloned()
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<()> {
        let table_file = self.table_files.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?
            .clone();

        let metadata_arc = self.tables.get(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?
            .clone();
        let metadata = metadata_arc.read();

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

        // Create tuple pointer for the inserted row
        let tuple_ptr = TuplePointer::new(segment_id, block_id, slot_id);

        // Update primary key index if table has one
        if let Some(primary_index_meta) = &metadata.primary_index {
            // Extract primary key from first column (TODO: assume first column is PK)
            let key_value = row.get(0)
                .ok_or_else(|| "Row must have at least one column for primary key".to_string())?;

            // Convert Value to u64 key (handle Int type)
            let key = match key_value {
                crate::types::Value::Int(n) => *n as u64,
                crate::types::Value::Null => return Err("Primary key cannot be NULL".to_string()),
                _ => return Err(format!("Primary key must be Int type, got {:?}", key_value)),
            };

            // Get index file
            let index_file = self.index_files.get(table_name)
                .ok_or_else(|| format!("Index file not found for table: {}", table_name))?;

            // Lock index and insert
            let mut index_guard = primary_index_meta.index.lock();
            index_guard.insert(key, tuple_ptr, index_file)
                .map_err(|e| format!("Failed to insert into primary index: {}", e))?;
        }

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
        let metadata_arc = self.get_table(table_name)?;
        let metadata = metadata_arc.read();
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

    /// Point lookup using primary index
    pub fn get_by_key(&self, table_name: &str, key: u64) -> Result<Option<TuplePointer>> {
        let metadata_arc = self.get_table(table_name)?;
        let metadata = metadata_arc.read();

        // Return None if table has no primary index
        let primary_index_meta = match &metadata.primary_index {
            Some(idx) => idx,
            None => {
                debug!(table_name, "no primary index found on table");
                return Ok(None)
            },
        };

        // Get index file
        let index_file = self.index_files.get(table_name)
            .ok_or_else(|| format!("Index file not found for table: {}", table_name))?;

        // Lock index and search
        let index_guard = primary_index_meta.index.lock();
        index_guard.search(key, index_file)
            .map_err(|e| format!("Failed to search primary index: {}", e))
    }

    /// Range scan using primary index
    /// Returns all tuple pointers for keys in [start_key, end_key] inclusive
    /// Returns empty vec if table has no primary index or index doesn't support range scans
    pub fn range_scan_index(&self, table_name: &str, start_key: u64, end_key: u64) -> Result<Vec<TuplePointer>> {
        let metadata_arc = self.get_table(table_name)?;
        let metadata = metadata_arc.read();

        // Return empty if table has no primary index
        let primary_index_meta = match &metadata.primary_index {
            Some(idx) => idx,
            None => {
                debug!(table_name, "no primary index found on table");
                return Ok(Vec::new())
            },
        };

        // Check if index supports range scans
        if primary_index_meta.index.lock().capability() != index::IndexCapability::Ordered {
            // TODO do we just proceed with a full table scan?
            return Ok(Vec::new());
        }

        // Get index file
        let index_file = self.index_files.get(table_name)
            .ok_or_else(|| format!("Index file not found for table: {}", table_name))?;

        // Lock index and perform range scan
        let index_guard = primary_index_meta.index.lock();
        index_guard.range_scan(start_key, end_key, index_file)
            .map(|results| results.into_iter().map(|(_, ptr)| ptr).collect())
            .map_err(|e| format!("Failed to range scan primary index: {}", e))
    }

    /// Find a secondary index by table name and column name
    /// Returns (index_name, IndexMetadata) if found
    pub fn find_secondary_index(&self, table_name: &str, column_name: &str) -> Result<Option<(String, Arc<Mutex<Box<dyn index::Index>>>)>> {
        let metadata_arc = self.get_table(table_name)?;
        let metadata = metadata_arc.read();

        // Search secondary indexes for matching column
        for idx_meta in &metadata.secondary_indexes {
            if idx_meta.column == column_name {
                return Ok(Some((idx_meta.name.clone(), idx_meta.index.clone())));
            }
        }

        Ok(None)
    }

    /// Search a secondary index by table and column name
    /// Returns Some(TuplePointer) if found, None if not found
    pub fn search_secondary_index(&self, table_name: &str, column_name: &str, key: u64) -> Result<Option<TuplePointer>> {
        // Find the secondary index
        let index_opt = self.find_secondary_index(table_name, column_name)?;

        if let Some((index_name, index_arc)) = index_opt {
            // Get the index file using the table and index name
            let index_file_key = format!("{}_{}", table_name, index_name);
            let index_file = self.index_files.get(&index_file_key)
                .ok_or_else(|| format!("Index file not found for secondary index {}", index_name))?;

            // Search the index
            let mut index = index_arc.lock();
            index.search(key, index_file)
                .map_err(|e| format!("Index search error: {}", e))
        } else {
            Ok(None)
        }
    }

    /// Create a secondary index on a table
    pub fn create_secondary_index(&mut self, index_name: String, table_name: String, column_name: String, index_type: String) -> Result<()> {
        // Get the table metadata
        let metadata_arc = self.get_table(&table_name)?;

        // Create index file
        let index_file_path = PathBuf::from(format!("index_{}_{}_{}.idx", table_name, column_name, &index_name));
        let index_file = IndexFile::open(&index_file_path)
            .map_err(|e| format!("Failed to open index file: {}", e))?;

        // Allocate root page for the secondary index
        let root_page_id = index_file.allocate_page()
            .map_err(|e| format!("Failed to allocate index root page: {}", e))?;

        // Create index instance via registry
        let index = self.index_builder_registry.create_index(&index_type, Some(root_page_id))
            .ok_or_else(|| format!("Failed to create {} index", index_type))?;

        // Create index metadata
        let index_meta = IndexMetadata {
            name: index_name.clone(),
            column: column_name.clone(),
            index_type: index_type.clone(),
            index: Arc::new(Mutex::new(index)),
        };

        // Add to TableMetadata.secondary_indexes
        {
            let mut metadata = metadata_arc.write();
            metadata.secondary_indexes.push(index_meta);
        }

        // Store index file for later access
        let index_file_key = format!("{}_{}", table_name, index_name);
        self.index_files.insert(index_file_key, Arc::new(index_file));

        // TODO: Update catalog to persist secondary index metadata
        // catalog.add_secondary_index(...)?;

        Ok(())
    }
}