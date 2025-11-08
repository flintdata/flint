mod io;
mod base;
mod internal;
pub mod index;

// Re-export for extension types
pub use self::base::TuplePointer;

use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicU8, Ordering}};
use parking_lot::Mutex;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use crate::types::{Row, Schema};
use crate::config::Config;
#[cfg(feature = "extensions")]
use crate::extensions::registry::{TypeRegistry, OperatorRegistry, FunctionRegistry, IndexBuilderRegistry};
use self::internal::DatabaseFile;
use self::base::SegmentId;

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

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TableMetadata {
    pub schema: Schema,
    pub segments: Vec<SegmentId>,
    /// Primary key index root pointer (None if no PK defined)
    pub primary_index_root: Option<TuplePointer>,
    /// Secondary indexes: (index_name -> root pointer)
    pub secondary_indexes: Vec<(String, TuplePointer)>,
}

/// Database with file-based storage
pub struct Database {
    file: DatabaseFile,
    tables: HashMap<String, TableMetadata>,
    next_segment_id: Mutex<SegmentId>,
    metadata_mgr: MetadataManager,
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
        let file = DatabaseFile::open("data.db")
            .expect("Failed to open database file");

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
                file,
                tables: HashMap::new(),
                next_segment_id: Mutex::new(2), // segments 0,1 reserved for metadata
                metadata_mgr: MetadataManager::new(),
                index_latches: HashMap::new(),
                type_registry: Arc::new(type_registry),
                operator_registry: Arc::new(operator_registry),
                function_registry: Arc::new(function_registry),
                index_builder_registry: Arc::new(index_builder_registry),
            }
        };

        #[cfg(not(feature = "extensions"))]
        let mut db = Database {
            file,
            tables: HashMap::new(),
            next_segment_id: Mutex::new(2), // segments 0,1 reserved for metadata
            metadata_mgr: MetadataManager::new(),
            index_latches: HashMap::new(),
        };

        // Try to load existing metadata from active segment
        let _ = db.load_catalog();

        db
    }

    /// Load catalog from active metadata segment (0 or 1)
    fn load_catalog(&mut self) -> Result<()> {
        // Try to load from active segment first, fallback to inactive
        let active = self.metadata_mgr.get_active();
        let inactive = self.metadata_mgr.get_inactive();

        match self.load_from_segment(active) {
            Ok(()) => Ok(()),
            Err(_) => {
                // Try inactive segment as fallback
                match self.load_from_segment(inactive) {
                    Ok(()) => {
                        // If we loaded from inactive, that means active was corrupted
                        // Switch to inactive as the new active
                        self.metadata_mgr.flip();
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Load catalog from specific metadata segment
    fn load_from_segment(&mut self, segment_id: u32) -> Result<()> {
        let header_block = self.file.read_block(segment_id, 0)
            .map_err(|e| format!("Failed to read catalog from segment {}: {}", segment_id, e))?;

        // Deserialize and validate checksum
        let (catalog, bytes_read): (CatalogHeader, usize) = bincode::decode_from_slice(&header_block.data, bincode::config::standard())
            .map_err(|e| format!("Failed to decode catalog header from segment {}: {}", segment_id, e))?;

        // Verify checksum (compute from metadata bytes)
        let metadata_bytes = &header_block.data[bytes_read..];
        let expected_checksum = compute_checksum(metadata_bytes);
        if catalog.checksum != expected_checksum {
            return Err(format!("Checksum mismatch in segment {}: expected {}, got {}",
                segment_id, expected_checksum, catalog.checksum));
        }

        // Load each table metadata
        let mut offset = bytes_read;
        for (table_name, _) in &catalog.table_offsets {
            let metadata_bytes = &header_block.data[offset..];
            let (metadata, bytes_read): (TableMetadata, usize) = bincode::decode_from_slice(metadata_bytes, bincode::config::standard())
                .map_err(|e| format!("Failed to decode table {} from segment {}: {}", table_name, segment_id, e))?;

            self.tables.insert(table_name.clone(), metadata);
            offset += bytes_read;

            // Update next_segment_id based on highest segment seen
            if let Some(table) = self.tables.get(table_name) {
                if let Some(&max_seg) = table.segments.iter().max() {
                    let mut next_id = self.next_segment_id.lock();
                    *next_id = next_id.max(max_seg + 1);
                }
            }
        }

        Ok(())
    }

    /// Check if catalog would fit in a block with current tables
    fn catalog_fits(&self) -> Result<()> {
        let mut catalog = CatalogHeader::new();
        catalog.num_tables = self.tables.len() as u32;

        let mut metadata_bytes = Vec::new();
        let mut offsets = Vec::new();

        for (table_name, table_meta) in &self.tables {
            offsets.push((table_name.clone(), metadata_bytes.len() as u32));
            let encoded = bincode::encode_to_vec(table_meta, bincode::config::standard())
                .map_err(|e| format!("Failed to encode table {}: {}", table_name, e))?;
            metadata_bytes.extend_from_slice(&encoded);
        }
        catalog.table_offsets = offsets;

        let header_bytes = bincode::encode_to_vec(&catalog, bincode::config::standard())
            .map_err(|e| format!("Failed to encode catalog: {}", e))?;

        if header_bytes.len() + metadata_bytes.len() > base::BLOCK_SIZE {
            return Err(format!("Catalog too large: {} bytes (max {})",
                header_bytes.len() + metadata_bytes.len(), base::BLOCK_SIZE));
        }

        Ok(())
    }

    /// Save catalog to inactive metadata segment, fsync, then flip active segment
    fn save_catalog(&mut self) -> Result<()> {
        // Build catalog header
        let mut catalog = CatalogHeader::new();
        catalog.num_tables = self.tables.len() as u32;

        // Serialize all tables into a buffer
        let mut metadata_bytes = Vec::new();
        let mut offsets = Vec::new();

        for (table_name, table_meta) in &self.tables {
            offsets.push((table_name.clone(), metadata_bytes.len() as u32));
            let encoded = bincode::encode_to_vec(table_meta, bincode::config::standard())
                .map_err(|e| format!("Failed to encode table {}: {}", table_name, e))?;
            metadata_bytes.extend_from_slice(&encoded);
        }
        catalog.table_offsets = offsets;

        // Compute checksum of metadata bytes
        catalog.checksum = compute_checksum(&metadata_bytes);

        // Encode catalog header
        let header_bytes = bincode::encode_to_vec(&catalog, bincode::config::standard())
            .map_err(|e| format!("Failed to encode catalog: {}", e))?;

        // Get inactive segment for atomic write
        let inactive_segment = self.metadata_mgr.get_inactive();

        // Prepare block with header and metadata
        if header_bytes.len() + metadata_bytes.len() > base::BLOCK_SIZE {
            return Err(format!("Catalog too large: {} bytes (max {})",
                header_bytes.len() + metadata_bytes.len(), base::BLOCK_SIZE));
        }

        let mut block = self.file.read_block(inactive_segment, 0)
            .map_err(|e| format!("Failed to read metadata block {}: {}", inactive_segment, e))?;

        // Clear block and write new data
        block.data.fill(0);
        block.data[..header_bytes.len()].copy_from_slice(&header_bytes);
        block.data[header_bytes.len()..header_bytes.len() + metadata_bytes.len()].copy_from_slice(&metadata_bytes);

        // Write to inactive segment
        self.file.write_block(inactive_segment, 0, &block)
            .map_err(|e| format!("Failed to write metadata block {}: {}", inactive_segment, e))?;

        // After successful write, flip active segment pointer (atomic)
        self.metadata_mgr.flip();

        Ok(())
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table already exists: {}", name));
        }

        // Allocate and reserve segment ID atomically under lock
        // This ensures no two concurrent creates get the same segment ID
        let segment_id = {
            let mut next_id = self.next_segment_id.lock();
            let seg = *next_id;
            *next_id += 1;  // Increment before releasing lock
            seg
        };  // Lock released here

        let metadata = TableMetadata {
            schema,
            segments: vec![segment_id],
            primary_index_root: None,
            secondary_indexes: Vec::new(),
        };

        // Temporarily insert to check if catalog fits
        self.tables.insert(name.clone(), metadata.clone());
        if let Err(e) = self.catalog_fits() {
            // Rollback on catalog size check failure
            self.tables.remove(&name);
            return Err(e);
        }

        // Initialize segment on disk
        self.file.initialize_segment(segment_id)
            .map_err(|e| {
                // Rollback on initialization failure
                self.tables.remove(&name);
                format!("Failed to initialize segment: {}", e)
            })?;

        // Persist catalog to segment 0
        self.save_catalog()
            .map_err(|e| {
                // Rollback on persist failure
                self.tables.remove(&name);
                // NOTE: segment_id is now orphaned on disk (allocated but not in catalog)
                // TODO Future: implement freelist to reclaim orphaned segments during recovery
                e
            })?;

        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableMetadata> {
        self.tables
            .get(name)
            .ok_or_else(|| format!("Table not found: {}", name))
    }

    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<()> {
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

        // Serialize row to bytes (simplified - TODO: proper serialization)
        let row_bytes = bincode::encode_to_vec(&row, bincode::config::standard())
            .map_err(|e| format!("Serialization error: {}", e))?;

        // Find segment with space
        let segment_id = *metadata.segments.last()
            .ok_or_else(|| "No segments for table".to_string())?;

        // Try to allocate block in segment
        let block_id = self.file.allocate_block(segment_id)
            .map_err(|e| format!("Failed to allocate block: {}", e))?
            .ok_or_else(|| "Segment full - need to allocate new segment".to_string())?;

        // Read block, append tuple, write back
        let mut block = self.file.read_block(segment_id, block_id)
            .map_err(|e| format!("Failed to read block: {}", e))?;

        let slot_id = block.append_tuple(&row_bytes)
            .ok_or_else(|| "Block full".to_string())?;

        self.file.write_block(segment_id, block_id, &block)
            .map_err(|e| format!("Failed to write block: {}", e))?;

        // Update primary key index if table has one
        let tuple_ptr = TuplePointer::new(segment_id, block_id, slot_id);
        if let Some(pk_col_idx) = metadata.schema.columns.iter().position(|c| c.is_primary_key) {
            // Extract primary key value from row (convert to u64)
            if let Some(key_val) = row.values.get(pk_col_idx) {
                let key = match key_val {
                    crate::types::Value::Int(n) => *n as u64,
                    crate::types::Value::Float(f) => f.to_bits(),
                    crate::types::Value::String(s) => {
                        // Hash string to u64
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut hasher = DefaultHasher::new();
                        s.hash(&mut hasher);
                        hasher.finish()
                    }
                    _ => return Err("Primary key cannot be NULL".to_string()),
                };

                // Update index (read from disk, insert, write back)
                self.update_primary_index(table_name, key, tuple_ptr)?;
            }
        }

        Ok(())
    }

    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let metadata = self.get_table(table_name)?;

        let mut rows = Vec::new();

        // Scan all segments for table
        for &segment_id in &metadata.segments {
            let header = self.file.read_segment_header(segment_id)
                .map_err(|e| format!("Failed to read segment header: {}", e))?;

            // Scan all used blocks
            for block_id in 0..base::BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
                if !header.is_block_free(block_id) {
                    let block = self.file.read_block(segment_id, block_id)
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
        }

        Ok(rows)
    }

    pub fn get_schema(&self, table_name: &str) -> Result<Schema> {
        let metadata = self.get_table(table_name)?;
        Ok(metadata.schema.clone())
    }

    /// Read a block from storage (for index/executor use)
    pub fn read_block(&self, segment_id: u32, block_id: u8) -> Result<base::Block> {
        self.file.read_block(segment_id, block_id)
            .map_err(|e| format!("Failed to read block: {}", e))
    }

    /// Update primary key index when a row is inserted
    /// Reads index from disk, inserts key, handles splits, writes back
    /// Protected by per-table index latch for concurrent safety
    fn update_primary_index(&mut self, table_name: &str, key: u64, tuple_ptr: TuplePointer) -> Result<()> {
        // Get or create latch for this table's index (must do before metadata access)
        let index_latch = {
            let latch = self.index_latches
                .entry(table_name.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone();
            latch
        };

        // Acquire latch for serialized index access
        let _latch_guard = index_latch.lock();

        let metadata = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;

        // Get or create index root
        let (index_segment, index_block) = if let Some(root_ptr) = metadata.primary_index_root {
            (root_ptr.segment_id, root_ptr.block_id)
        } else {
            // Allocate and reserve segment ID atomically under lock
            let seg_id = {
                let mut next_id = self.next_segment_id.lock();
                let seg = *next_id;
                *next_id += 1;  // Increment before releasing lock
                seg
            };  // Lock released here

            // Initialize segment for index if not already initialized
            self.file.initialize_segment(seg_id)
                .map_err(|e| format!("Failed to initialize index segment: {}", e))?;

            // Create root index page (leaf page)
            let root_page = index::IndexPage::new(true);
            let root_block = base::Block { data: root_page.data };
            self.file.write_block(seg_id, 0, &root_block)
                .map_err(|e| format!("Failed to write index root: {}", e))?;

            // Update metadata with root pointer
            metadata.primary_index_root = Some(TuplePointer::new(seg_id, 0, 0));
            self.save_catalog()
                .map_err(|e| {
                    // NOTE: seg_id is now orphaned on disk (allocated but not in catalog)
                    // TODO Future: implement freelist to reclaim orphaned segments during recovery
                    e
                })?;

            (seg_id, 0)
        };

        // Read index root page from disk
        let mut data_block = self.file.read_block(index_segment, index_block)
            .map_err(|e| format!("Failed to read index page: {}", e))?;

        // Convert Block to IndexPage (they're both 64KB buffers with compatible data layout)
        let mut idx_page = index::IndexPage {
            data: data_block.data.clone(),
        };

        // Insert into index
        match index::BTree::insert_into_page(&mut idx_page, key, tuple_ptr)
            .map_err(|e| format!("Index insertion error: {}", e))?
        {
            None => {
                // No split, copy back and write
                data_block.data = idx_page.data;
                self.file.write_block(index_segment, index_block, &data_block)
                    .map_err(|e| format!("Failed to write index page: {}", e))?;
            }
            Some(split) => {
                // Split occurred - write left page, write right page, promote key to parent
                data_block.data = idx_page.data;
                self.file.write_block(index_segment, index_block, &data_block)
                    .map_err(|e| format!("Failed to write left index page: {}", e))?;

                // Allocate new block for right page
                let right_block_id = self.file.allocate_block(index_segment)
                    .map_err(|e| format!("Failed to allocate right index block: {}", e))?
                    .ok_or_else(|| "Index segment full".to_string())?;

                let right_block = base::Block { data: split.right_page.data };
                self.file.write_block(index_segment, right_block_id, &right_block)
                    .map_err(|e| format!("Failed to write right index page: {}", e))?;

                // For now, store the promoted key (full tree traversal implementation is future work)
                // Update root pointer if we split at root level (would create new root)
                // This is simplified - full B+ tree needs proper parent tracking
                tracing::debug!(
                    "Index split at key {}: left in ({},{}), right in ({},{})",
                    split.promoted_key,
                    index_segment,
                    index_block,
                    index_segment,
                    right_block_id
                );
            }
        }

        Ok(())
    }

    /// Point lookup using primary index
    /// Returns TuplePointer for exact key match, or None if not found
    pub fn get_by_key(&self, table_name: &str, key: u64) -> Result<Option<TuplePointer>> {
        let metadata = self.get_table(table_name)?;

        // No index means no results
        let root_ptr = metadata.primary_index_root
            .ok_or_else(|| "No primary index available".to_string())?;

        // Read root page from disk
        let root_block = self.file.read_block(root_ptr.segment_id, root_ptr.block_id)
            .map_err(|e| format!("Failed to read index page: {}", e))?;

        // Convert Block to IndexPage
        let root_page = index::IndexPage {
            data: root_block.data,
        };

        // Perform point search on root page
        // For now, the tree is single-level (root is always a leaf)
        let result = index::BTree::search_page(&root_page, key)
            .map_err(|e| format!("Index search error: {}", e))?;

        Ok(result)
    }

    /// Range scan using primary index
    /// Returns all TuplePointers for keys in [start_key, end_key]
    pub fn range_scan_index(&self, table_name: &str, start_key: u64, end_key: u64) -> Result<Vec<TuplePointer>> {
        let metadata = self.get_table(table_name)?;

        // No index means no results
        let root_ptr = metadata.primary_index_root
            .ok_or_else(|| "No primary index available".to_string())?;

        // Read root page from disk
        let root_block = self.file.read_block(root_ptr.segment_id, root_ptr.block_id)
            .map_err(|e| format!("Failed to read index page: {}", e))?;

        // Convert Block to IndexPage
        let root_page = index::IndexPage {
            data: root_block.data,
        };

        // Perform range scan on root page
        // For now, the tree is single-level (root is always a leaf)
        let entries = index::BTree::range_scan_page(&root_page, start_key, end_key)
            .map_err(|e| format!("Index range scan error: {}", e))?;

        // Extract TuplePointers from entries
        let pointers = entries.into_iter()
            .map(|(_, ptr)| ptr)
            .collect();

        Ok(pointers)
    }
}