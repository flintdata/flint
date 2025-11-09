use std::collections::HashMap;
use std::io::{self, Result};
use std::sync::atomic::{AtomicU8, Ordering};
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use crate::types::Schema;

/// Metadata about a single index file
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct IndexFileMetadata {
    /// Logical index name
    pub name: String,
    /// Index type (e.g., "btree", "hash")
    pub index_type: String,
    /// Path to the .idx file
    pub file_path: String,
    /// Root page segment ID
    pub root_page_segment: u16,
    /// Root page offset
    pub root_page_offset: u16,
}

/// Metadata about a single table file
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct TableFileMetadata {
    /// Table name
    pub name: String,
    /// Path to the .tbl file
    pub file_path: String,
    /// Table schema
    pub schema: Schema,
    /// Next segment ID to allocate (for recovery)
    pub next_segment_id: u32,
    /// Primary key index (if any)
    pub primary_index: Option<IndexFileMetadata>,
    /// Secondary indexes
    pub secondary_indexes: Vec<IndexFileMetadata>,
}

/// Global catalog header
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct CatalogHeader {
    /// Catalog version
    pub version: u32,
    /// Number of tables
    pub num_tables: u32,
    /// Checksum of metadata bytes
    pub checksum: u64,
}

impl CatalogHeader {
    pub fn new() -> Self {
        CatalogHeader {
            version: 1,
            num_tables: 0,
            checksum: 0,
        }
    }
}

/// Manages global database catalog with per-file metadata
/// Uses dual-segment atomic writes for durability (like original metadata)
pub struct Catalog {
    /// Active metadata segment (0 or 1)
    active_segment: AtomicU8,
    /// All table metadata indexed by name
    tables: HashMap<String, TableFileMetadata>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Catalog {
            active_segment: AtomicU8::new(0),
            tables: HashMap::new(),
        }
    }

    /// Get the active metadata segment (0 or 1)
    pub fn active_segment(&self) -> u8 {
        self.active_segment.load(Ordering::SeqCst)
    }

    /// Get the inactive metadata segment (0 or 1)
    pub fn inactive_segment(&self) -> u8 {
        let active = self.active_segment.load(Ordering::SeqCst);
        1 - active
    }

    /// Flip to use the other metadata segment
    pub fn flip_segment(&self) {
        let current = self.active_segment.load(Ordering::SeqCst);
        self.active_segment.store(1 - current, Ordering::SeqCst);
    }

    /// Register a new table in the catalog
    pub fn add_table(&mut self, metadata: TableFileMetadata) -> Result<()> {
        self.tables.insert(metadata.name.clone(), metadata);
        Ok(())
    }

    /// Get table metadata by name
    pub fn get_table(&self, name: &str) -> Result<Option<&TableFileMetadata>> {
        Ok(self.tables.get(name))
    }

    /// Get all tables
    pub fn all_tables(&self) -> Vec<&TableFileMetadata> {
        self.tables.values().collect()
    }

    /// Remove a table from the catalog
    pub fn remove_table(&mut self, name: &str) -> Result<Option<TableFileMetadata>> {
        Ok(self.tables.remove(name))
    }

    /// Serialize catalog to bytes for persistence
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut header = CatalogHeader::new();
        header.num_tables = self.tables.len() as u32;

        // Serialize all table metadata
        let mut table_bytes = Vec::new();
        for table_meta in self.tables.values() {
            let encoded = bincode::encode_to_vec(table_meta, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            table_bytes.extend_from_slice(&encoded);
        }

        // Compute checksum
        header.checksum = compute_checksum(&table_bytes);

        // Encode header + tables
        let mut result = bincode::encode_to_vec(&header, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        result.extend_from_slice(&table_bytes);

        Ok(result)
    }

    /// Deserialize catalog from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let (header, bytes_read): (CatalogHeader, usize) =
            bincode::decode_from_slice(data, bincode::config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        // Verify checksum
        let table_bytes = &data[bytes_read..];
        let expected_checksum = compute_checksum(table_bytes);
        if header.checksum != expected_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Catalog checksum mismatch: expected {}, got {}", expected_checksum, header.checksum),
            ));
        }

        // Deserialize tables
        let mut catalog = Catalog::new();
        let mut offset = 0;
        for _ in 0..header.num_tables {
            let (metadata, bytes_read): (TableFileMetadata, usize) =
                bincode::decode_from_slice(&table_bytes[offset..], bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            catalog.tables.insert(metadata.name.clone(), metadata);
            offset += bytes_read;
        }

        Ok(catalog)
    }
}

/// Compute simple checksum for metadata validation
fn compute_checksum(data: &[u8]) -> u64 {
    data.iter().fold(0u64, |acc, &byte| {
        acc.wrapping_mul(31).wrapping_add(byte as u64)
    })
}
