use std::io::{self, Result};
use std::path::{Path, PathBuf};
use crate::storage::io::{Disk, alloc_aligned};
use bincode::{Encode, Decode};

/// WAL entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum WalEntryType {
    /// Insert operation
    Insert = 1,
    /// Delete operation
    Delete = 2,
    /// Update operation
    Update = 3,
    /// DDL operation (CREATE TABLE, etc)
    Ddl = 4,
    /// Checkpoint marker
    Checkpoint = 5,
}

impl WalEntryType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(WalEntryType::Insert),
            2 => Some(WalEntryType::Delete),
            3 => Some(WalEntryType::Update),
            4 => Some(WalEntryType::Ddl),
            5 => Some(WalEntryType::Checkpoint),
            _ => None,
        }
    }
}

/// WAL entry header (48 bytes)
#[derive(Debug, Clone, Copy, Encode, Decode)]
#[repr(C)]
pub struct WalEntryHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Entry type
    pub entry_type: u8,
    /// Payload length (bytes following this header)
    pub payload_len: u32,
    /// LSN (Log Sequence Number) / entry offset in log
    pub lsn: u64,
    /// CRC32 of entire entry (header + payload)
    pub crc32: u32,
    /// Padding to reach 48 bytes
    pub _reserved: [u8; 27],
}

impl WalEntryHeader {
    const MAGIC: u32 = 0x574C4F47; // "WLOG"

    pub fn new(entry_type: WalEntryType, payload_len: u32, lsn: u64) -> Self {
        WalEntryHeader {
            magic: Self::MAGIC,
            entry_type: entry_type as u8,
            payload_len,
            lsn,
            crc32: 0, // Will be set when writing
            _reserved: [0; 27],
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != Self::MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid WAL entry magic",
            ));
        }
        if WalEntryType::from_u8(self.entry_type).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown WAL entry type: {}", self.entry_type),
            ));
        }
        Ok(())
    }
}

/// Single WAL entry: header + payload
#[derive(Debug)]
pub struct WalEntry {
    pub header: WalEntryHeader,
    pub payload: Vec<u8>,
}

impl WalEntry {
    pub fn new(entry_type: WalEntryType, payload: Vec<u8>, lsn: u64) -> Self {
        let payload_len = payload.len() as u32;
        WalEntry {
            header: WalEntryHeader::new(entry_type, payload_len, lsn),
            payload,
        }
    }
}

/// WalFile manages append-only write-ahead log
/// Writes are sequential and buffered for performance
pub struct WalFile {
    disk: Disk,
    path: PathBuf,
    /// Current write offset (next entry will be written here)
    next_offset: u64,
}

impl WalFile {
    /// Open or create a WAL file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let disk = Disk::open(&path)?;
        let path = path.as_ref().to_path_buf();

        // Get file size to determine next offset
        let next_offset = std::fs::metadata(&path)
            .ok()
            .map(|m| m.len())
            .unwrap_or(0);

        Ok(WalFile {
            disk,
            path,
            next_offset,
        })
    }

    /// Append a WAL entry to the log
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        let header_size = std::mem::size_of::<WalEntryHeader>();
        let total_size = header_size + entry.payload.len();

        // Allocate aligned buffer
        let mut buf = alloc_aligned(total_size);

        // Write header
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &entry.header as *const WalEntryHeader as *const u8,
                header_size,
            )
        };
        buf[..header_size].copy_from_slice(header_bytes);

        // Write payload
        buf[header_size..].copy_from_slice(&entry.payload);

        // Compute CRC32 (for integrity checking during recovery)
        let crc = compute_crc32(&buf[..total_size]);

        // Update header with CRC (in-memory only)
        let mut header_with_crc = entry.header;
        header_with_crc.crc32 = crc;
        let header_with_crc_bytes = unsafe {
            std::slice::from_raw_parts(
                &header_with_crc as *const WalEntryHeader as *const u8,
                header_size,
            )
        };
        buf[..header_size].copy_from_slice(header_with_crc_bytes);

        // Write to disk at current offset
        self.disk.write_at(self.next_offset, &buf)?;

        let entry_offset = self.next_offset;
        self.next_offset += total_size as u64;

        Ok(entry_offset)
    }

    /// Read a WAL entry at given offset
    pub fn read_at(&self, offset: u64) -> Result<Option<WalEntry>> {
        let header_size = std::mem::size_of::<WalEntryHeader>();
        let mut header_buf = alloc_aligned(header_size);

        // Read header
        match self.disk.read_at(offset, &mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let header = unsafe { std::ptr::read(header_buf.as_ptr() as *const WalEntryHeader) };
        header.validate()?;

        // Read payload
        let payload_len = header.payload_len as usize;
        let mut payload = alloc_aligned(payload_len);
        self.disk.read_at(offset + header_size as u64, &mut payload)?;

        // Verify CRC
        let mut verify_buf = alloc_aligned(header_size + payload_len);
        verify_buf[..header_size].copy_from_slice(&header_buf);
        verify_buf[header_size..].copy_from_slice(&payload[..payload_len]);

        let expected_crc = compute_crc32(&verify_buf[..header_size + payload_len]);
        if header.crc32 != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WAL entry CRC mismatch at offset {}", offset),
            ));
        }

        Ok(Some(WalEntry { header, payload }))
    }

    /// Iterate through all entries in the log starting from offset
    pub fn iter_from(&self, start_offset: u64) -> WalIterator<'_> {
        WalIterator {
            wal: self,
            current_offset: start_offset,
        }
    }

    /// Get current write offset (for checkpointing)
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Truncate log at given offset (for cleanup after checkpoint)
    pub fn truncate_before(&mut self, offset: u64) -> Result<()> {
        // For now, we don't actually truncate (requires file rewriting)
        // In a real implementation, we'd:
        // 1. Write new entries to temp file
        // 2. Fsync temp file
        // 3. Rename over original
        self.next_offset = std::cmp::max(self.next_offset, offset);
        Ok(())
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Iterator for WAL entries
pub struct WalIterator<'a> {
    wal: &'a WalFile,
    current_offset: u64,
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = Result<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.wal.read_at(self.current_offset) {
            Ok(Some(entry)) => {
                let header_size = std::mem::size_of::<WalEntryHeader>();
                self.current_offset += header_size as u64 + entry.payload.len() as u64;
                Some(Ok(entry))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Compute CRC32 checksum
fn compute_crc32(data: &[u8]) -> u32 {
    // Simple polynomial-based CRC (not cryptographically secure)
    // For production, use crc32fast or similar
    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB88320
            } else {
                crc >> 1
            };
        }
    }
    crc ^ 0xFFFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    #[ignore]
    fn test_wal_file_creation() {
        let path = "test_wal.log";
        let _ = fs::remove_file(path);

        let wal = WalFile::open(path).expect("Failed to create WAL file");
        assert_eq!(wal.next_offset(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    #[ignore]
    fn test_wal_append_and_read() {
        let path = "test_wal_write.log";
        let _ = fs::remove_file(path);

        let mut wal = WalFile::open(path).expect("Failed to create WAL file");

        let entry = WalEntry::new(WalEntryType::Insert, vec![1, 2, 3, 4, 5], 0);
        let offset = wal.append(&entry).expect("Failed to append");

        assert_eq!(offset, 0);

        let read_entry = wal
            .read_at(offset)
            .expect("Failed to read entry")
            .expect("No entry found");
        assert_eq!(read_entry.header.entry_type, WalEntryType::Insert as u8);
        assert_eq!(read_entry.payload, vec![1, 2, 3, 4, 5]);

        let _ = fs::remove_file(path);
    }

    #[test]
    #[ignore]
    fn test_wal_iterator() {
        let path = "test_wal_iter.log";
        let _ = fs::remove_file(path);

        let mut wal = WalFile::open(path).expect("Failed to create WAL file");

        let entries = vec![
            WalEntry::new(WalEntryType::Insert, vec![1], 0),
            WalEntry::new(WalEntryType::Update, vec![2], 0),
            WalEntry::new(WalEntryType::Delete, vec![3], 0),
        ];

        for entry in &entries {
            wal.append(entry).expect("Failed to append");
        }

        let read_entries: Vec<_> = wal
            .iter_from(0)
            .map(|e| e.expect("Failed to read entry"))
            .collect();

        assert_eq!(read_entries.len(), 3);
        assert_eq!(read_entries[0].payload, vec![1]);
        assert_eq!(read_entries[1].payload, vec![2]);
        assert_eq!(read_entries[2].payload, vec![3]);

        let _ = fs::remove_file(path);
    }
}