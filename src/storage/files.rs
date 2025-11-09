use std::io::{self, Result};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use crate::storage::base::{Block, SegmentHeader, SEGMENT_SIZE, SEGMENT_HEADER_SIZE, BLOCK_SIZE, BLOCKS_PER_UNCOMPRESSED_SEGMENT};
use crate::storage::io::{Disk, alloc_aligned};
use crate::storage::base::PageId;

const PAGE_SIZE: usize = 4096;

/// TableFile manages per-table data storage in .tbl files
/// Uses 2MB segment structure identical to DatabaseFile
pub struct TableFile {
    disk: Disk,
    path: PathBuf,
    /// Next segment ID to allocate (protected by mutex for thread safety)
    next_segment_id: Mutex<u32>,
}

impl TableFile {
    /// Open or create a table file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let disk = Disk::open(&path)?;
        let path = path.as_ref().to_path_buf();

        Ok(TableFile {
            disk,
            path,
            next_segment_id: Mutex::new(0),
        })
    }

    /// Calculate file offset for segment header
    fn segment_offset(segment_id: u32) -> u64 {
        segment_id as u64 * SEGMENT_SIZE as u64
    }

    /// Calculate file offset for block within segment
    fn block_offset(segment_id: u32, block_id: u8) -> u64 {
        Self::segment_offset(segment_id)
            + SEGMENT_HEADER_SIZE as u64
            + (block_id as u64 * BLOCK_SIZE as u64)
    }

    /// Read segment header (64KB)
    pub fn read_segment_header(&self, segment_id: u32) -> Result<SegmentHeader> {
        let offset = Self::segment_offset(segment_id);
        let mut buf = alloc_aligned(SEGMENT_HEADER_SIZE);
        self.disk.read_at(offset, &mut buf)?;

        // Deserialize header
        let header = unsafe { std::ptr::read(buf.as_ptr() as *const SegmentHeader) };

        // Validate magic
        if header.magic != 0x464C4E54 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid segment magic for segment {}", segment_id),
            ));
        }

        Ok(header)
    }

    /// Write segment header (64KB)
    pub fn write_segment_header(&self, segment_id: u32, header: &SegmentHeader) -> Result<()> {
        let offset = Self::segment_offset(segment_id);
        let mut buf = alloc_aligned(SEGMENT_HEADER_SIZE);

        // Serialize header
        unsafe {
            std::ptr::copy_nonoverlapping(
                header as *const SegmentHeader as *const u8,
                buf.as_mut_ptr(),
                std::mem::size_of::<SegmentHeader>(),
            );
        }

        self.disk.write_at(offset, &buf)?;
        Ok(())
    }

    /// Read block (64KB) - atomic read unit
    pub fn read_block(&self, segment_id: u32, block_id: u8) -> Result<Block> {
        if block_id >= BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_id {} out of range", block_id),
            ));
        }

        let offset = Self::block_offset(segment_id, block_id);
        let mut buf = alloc_aligned(BLOCK_SIZE);
        self.disk.read_at(offset, &mut buf)?;

        Ok(Block { data: buf })
    }

    /// Write block (64KB) - atomic write unit
    pub fn write_block(&self, segment_id: u32, block_id: u8, block: &Block) -> Result<()> {
        if block_id >= BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_id {} out of range", block_id),
            ));
        }

        let offset = Self::block_offset(segment_id, block_id);
        self.disk.write_at(offset, &block.data)?;
        Ok(())
    }

    /// Initialize a new segment
    pub fn initialize_segment(&self, segment_id: u32) -> Result<()> {
        let header = SegmentHeader::new(segment_id);
        self.write_segment_header(segment_id, &header)
    }

    /// Allocate a free block in segment
    /// Note: segment 0 block 0 is reserved for table header
    pub fn allocate_block(&self, segment_id: u32) -> Result<Option<u8>> {
        let mut header = self.read_segment_header(segment_id)?;

        // If this is segment 0, skip block 0 (reserved for table header)
        let start_block = if segment_id == 0 { 1 } else { 0 };

        // Find first free block
        for block_id in start_block..BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            if header.is_block_free(block_id) {
                header.mark_block_used(block_id);
                self.write_segment_header(segment_id, &header)?;
                return Ok(Some(block_id));
            }
        }

        Ok(None) // Segment full
    }

    /// Allocate a new segment
    pub fn allocate_segment(&self) -> Result<u32> {
        let segment_id = {
            let mut next_id = self.next_segment_id.lock().unwrap();
            let seg = *next_id;
            *next_id += 1;
            seg
        };

        self.initialize_segment(segment_id)?;
        Ok(segment_id)
    }

    /// Get the next segment ID that would be allocated
    pub fn next_segment_id(&self) -> u32 {
        *self.next_segment_id.lock().unwrap()
    }

    /// Set the next segment ID (for recovery/loading)
    pub fn set_next_segment_id(&self, id: u32) -> Result<()> {
        let mut next_id = self.next_segment_id.lock().unwrap();
        *next_id = id;
        Ok(())
    }

    /// Free a block in segment
    pub fn free_block(&self, segment_id: u32, block_id: u8) -> Result<()> {
        let mut header = self.read_segment_header(segment_id)?;
        header.mark_block_free(block_id);
        self.write_segment_header(segment_id, &header)?;
        Ok(())
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// IndexFile manages per-index data storage in .idx files
/// Uses 4KB page-based storage (no segment wrapping)
pub struct IndexFile {
    disk: Disk,
    path: PathBuf,
    /// Next page ID to allocate (protected by mutex for thread safety)
    next_page_id: Mutex<u32>,
}

impl IndexFile {
    /// Open or create an index file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let disk = Disk::open(&path)?;
        let path = path.as_ref().to_path_buf();

        Ok(IndexFile {
            disk,
            path,
            next_page_id: Mutex::new(0),
        })
    }

    /// Calculate file offset for a page
    fn page_offset(page_id: u32) -> u64 {
        page_id as u64 * PAGE_SIZE as u64
    }

    /// Read a 4KB page from index file
    pub fn read_page(&self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = Self::page_offset(page_id.raw());
        let mut buf = alloc_aligned(PAGE_SIZE);
        self.disk.read_at(offset, &mut buf)?;
        Ok(buf)
    }

    /// Write a 4KB page to index file
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> Result<()> {
        if data.len() != PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Page data must be exactly {} bytes, got {}", PAGE_SIZE, data.len()),
            ));
        }

        let offset = Self::page_offset(page_id.raw());
        self.disk.write_at(offset, data)?;
        Ok(())
    }

    /// Allocate a new page ID
    pub fn allocate_page(&self) -> Result<PageId> {
        let page_id = {
            let mut next_id = self.next_page_id.lock().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        Ok(PageId::new(0, (page_id & 0xFFFF) as u16))
    }

    /// Get the next page ID that would be allocated
    pub fn next_page_id(&self) -> u32 {
        *self.next_page_id.lock().unwrap()
    }

    /// Set the next page ID (for recovery/loading)
    pub fn set_next_page_id(&self, id: u32) -> Result<()> {
        let mut next_id = self.next_page_id.lock().unwrap();
        *next_id = id;
        Ok(())
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_table_file_creation() {
        let path = "test_table.tbl";
        let _ = fs::remove_file(path);

        let table_file = TableFile::open(path).expect("Failed to create table file");
        assert_eq!(table_file.next_segment_id(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_allocate_segment() {
        let path = "test_segment.tbl";
        let _ = fs::remove_file(path);

        let table_file = TableFile::open(path).expect("Failed to create table file");
        let seg_id = table_file.allocate_segment().expect("Failed to allocate segment");

        assert_eq!(seg_id, 0);
        assert_eq!(table_file.next_segment_id(), 1);

        let _ = fs::remove_file(path);
    }
}