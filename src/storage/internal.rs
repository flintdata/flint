use std::io::{self, Result};
use std::path::Path;

use crate::storage::base::*;
use crate::storage::io::{Disk, alloc_aligned};
use zerocopy::{IntoBytes, FromBytes};

/// Page size (4KB) - buffer pool granularity
pub const PAGE_SIZE: usize = 4096;

/// Pages per block (64KB / 4KB = 16)
pub const PAGES_PER_BLOCK: usize = BLOCK_SIZE / PAGE_SIZE;

/// Database file managing multiple segments
pub struct DatabaseFile {
    disk: Disk,
}

impl DatabaseFile {
    /// Open or create database file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let disk = Disk::open(path)?;
        Ok(DatabaseFile { disk })
    }

    /// Calculate file offset for segment header
    fn segment_offset(segment_id: SegmentId) -> u64 {
        segment_id as u64 * SEGMENT_SIZE as u64
    }

    /// Calculate file offset for block within segment
    fn block_offset(segment_id: SegmentId, block_id: BlockId) -> u64 {
        Self::segment_offset(segment_id)
            + SEGMENT_HEADER_SIZE as u64
            + (block_id as u64 * BLOCK_SIZE as u64)
    }

    /// Read segment header (64KB)
    pub fn read_segment_header(&self, segment_id: SegmentId) -> Result<SegmentHeader> {
        let offset = Self::segment_offset(segment_id);
        let mut buf = alloc_aligned(SEGMENT_HEADER_SIZE);
        self.disk.read_at(offset, &mut buf)?;

        // Deserialize header
        let header = match SegmentHeader::read_from_bytes(&buf[..std::mem::size_of::<SegmentHeader>()]) {
            Ok(h) => h,
            Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "Failed to read segment header")),
        };

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
    pub fn write_segment_header(&self, segment_id: SegmentId, header: &SegmentHeader) -> Result<()> {
        let offset = Self::segment_offset(segment_id);
        let mut buf = alloc_aligned(SEGMENT_HEADER_SIZE);

        // Serialize header
        let header_bytes = header.as_bytes();
        buf[..header_bytes.len()].copy_from_slice(header_bytes);

        self.disk.write_at(offset, &buf)?;
        Ok(())
    }

    /// Read block (64KB) - atomic read unit
    pub fn read_block(&self, segment_id: SegmentId, block_id: BlockId) -> Result<Block> {
        if block_id >= BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_id {} out of range", block_id),
            ));
        }

        let offset = Self::block_offset(segment_id, block_id);
        // Allocate as Vec<u32> to ensure 4-byte alignment for zerocopy
        let num_u32s = BLOCK_SIZE / std::mem::size_of::<u32>();
        let mut data = vec![0u32; num_u32s];
        let data_bytes = data.as_mut_bytes();
        self.disk.read_at(offset, data_bytes)?;

        Ok(Block { data })
    }

    /// Write block (64KB) - atomic write unit
    pub fn write_block(&self, segment_id: SegmentId, block_id: BlockId, block: &Block) -> Result<()> {
        if block_id >= BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_id {} out of range", block_id),
            ));
        }

        let offset = Self::block_offset(segment_id, block_id);
        self.disk.write_at(offset, block.as_bytes())?;
        Ok(())
    }

    /// Initialize a new segment
    pub fn initialize_segment(&self, segment_id: SegmentId) -> Result<()> {
        let header = SegmentHeader::new(segment_id);
        self.write_segment_header(segment_id, &header)
    }

    /// Allocate a free block in segment
    pub fn allocate_block(&self, segment_id: SegmentId) -> Result<Option<BlockId>> {
        // Segments 0 and 1 are reserved for metadata
        if segment_id == 0 || segment_id == 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot allocate blocks in segments 0-1 (reserved for metadata)",
            ));
        }

        let mut header = self.read_segment_header(segment_id)?;

        // Find first free block
        for block_id in 0..BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            if header.is_block_free(block_id) {
                header.mark_block_used(block_id);
                self.write_segment_header(segment_id, &header)?;
                return Ok(Some(block_id));
            }
        }

        Ok(None) // Segment full
    }

    /// Free a block in segment
    pub fn free_block(&self, segment_id: SegmentId, block_id: BlockId) -> Result<()> {
        let mut header = self.read_segment_header(segment_id)?;
        header.mark_block_free(block_id);
        self.write_segment_header(segment_id, &header)?;
        Ok(())
    }

    /// Read a single page (4KB) from uncompressed block
    /// Only use for uncompressed blocks - compressed blocks must read full block
    pub fn read_page(&self, segment_id: SegmentId, block_id: BlockId, page_id: u8) -> Result<Vec<u8>> {
        if block_id >= BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("block_id {} out of range", block_id),
            ));
        }

        if page_id >= PAGES_PER_BLOCK as u8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("page_id {} out of range", page_id),
            ));
        }

        let offset = Self::block_offset(segment_id, block_id) + (page_id as u64 * PAGE_SIZE as u64);
        let mut buf = alloc_aligned(PAGE_SIZE);
        self.disk.read_at(offset, &mut buf)?;

        Ok(buf)
    }

    /// Read multiple pages (4KB each) from uncompressed block
    /// Returns pages in order requested
    pub fn read_pages(&self, segment_id: SegmentId, block_id: BlockId, page_ids: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut pages = Vec::new();
        for &page_id in page_ids {
            let page = self.read_page(segment_id, block_id, page_id)?;
            pages.push(page);
        }
        Ok(pages)
    }
}