use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use zerocopy::{IntoBytes, FromBytes, Immutable, KnownLayout, Ref};

/// Block size for I/O operations (64KB)
pub const BLOCK_SIZE: usize = 64 * 1024;

/// Segment size (2MB)
pub const SEGMENT_SIZE: usize = 2 * 1024 * 1024;

/// Number of blocks per segment (31 data blocks + 1 header block)
pub const BLOCKS_PER_UNCOMPRESSED_SEGMENT: usize = 31;

/// Segment header size (64KB)
pub const SEGMENT_HEADER_SIZE: usize = BLOCK_SIZE;

/// Transaction ID for MVCC
pub type TxId = u64;

/// Segment ID (file offset = segment_id * SEGMENT_SIZE)
pub type SegmentId = u32;

/// Block ID within a segment (0-30)
pub type BlockId = u8;

/// Slot ID within a block
pub type SlotId = u16;

/// Stable tuple address
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode, Serialize, Deserialize)]
pub struct TuplePointer {
    pub segment_id: SegmentId,
    pub block_id: BlockId,
    pub slot_id: SlotId,
}

impl TuplePointer {
    pub fn new(segment_id: SegmentId, block_id: BlockId, slot_id: SlotId) -> Self {
        TuplePointer {
            segment_id,
            block_id,
            slot_id,
        }
    }

    /// Calculate file offset for the block containing this tuple
    pub fn block_offset(&self) -> u64 {
        let segment_offset = self.segment_id as u64 * SEGMENT_SIZE as u64;
        let block_offset = self.block_id as u64 * BLOCK_SIZE as u64;
        segment_offset + SEGMENT_HEADER_SIZE as u64 + block_offset
    }
}

/// MVCC metadata for each tuple
#[derive(Debug, Clone, Copy)]
pub struct TupleMeta {
    /// Transaction ID that created this tuple
    pub xmin: TxId,
    /// Transaction ID that deleted this tuple (0 if not deleted)
    pub xmax: TxId,
}

impl TupleMeta {
    pub fn new(xmin: TxId) -> Self {
        TupleMeta { xmin, xmax: 0 }
    }

    pub fn is_deleted(&self) -> bool {
        self.xmax != 0
    }

    pub fn mark_deleted(&mut self, xmax: TxId) {
        self.xmax = xmax;
    }
}

/// Segment header (64KB at start of each segment)
#[derive(IntoBytes, FromBytes, Immutable)]
#[repr(C, align(4096))]
pub struct SegmentHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Segment ID
    pub segment_id: SegmentId,
    /// Number of blocks in use
    pub blocks_used: u32,
    /// Bitmap of free blocks (bit 1 = free, bit 0 = used)
    pub block_free_bitmap: u32,
    /// Reserved for future use (block directory, bloom filters, etc.)
    pub reserved: [u8; SEGMENT_HEADER_SIZE - 16],
}

const SEGMENT_MAGIC: u32 = 0x464C4E54; // "FLNT"

impl SegmentHeader {
    pub fn new(segment_id: SegmentId) -> Self {
        SegmentHeader {
            magic: SEGMENT_MAGIC,
            segment_id,
            blocks_used: 0,
            block_free_bitmap: !0, // All blocks free
            reserved: [0; SEGMENT_HEADER_SIZE - 16],
        }
    }

    pub fn is_block_free(&self, block_id: BlockId) -> bool {
        assert!(block_id < BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8);
        (self.block_free_bitmap & (1 << block_id)) != 0
    }

    pub fn mark_block_used(&mut self, block_id: BlockId) {
        assert!(block_id < BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8);
        self.block_free_bitmap &= !(1 << block_id);
        self.blocks_used += 1;
    }

    pub fn mark_block_free(&mut self, block_id: BlockId) {
        assert!(block_id < BLOCKS_PER_UNCOMPRESSED_SEGMENT as u8);
        self.block_free_bitmap |= 1 << block_id;
        if self.blocks_used > 0 {
            self.blocks_used -= 1;
        }
    }
}

/// Block header for slotted page
/// zerocopy-verified safe layout: IntoBytes + FromBytes guarantee no padding between fields
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct BlockHeader {
    /// Number of slots in this block
    pub slot_count: u16,
    /// Flags (compression, etc.)
    pub flags: u16,
    /// Offset to start of free space
    pub free_start: u32,
    /// Offset to end of free space (grows backward from end)
    pub free_end: u32,
    /// Reserved for future use
    pub reserved: [u8; 4],
}

const BLOCK_HEADER_SIZE: usize = 16;
const _: () = assert!(size_of::<BlockHeader>() == BLOCK_HEADER_SIZE);

impl BlockHeader {
    pub fn new() -> Self {
        BlockHeader {
            slot_count: 0,
            flags: 0,
            free_start: BLOCK_HEADER_SIZE as u32,
            free_end: BLOCK_SIZE as u32,
            reserved: [0; 4],
        }
    }

    pub fn free_space(&self) -> usize {
        (self.free_end - self.free_start) as usize
    }
}

/// Slot directory entry
/// zerocopy-verified safe layout: IntoBytes + FromBytes guarantee no padding between fields
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SlotEntry {
    /// Offset to tuple data within block
    pub offset: u16,
    /// Length of tuple data
    pub length: u16,
}

const SLOT_ENTRY_SIZE: usize = 4;
const _: () = assert!(size_of::<SlotEntry>() == SLOT_ENTRY_SIZE);

impl SlotEntry {
    pub fn new(offset: u16, length: u16) -> Self {
        SlotEntry { offset, length }
    }

    pub fn is_empty(&self) -> bool {
        self.offset == 0 && self.length == 0
    }
}

/// In-memory representation of a block
/// Allocated as Vec<u32> to ensure 4-byte alignment for zerocopy safety
pub struct Block {
    /// Block data (64KB) - stored as u32 for guaranteed alignment
    pub data: Vec<u32>,
}

impl Block {
    pub fn new() -> Self {
        // Allocate as u32 to ensure 4-byte alignment for zerocopy reads
        let num_u32s = BLOCK_SIZE / size_of::<u32>();
        let mut data = vec![0u32; num_u32s];

        // Initialize header using zerocopy: convert to bytes safely
        let header = BlockHeader::new();
        let header_bytes = header.as_bytes();

        // Get mutable byte view using zerocopy's AsBytes trait
        let data_bytes = data.as_mut_bytes();
        data_bytes[..BLOCK_HEADER_SIZE].copy_from_slice(header_bytes);

        Block { data }
    }

    /// Get byte view of block data using zerocopy's AsBytes trait
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_bytes()
    }

    /// Get mutable byte view of block data using zerocopy's AsBytes trait
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        self.data.as_mut_bytes()
    }

    pub fn header(&self) -> &BlockHeader {
        // Safe: Ref::from_bytes validates alignment and returns reference without unsafe
        // Vec<u32> allocation guarantees sufficient alignment
        let bytes = self.as_bytes();
        Ref::<&[u8], BlockHeader>::from_bytes(&bytes[..BLOCK_HEADER_SIZE])
            .map(Ref::into_ref)
            .expect("Block alignment guaranteed by Vec<u32>")
    }

    pub fn header_mut(&mut self) -> &mut BlockHeader {
        // Safe: Ref::from_bytes validates alignment and returns mutable reference without unsafe
        // Vec<u32> allocation guarantees sufficient alignment
        let bytes = self.as_bytes_mut();
        Ref::<&mut [u8], BlockHeader>::from_bytes(&mut bytes[..BLOCK_HEADER_SIZE])
            .map(Ref::into_mut)
            .expect("Block alignment guaranteed by Vec<u32>")
    }

    pub fn slot(&self, slot_id: SlotId) -> &SlotEntry {
        let offset = BLOCK_HEADER_SIZE + slot_id as usize * SLOT_ENTRY_SIZE;
        let bytes = self.as_bytes();
        // Safe: Ref::from_bytes validates alignment and returns reference without unsafe
        // Vec<u32> allocation guarantees sufficient alignment
        Ref::<&[u8], SlotEntry>::from_bytes(&bytes[offset..offset + SLOT_ENTRY_SIZE])
            .map(Ref::into_ref)
            .expect("Block alignment guaranteed by Vec<u32>")
    }

    pub fn slot_mut(&mut self, slot_id: SlotId) -> &mut SlotEntry {
        let offset = BLOCK_HEADER_SIZE + slot_id as usize * SLOT_ENTRY_SIZE;
        let bytes = self.as_bytes_mut();
        // Safe: Ref::from_bytes validates alignment and returns mutable reference without unsafe
        // Vec<u32> allocation guarantees sufficient alignment
        Ref::<&mut [u8], SlotEntry>::from_bytes(&mut bytes[offset..offset + SLOT_ENTRY_SIZE])
            .map(Ref::into_mut)
            .expect("Block alignment guaranteed by Vec<u32>")
    }

    /// Read tuple data at slot
    pub fn read_tuple(&self, slot_id: SlotId) -> Option<&[u8]> {
        let slot = self.slot(slot_id);
        if slot.is_empty() {
            return None;
        }
        let bytes = self.as_bytes();
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        Some(&bytes[start..end])
    }

    /// Append tuple data to block (allocates new slot)
    pub fn append_tuple(&mut self, data: &[u8]) -> Option<SlotId> {
        // Get values from header first
        let slot_id = self.header().slot_count;
        let free_end = self.header().free_end;
        let free_space = self.header().free_space();

        // Check space for slot entry + data
        let slot_space = SLOT_ENTRY_SIZE;
        let data_space = data.len();
        let total_space = slot_space + data_space;

        if free_space < total_space {
            return None;
        }

        // Allocate from end (tuple data)
        let new_free_end = free_end - data_space as u32;
        let bytes = self.as_bytes_mut();
        bytes[new_free_end as usize..free_end as usize].copy_from_slice(data);

        // Create slot entry
        *self.slot_mut(slot_id) = SlotEntry::new(new_free_end as u16, data.len() as u16);

        // Update header
        let header = self.header_mut();
        header.slot_count += 1;
        header.free_start += SLOT_ENTRY_SIZE as u32;
        header.free_end = new_free_end;

        Some(slot_id)
    }
}

/// Page identifier for index pages (4KB)
/// Encoded as (segment_id << 16 | page_offset_in_segment)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(u32);

impl PageId {
    /// Create a PageId from segment_id and page offset
    pub fn new(segment_id: u16, page_offset: u16) -> Self {
        PageId((segment_id as u32) << 16 | page_offset as u32)
    }

    /// Extract segment_id from PageId
    pub fn segment_id(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// Extract page offset from PageId
    pub fn page_offset(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Get the raw u32 value
    pub fn raw(&self) -> u32 {
        self.0
    }
}