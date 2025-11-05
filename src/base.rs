pub const SEGMENT_SIZE: usize = 2 * 1024 * 1024; // 2MB
const PAGE_SIZE: usize = 4096; // 4KB
const HEADER_SIZE: usize = PAGE_SIZE; // 4KB, one page

#[repr(C, align(4096))]
struct CatalogSegment {
    magic: u64,
    version: u64,
    next_segment_id: u32,
    next_table_id: u32,
    next_tx_id: u64,
    segment_bitmap: [u64; 8192],
    _padding: [u8; 1], // TODO
}

#[repr(C)]
struct TableMetadata {
    table_id: u32,
    name_len: u32,
    name_offset: u32,
    segment_count: u32,
    segment_list_offset: u32,
    schema_offset: u32,
    flags: u32,
}

#[repr(C, align(4096))]
struct SegmentHeader {
    id: u32,
    table_id: u32,
    slots: u32,
    unused: u32,
    xmin_horizon: u64,
    xmax_horizon: u64,
    slot_bitmap: [u64; 256], // 16K slots maximum
    checksum: u64,
    _padding: [u8; 2008] // Pad to 4KB TODO
}

#[repr(C)]
struct Slot {
    xmin: u64, // tx that created the version
    xmax: u64, // tx that deleted (0, visible)
    offset: u32,
    len: u32,
}
