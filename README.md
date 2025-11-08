# flint

A *lighter* database

## Architecture

Flint follows a variation of the heap disk format that I've dubbed Log-structured
Segmented Heap (LSH). The heap is split into segments (each segment corresponds
to only one table) of 2MB. Our writes first go to an in-memory table + write-ahead
log (WAL). In high-throughput scenarios where we have a growing queue of 
in-memory tables, we append new segments to the heap file, permanently growing
the file. In all other cases we have a background vacuum process that queues up
segments sorted by recency of last write (for temporal locality) and the amount
of free slots in a segment. We can then perform insertions of inserts and updates
of tuples to the "old" segment reclaiming dead tuple slots. Additionally,
between segments (2MB) and pages (4KB), segments are split into *blocks* (64KB).
This is the compressible unit and the unit of writes from the flushed in-memory
tables. For point inserts and updates we also are able to do single page writes
for uncompressed pages.

This architecture hopes to take advantage of the write-throughput of LSM
designs utilizing in-memory tables + WAL, while avoiding the write-amplification
induced by leveling and compaction of sstables. This design also maintains the
advantage of traditional heap structure's superior point queries. We also leave
room for optimizations such as compression and Postgres' HOT tuple locality.

### Indexes

Indexing follows the MySQL primary-key indirection approach instead of updating
every index for every tuple update. At the *slight* cost of read latency, we
can maintain our higher write throughput we gained through the LSH architecture.

### MVCC

Similarly to Postgres, Flint performs tuple level MVCC. All tuples are immutable
once written, until they are automatically vacuumed.

### Compression

Flint (will) implements LZ4 and Zstd compression at the 64KB block level. Blocks
go through two phases when compression is enabled. Phase 1: an uncompressed
block fills to 64KB. At which point this block is eligible for compression in
future writes. We assume that if an entire block is written at once that there
is a large sum of writes at the time and that we want to defer compression to a
later time. Phase 2: compression of the block with inserts. The block is filled
up to a set maximum that gives enough buffer space for future updates. Updates
can have the same uncompressed size, but differing compression ratios so space
is sacrificed to avoid overwrites that go beyond a block's fixed 64KB size (note:
blocks are always padded to 64KB length whether compressed or not).

Phase 1 and 2 make up the transition phase of a block. Once compressed a block
stays compressed (hot blocks defer compression for this reason). Operations on
the block are now always in at block-level granularity; reads and writes. A
compressed block maintains the buffer space so it can continue to be mutable.
Its dead slots and vacuumed and reused for new point updates.

## Source Layout
```
  Server
    └─> Handler
          └─> Executor
                ├─> Parser (SQL → AST)
                ├─> Planner (AST → Plan)
                ├─> Storage Engine (data)
                └─> execute_plan(Plan, Storage) → Response
```