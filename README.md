<!-- flint: A lighter SQL database -->
<h1 align="center">
  <a alt="flintdata" href="https://github.com/flintdata"><img width="256" height="256" alt="flintdata-logo-complex" src="docs/logo/flint-logo-complex.png"/></a>
<br>
</h1>

<p align="center">
  <b>Flint<b><br/><br/>
  <b>A lighter SQL database</b><br/>
</p>

<!-- future use
<h3 align="center">
  <a href="https://flintdatalabs.com">Website</a> &bull;
  <a href="https://docs.flintdatalabs.com">Docs</a> &bull;
  <a href="https://blog.flintdatalabs.com">Blog</a> &bull;
  <a href="https://docs.paradedb.com/changelog/">Changelog</a>
</h3>
-->

## Use case

Use Flint if you need: OLTP with low-latency writes of a single-writer SQL 
database, high-availability, and read replication.

Use something like Cassandra or ScyllaDB if you need maximum write *throughput*
and single denormalized tables (no JOINs) with sharding across nodes.

If you do not fall within these two categories you may need to reconsider your
schema requirements. Or, your usecase may fall into a specialized category with
specific requirements. Geographic writes: Cockroach or Yubabyte, Analytics:
Clickhouse, or Flexible schema: MongoDB. There are many more application
specific databases, and you will likely know which to choose from if your
requirements necessitate them.

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

### Replication (future improvements)

Standard deployment model is a single writer database with optional read replicas.
This follows the common practice (used by Postgres) of streaming WAL changes to
read replicas. This requires zero HA infrastructure and is the default solution
for multi-instance deployments.

#### Failover and HA

Optionally, failover with a lease-based, single writer deployment is available.
With a single writer, Flint maintains the low-latency writes of the single-writer
model while having the HA benefits of multi-master deployments. The cost is a
brief amount of downtime and dropped queries (still much shorter of than
alternatives such as Postgres + Patroni). This designates 3-5 nodes in a 
membership Raft quorum, and it elects a primary write instance. (*MAYBE*) Each
database (logical) is assigned a Raft group and can assign any of the nodes as
the primary write instance. This distributes logical databases among the Flint
instances for best write-throughput.

What Flint is not. An "infinitely" scaling multi-master database. This meets
the 95% of use-cases where you have single-node writing and storage scale, but
want the option for simple to manage HA and read replication to meet read
throughput demands. You get fast writes, HA, read scaling, and operational sanity
at the cost of geographically dispersed local writes and storage scaling for
single deployments.

#### Heterogeneous Deployments (Advanced)

Each logical database can be configured to a different replica failover (RF)
count. For a 5 node deployment, you can have your primary database as an RF of 5.
So that it has 4 read replicas and failover nodes at any given time. Another
logical database may not have the same read or failover requirements (RF 1), but 
it may require greater storage scaling. You can deploy 5 nodes, 4 of which meet 
the storage capacity needs of the primary database, and 1 that meets the storage
capacity needs of the primary + secondary logical database. Thus, you have two
logical databases within the same control plane with different HA, read replica,
and capacity constraints.

Example:
```
You don't buy hardware then figure out where databases go. You decide:

db_users: 10TB, RF=3
db_analytics: 5TB, RF=1
db_events: 2TB, RF=2

Then compute required nodes:

db_users needs 3 nodes of at least 10TB each
db_analytics needs 1 node of at least 5TB
db_events needs 2 nodes of at least 2TB

Overlap these assignments:

Node 1: 10TB (db_users primary) + 2TB (db_events primary) = 12TB minimum
Node 2: 10TB (db_users replica) + 2TB (db_events replica) = 12TB minimum
Node 3: 10TB (db_users replica) = 10TB minimum
Node 4: 5TB (db_analytics) = 5TB minimum
```

This requires upfront planning, but has the benefit of deterministic hardware
procurement. Any future "scaling" is vertical scaling of nodes following this
calculation.

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

## Todo
- [ ] Support for variable length primary and secondary keys on indexes, currently
  flint currently only supports fixed-length values.
- [ ] Proper serialization of segments/blocks
- [ ] Hash indexes
- [ ] MVCC for indexes (once UPDATE and DELETE are implemented)
- [ ] Support splitting files into multi-file chunks for user fs backup convenience
- [ ] Reverse index scans
- [ ] Store table column names in a hashmap (for in-memory) once reaches capacity of a vec
- [ ] Only accepts table-level PRIMARY KEY (id) syntax, not inline id INT PRIMARY KEY
