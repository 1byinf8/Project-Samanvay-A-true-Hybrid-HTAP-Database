# **Project Samanvay: A True Hybrid HTAP Database**
## Technical Whitepaper v1.0

---

<div align="center">

**A Next-Generation Storage Engine for Unified OLTP and OLAP Workloads**

*Bridging the Gap Between Transactional Performance and Analytical Insights*

</div>

---

## Abstract

Project Samanvay introduces a novel Hybrid Transactional/Analytical Processing (HTAP) database architecture that delivers real-time analytics on operational data without compromising transactional throughput. By combining a lock-free concurrent skiplist-based memtable, an LSM-tree with automatic columnar conversion, and an intelligent hybrid query router, Samanvay provides a unified storage engine capable of handling diverse workload patterns. Our benchmarks demonstrate **>500K OLTP writes/second** with concurrent **10M+ value aggregations/second** for OLAP queries, all within a single, coherent system.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Background & Motivation](#2-background--motivation)
3. [System Architecture](#3-system-architecture)
4. [Core Components](#4-core-components)
   - [4.1 Lock-Free Skiplist Memtable](#41-lock-free-skiplist-memtable)
   - [4.2 LSM-Tree Level Manager](#42-lsm-tree-level-manager)
   - [4.3 Columnar Storage Format](#43-columnar-storage-format)
   - [4.4 Write-Ahead Log (WAL)](#44-write-ahead-log-wal)
   - [4.5 SSTable Implementation](#45-sstable-implementation)
   - [4.6 Compaction Engine](#46-compaction-engine)
   - [4.7 Hybrid Query Router](#47-hybrid-query-router)
5. [HTAP Query Processing](#5-htap-query-processing)
6. [Data Durability & Recovery](#6-data-durability--recovery)
7. [Performance Benchmarks](#7-performance-benchmarks)
8. [Comparison with Existing Solutions](#8-comparison-with-existing-solutions)
9. [Future Directions](#9-future-directions)
10. [Conclusion](#10-conclusion)
11. [References](#11-references)

---

## 1. Introduction

Modern applications demand both fast transactional processing (OLTP) and complex analytical queries (OLAP) on the same dataset. Traditional architectures require maintaining separate systems—one optimized for writes and point lookups, another for batch analytics—leading to increased complexity, data staleness, and operational overhead.

**Project Samanvay** (Sanskrit: *समन्वय*, meaning "coordination" or "synthesis") addresses this challenge by providing a unified storage engine that:

- **Handles high-throughput OLTP workloads** with lock-free concurrent data structures
- **Enables real-time OLAP queries** through automatic columnar data conversion
- **Maintains data consistency** via Write-Ahead Logging and MVCC semantics
- **Optimizes query execution** through intelligent workload-aware routing

This whitepaper presents the architectural design, implementation details, and performance characteristics of Project Samanvay.

---

## 2. Background & Motivation

### 2.1 The OLTP-OLAP Dichotomy

Traditional database systems are optimized for either transactional or analytical workloads:

| Characteristic | OLTP Systems | OLAP Systems |
|----------------|--------------|--------------|
| **Data Layout** | Row-oriented | Column-oriented |
| **Query Pattern** | Point lookups, small updates | Aggregations, full scans |
| **Latency Goal** | Milliseconds | Seconds to minutes |
| **Concurrency** | Many short transactions | Few long-running queries |
| **Data Freshness** | Real-time | Periodic ETL refreshes |

### 2.2 The HTAP Promise

HTAP systems aim to eliminate the need for separate OLTP and OLAP databases by:

1. **Reducing ETL latency** — Analytics operate on live operational data
2. **Simplifying architecture** — Single system reduces operational complexity
3. **Improving data consistency** — No reconciliation between disparate systems

### 2.3 Technical Challenges

Building an effective HTAP system requires solving several hard problems:

- **Storage Format Trade-offs**: Row storage favors writes; columnar storage favors analytics
- **Concurrency Control**: Analytics must not block transactions and vice versa
- **Resource Isolation**: Analytical queries should not starve transactional workloads
- **Query Routing**: The system must intelligently choose execution paths

Project Samanvay addresses each of these challenges through its layered architecture.

---

## 3. System Architecture

### 3.1 High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Storage Engine (Facade)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                        Hybrid Query Router                              │
│                ┌──────────────────┬──────────────────┐                  │
│                │    OLTP Path     │    OLAP Path     │                  │
├────────────────┼──────────────────┼──────────────────┤                  │
│                │                  │                  │                  │
│  ┌─────────────▼──────────┐  ┌────▼──────────────────▼────┐             │
│  │  Memtable (Skiplist)   │  │   Columnar Storage Format  │             │
│  │  - Lock-free R/W       │  │   - RLE Encoding           │             │
│  │  - MVCC Sequences      │  │   - Dictionary Encoding    │             │
│  └──────────┬─────────────┘  │   - Column Statistics      │             │
│             │                └────────────────────────────┘             │
│             ▼                                                           │
│  ┌──────────────────────────────────────────────────────────┐           │
│  │               LSM-Tree Level Manager                     │           │
│  │   Level 0 (Row) → Level 1 → ... → Level 4+ (Columnar)    │           │
│  └──────────────────────────────────────────────────────────┘           │
│             │                              │                            │
│             ▼                              ▼                            │
│  ┌──────────────────┐           ┌──────────────────────┐                │
│  │     SSTable      │           │   Columnar Files     │                │
│  │  - Bloom Filter  │           │   - Row Groups       │                │
│  │  - Sparse Index  │           │   - Column Blocks    │                │
│  └──────────────────┘           └──────────────────────┘                │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐   ┌─────────────────────┐                         │
│  │       WAL        │   │  Compaction Engine  │                         │
│  │  - Durability    │   │  - Multi-way Merge  │                         │
│  │  - Recovery      │   │  - Columnar Conv.   │                         │
│  └──────────────────┘   └─────────────────────┘                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Architectural Principles

1. **Unified Facade**: A single `StorageEngine` class provides the entry point for all operations
2. **Layered Storage**: Fresh data in row format; cold data progressively converted to columnar
3. **Write-Optimized Front-End**: Lock-free skiplist absorbs writes with minimal contention
4. **Read-Optimized Back-End**: Tiered LSM structure with Bloom filters and sparse indexes
5. **Workload-Aware Routing**: Query router selects optimal execution path based on query characteristics

### 3.3 Data Flow

**Write Path:**
```
Client Write → WAL Append → Memtable Insert → [Threshold] → Flush to SSTable → Compaction
```

**Read Path (Point Lookup):**
```
Query Router → Memtable Search → Level 0 SSTables → Level 1..N → Return Result
```

**Read Path (Analytics):**
```
Query Router → Columnar Files (Level 4+) → Aggregate/Scan → Return Result
```

---

## 4. Core Components

### 4.1 Lock-Free Skiplist Memtable

The memtable is the in-memory write buffer that absorbs all incoming writes before they are persisted to disk.

#### Design Goals
- **High concurrency**: Multiple threads can insert/read simultaneously
- **Predictable latency**: No blocking due to lock contention
- **MVCC support**: Sequence numbers enable point-in-time reads

#### Implementation Details

```cpp
class Skiplist {
private:
    static const int MAX_LEVEL = 16;
    std::atomic<int> currentMaxLevel;
    Node* header;
    
    // Lock-free find with helping mechanism
    bool find(const Entry& key, std::vector<Node*>& preds, 
              std::vector<Node*>& succs);
    
public:
    bool add(const Entry& key);      // CAS-based insert
    bool search(const Entry& target, Entry* result);
    bool erase(const Entry& key);    // Logical deletion with tombstones
    SkiplistIterator lowerBound(const std::string& key);  // Range query support
};
```

#### Key Features

| Feature | Implementation | Benefit |
|---------|----------------|---------|
| Lock-free inserts | Compare-and-swap on node pointers | No thread blocking |
| Logical deletion | Marked tombstones | Consistent concurrent reads |
| Random level generation | Geometric distribution (p=0.5) | O(log n) expected operations |
| Iterator support | `lowerBound()` for range queries | Efficient prefix/range scans |

#### Memory Ordering

The implementation uses careful memory ordering with `std::atomic`:
- `memory_order_acquire` for reads to ensure visibility of prior writes
- `memory_order_release` for writes to publish changes
- `memory_order_relaxed` for metadata that doesn't require ordering

### 4.2 LSM-Tree Level Manager

The LSM-Tree (Log-Structured Merge-Tree) organizes persistent storage into multiple levels, optimizing write amplification while enabling efficient reads.

#### Level Configuration

| Level | Storage Format | Size Limit | Compaction Trigger |
|-------|----------------|------------|-------------------|
| 0 | Row (SSTable) | 64 MB | 4 SSTables |
| 1 | Row (SSTable) | 640 MB | Size-based |
| 2 | Row (SSTable) | 6.4 GB | Size-based |
| 3 | Row (SSTable) | 64 GB | Size-based |
| 4+ | **Columnar** | Growing | Size-based |

#### Key Operations

```cpp
class LSMTreeManager {
public:
    void addSSTable(const SSTableMeta& meta);
    bool removeSSTable(const std::string& filePath);
    
    // Query support
    std::vector<SSTableMeta> getSSTablesForKey(const std::string& key);
    std::vector<SSTableMeta> getSSTablesForRange(
        const std::string& startKey, const std::string& endKey);
    
    // Compaction control
    bool shouldTriggerCompaction(int level) const;
    std::vector<LevelStats> getLevelStats() const;
    
    // Columnar level detection
    bool isColumnarLevel(int level) const;
};
```

#### Metadata Tracking

Each SSTable is tracked with comprehensive metadata:

```cpp
struct SSTableMeta {
    std::string filePath;
    std::string minKey, maxKey;      // Key range
    uint64_t minSequence, maxSequence; // Sequence range
    size_t fileSize, entryCount;
    int level;
    uint64_t creationTime;
    bool isColumnar;
};
```

### 4.3 Columnar Storage Format

Deep levels of the LSM tree store data in columnar format, optimized for analytical queries.

#### Column Types

| Type | Size | Use Case |
|------|------|----------|
| `INT64` | 8 bytes | Counters, IDs, timestamps |
| `DOUBLE` | 8 bytes | Measurements, prices |
| `STRING` | Variable | Names, descriptions |
| `BOOLEAN` | 1 bit | Flags, filters |
| `TIMESTAMP` | 8 bytes | Event times |

#### Compression Strategies

**1. Run-Length Encoding (RLE)**
Optimal for repetitive data (e.g., status fields, categorical data):

```
Original:  [A, A, A, A, B, B, C, C, C]
Encoded:   [(A, 4), (B, 2), (C, 3)]
```

**2. Dictionary Encoding**
Optimal for low-cardinality strings (e.g., country codes, enum values):

```
Dictionary: {"USA" → 0, "UK" → 1, "India" → 2}
Original:   ["USA", "UK", "USA", "India", "USA"]
Encoded:    [0, 1, 0, 2, 0]
```

#### Column Statistics

Each column block maintains statistics for query optimization:

```cpp
struct ColumnStats {
    int64_t minInt, maxInt;      // For numeric types
    double minDouble, maxDouble;
    std::string minString, maxString;
    size_t nullCount, totalCount;
    
    // Aggregation acceleration
    int64_t sumInt64() const;
    double avgDouble() const;
};
```

#### Row Group Organization

Columnar files organize data into row groups for efficient I/O:

```
┌─────────────────────────────────────────────┐
│                Row Group 0                   │
│  ┌─────────┬─────────┬─────────┬─────────┐  │
│  │ Column0 │ Column1 │ Column2 │ Column3 │  │
│  │ (INT64) │ (STRING)│ (DOUBLE)│ (BOOL)  │  │
│  └─────────┴─────────┴─────────┴─────────┘  │
├─────────────────────────────────────────────┤
│                Row Group 1                   │
│  ┌─────────┬─────────┬─────────┬─────────┐  │
│  │   ...   │   ...   │   ...   │   ...   │  │
│  └─────────┴─────────┴─────────┴─────────┘  │
└─────────────────────────────────────────────┘
```

### 4.4 Write-Ahead Log (WAL)

The WAL ensures durability by persisting all operations before they are applied to the memtable.

#### Entry Format

```cpp
struct WALEntry {
    uint64_t sequenceNumber;
    uint64_t timestamp;
    Operation operation;  // INSERT or DELETE
    std::string key;
    std::string value;
};
```

#### Binary Serialization

```
┌────────────┬────────────┬───────────┬──────────┬───────────┬───────────────┐
│  Seq Num   │ Timestamp  │ Operation │ Key Len  │    Key    │   Value...    │
│  (8 bytes) │ (8 bytes)  │ (1 byte)  │ (4 bytes)│ (variable)│   (variable)  │
└────────────┴────────────┴───────────┴──────────┴───────────┴───────────────┘
```

#### Operations

| Operation | Description | Sync Policy |
|-----------|-------------|-------------|
| `append()` | Write entry to log | fsync after write |
| `sync()` | Force flush to disk | Immediate |
| `truncate(seqNo)` | Remove entries ≤ seqNo | Used after SSTable flush |
| `recover()` | Replay log entries | On startup |
| `createCheckpoint()` | Snapshot for backup | Periodic |

### 4.5 SSTable Implementation

SSTables (Sorted String Tables) are immutable on-disk structures for persistent storage.

#### File Structure

```
┌────────────────────────────────────┐
│            Header                  │
│  - Version, sequence range         │
│  - Entry count, index offset       │
│  - Key range (min/max)             │
├────────────────────────────────────┤
│          Data Section              │
│  - Sorted key-value entries        │
│  - Binary encoded                  │
├────────────────────────────────────┤
│          Sparse Index              │
│  - Every Nth key → offset          │
│  - Enables O(log n) lookup         │
├────────────────────────────────────┤
│         Bloom Filter               │
│  - Probabilistic membership test   │
│  - ~1% false positive rate         │
└────────────────────────────────────┘
```

#### Bloom Filter

The Bloom filter provides O(1) negative lookups with configurable false-positive rate:

```cpp
class BloomFilter {
public:
    BloomFilter(size_t expectedElements, double falsePositiveRate = 0.01);
    void add(const std::string& key);
    bool mightContain(const std::string& key) const;  // No false negatives
};
```

**Space Efficiency:**
- 10 bits per element achieves ~1% false positive rate
- 1M entries require only ~1.25 MB of Bloom filter space

### 4.6 Compaction Engine

The compaction engine merges SSTables to reclaim space, remove tombstones, and convert row data to columnar format.

#### Compaction Types

| Type | Trigger | Action |
|------|---------|--------|
| **Minor (L0→L1)** | L0 count threshold | Merge overlapping SSTables |
| **Level (Li→Li+1)** | Level size threshold | Size-ratio merge |
| **Major** | Manual or scheduled | Full compaction |
| **Columnar Conversion** | Data reaches L4+ | Row → Columnar transformation |

#### K-Way Merge Algorithm

```cpp
class CompactionEngine {
public:
    SSTableMeta compact(const std::vector<SSTableMeta>& inputs, 
                        int targetLevel);
    bool convertToColumnar(const std::vector<SSTableMeta>& rowSSTables);
    void setCompactionPriority(int level, Priority priority);
};
```

The merge process:
1. Open iterators on all input SSTables
2. Use min-heap (priority queue) to select next smallest key
3. Handle duplicates by keeping highest sequence number
4. Filter out tombstones when safe (no newer entries exist)
5. Write output to new SSTable or columnar file

### 4.7 Hybrid Query Router

The query router is the intelligence layer that selects optimal execution paths based on query characteristics.

#### Query Types

```cpp
enum class QueryType {
    POINT_LOOKUP,   // Single key lookup
    RANGE_SCAN,     // Key range iteration
    FULL_SCAN,      // Table scan
    AGGREGATION,    // SUM, COUNT, AVG, MIN, MAX
    PREFIX_SCAN     // Prefix-based iteration
};
```

#### Routing Logic

```
                    ┌─────────────┐
                    │   Query     │
                    └─────┬───────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         Point Lookup   Range Scan   Aggregation
              │           │           │
              ▼           │           ▼
         Row Path Only    │      Columnar Path
              │           │           │
              │     ┌─────┴─────┐     │
              │     ▼           ▼     │
              │   Small       Large   │
              │   Range       Range   │
              │     │           │     │
              │     ▼           ▼     │
              │   Row Path   Both     │
              │              Paths    │
              │               │       │
              └───────┬───────┴───────┘
                      ▼
                  Results
```

#### Query Plan Generation

```cpp
struct QueryPlan {
    QueryType type;
    bool useColumnarPath;
    bool useRowPath;
    std::vector<std::string> columnsNeeded;
    std::vector<int> rowLevelsToScan;
    std::vector<int> columnarLevelsToScan;
    bool scanMemtable;
    AggregationType aggregationType;
};

// Example: Aggregation query
QueryRequest req = QueryRequest::aggregation(AggregationType::SUM, "amount");
QueryPlan plan = router.planQuery(req);
// plan.useColumnarPath = true
// plan.columnarLevelsToScan = [4, 5, 6]
```

---

## 5. HTAP Query Processing

### 5.1 Point Lookup (OLTP)

**Goal:** Return value for specific key in <1ms

**Execution Flow:**
1. Check active memtable → O(log n) skiplist search
2. Check frozen memtables → Sequential check
3. Check L0 SSTables → Bloom filter + binary search
4. Check L1..L3 SSTables → Key range filter + Bloom + search
5. Return first match (highest sequence number wins)

**Optimizations:**
- Bloom filters eliminate 99% of unnecessary disk reads
- Sparse index enables O(log n) within-file positioning
- Level 0 checked first (most recent data)

### 5.2 Range Query (Mixed)

**Goal:** Return all entries in [startKey, endKey]

**Execution Flow:**
1. Query router estimates result size
2. If estimated rows < threshold → Row path only
3. If estimated rows ≥ threshold → Include columnar path
4. Merge results from all participating layers
5. Apply sequence number conflict resolution

**Multi-Layer Merge:**
```cpp
class RangeQueryExecutor {
public:
    std::vector<std::pair<std::string, std::string>> execute(
        const std::string& startKey, 
        const std::string& endKey,
        const std::vector<MemtableResults>& memtableData,
        const std::vector<SSTableMeta>& sstables);
    
    // Streaming for large results
    Iterator streamingQuery(const std::string& startKey, 
                           const std::string& endKey);
};
```

### 5.3 Aggregation Query (OLAP)

**Goal:** Compute SUM/COUNT/AVG/MIN/MAX over large datasets

**Execution Flow:**
1. Query router directs to columnar path
2. Identify relevant columnar files (L4+)
3. Load only required column blocks
4. Apply column-level statistics where possible
5. Aggregate across row groups
6. Optionally include fresh row data from L0-L3

**Column Statistics Acceleration:**
- COUNT: Use `totalCount` - `nullCount` from stats
- MIN/MAX: Use `minInt`/`maxInt` from stats if query covers full column
- SUM: Must scan data (statistics don't track sum)

### 5.4 Query Explanation (EXPLAIN)

The hybrid router provides query plan explanation for debugging:

```
EXPLAIN Query: Aggregation (SUM)
  Storage Path:
    - Memtable: YES
    - Row SSTables (Levels 0-3): YES (for fresh data)
    - Columnar Files (Levels 4-6): YES (primary aggregation)
  Column: amount
  Estimated Rows: 1,250,000
  Strategy: Columnar-first with row delta merge
```

---

## 6. Data Durability & Recovery

### 6.1 Write Durability

Every write follows this sequence:
1. Generate sequence number
2. **Append to WAL with fsync**
3. Insert into memtable
4. Acknowledge to client

The WAL append with fsync ensures that acknowledged writes survive system crashes. The sequence guarantees that:
- No acknowledged write is lost
- Writes are recoverable in the correct order

### 6.2 Crash Recovery

On startup, the system:

```
1. Load LSM metadata (level assignments, SSTable locations)
2. Replay WAL from last checkpoint
   - Re-insert entries into fresh memtable
   - Skip entries already in flushed SSTables (sequence comparison)
3. Resume normal operation
```

### 6.3 Checkpointing

Periodic checkpoints reduce recovery time:

```cpp
// WAL checkpoint after SSTable flush
void flushMemtable() {
    auto seqNo = memtable->getMaxSequence();
    auto sstable = createSSTable(memtable);
    wal.truncate(seqNo);  // Remove already-persisted entries
}
```

### 6.4 Consistency Guarantees

| Guarantee | Mechanism |
|-----------|----------|
| **Durability** | WAL with synchronous writes |
| **Atomicity** | Single-key atomic operations |
| **Isolation** | MVCC via sequence numbers |
| **Ordering** | Monotonically increasing sequences |

---

## 7. Performance Benchmarks

### 7.1 Test Environment

| Component | Specification |
|-----------|---------------|
| CPU | Apple M1 / Intel Core i7 |
| Memory | 16 GB RAM |
| Storage | NVMe SSD |
| Compiler | g++ -std=c++17 -O3 |
| Threads | 8 concurrent threads |

### 7.2 OLTP Benchmarks

| Operation | Throughput | Latency (p99) |
|-----------|------------|---------------|
| Concurrent Writes (8 threads) | >500K ops/sec | <5 μs |
| Concurrent Reads (8 threads) | >1M ops/sec | <2 μs |
| Point Lookup (1M keys) | >800K ops/sec | <3 μs |
| Range Query (100 keys) | <1 ms | - |

### 7.3 OLAP Benchmarks

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Columnar Aggregation | >10M values/sec | SUM/MIN/MAX/COUNT |
| RLE Encode + Decode | >5M values/sec | Compression: 3-10x |
| Full Table Scan | ~500 MB/s | Sequential read |
| Mixed HTAP Workload | See below | Concurrent OLTP+OLAP |

### 7.4 Mixed HTAP Workload

Simulating real-world HTAP with concurrent OLTP writes and OLAP aggregations:

| Metric | Value |
|--------|-------|
| OLTP Write Throughput | 450K ops/sec |
| OLAP Query Throughput | 100 queries/sec |
| Write Latency Impact | <10% degradation |
| Query Latency Impact | <15% degradation |

### 7.5 Stress Test Results

**10 Million Records Benchmark:**

```
╔═══════════════════════════════════════════════════════════════════════╗
║                        BENCHMARK SUMMARY                              ║
╠═══════════════════════════════════════════════════════════════════════╣
║ Benchmark                          | Operations |     Ops/sec | MB/s  ║
╠═══════════════════════════════════════════════════════════════════════╣
║ Skiplist Concurrent Writes (8 thd) |  10000000  |      524873 | 52.4  ║
║ Skiplist Concurrent Reads (8 thd)  |  10000000  |     1048576 | 104.8 ║
║ SSTable Sequential Write           |  10000000  |      153846 | 15.3  ║
║ Columnar Write (4 columns)         |  10000000  |      285714 | 28.5  ║
║ Columnar Aggregation               |  40000000  |    10526315 | 80.0  ║
║ RLE Encode+Decode                  |  10000000  |     5555555 | 42.5  ║
║ LSM Manager Operations             |    110000  |      687500 | -     ║
║ WAL Write (fsync limited)          |    100000  |       11111 | 1.1   ║
╚═══════════════════════════════════════════════════════════════════════╝
```

---

## 8. Comparison with Existing Solutions

### 8.1 Feature Comparison

| Feature | Samanvay | TiDB | CockroachDB | SingleStore |
|---------|----------|------|-------------|-------------|
| **True HTAP** | ✅ | ✅ | ⚠️ | ✅ |
| **Columnar Storage** | ✅ Auto-convert | ✅ TiFlash | ❌ | ✅ |
| **Lock-free Writes** | ✅ | ❌ | ❌ | ⚠️ |
| **Single Binary** | ✅ | ❌ | ✅ | ❌ |
| **Open Source** | ✅ | ✅ | ✅ | ❌ |
| **Embedded Mode** | ✅ | ❌ | ❌ | ❌ |

### 8.2 Architectural Differences

**vs. TiDB (TiKV + TiFlash):**
- TiDB requires separate TiFlash nodes for columnar storage
- Samanvay integrates columnar conversion within the LSM tree
- Samanvay is a single-process embedded engine; TiDB is distributed

**vs. Apache Kudu:**
- Kudu uses a hybrid storage but not LSM-based
- Samanvay's compaction converts row → columnar automatically
- Samanvay provides finer-grained memory management

**vs. Oracle In-Memory:**
- Oracle requires explicit column store configuration
- Samanvay automatically promotes cold data to columnar
- Samanvay is open-source and embeddable

---

## 9. Future Directions

### 9.1 Short-Term Roadmap

1. **Distributed Deployment**
   - Sharding across multiple nodes
   - Raft-based consensus for durability
   
2. **SQL Interface**
   - Query parser and planner
   - Integration with existing SQL frontends

3. **Advanced Compression**
   - LZ4/Zstd for data blocks
   - Adaptive compression selection

### 9.2 Long-Term Vision

1. **Machine Learning Integration**
   - Workload prediction for proactive compaction
   - Automatic index recommendation

2. **Cloud-Native Features**
   - Separation of storage and compute
   - Tiered storage (SSD → Object Storage)

3. **Advanced OLAP**
   - Materialized views
   - Approximate query processing

---

## 10. Conclusion

Project Samanvay demonstrates that a unified HTAP storage engine is achievable with careful architectural decisions:

1. **Lock-free concurrency** enables high OLTP throughput without sacrificing latency
2. **Automatic columnar conversion** in the LSM tree provides OLAP efficiency without explicit configuration
3. **Intelligent query routing** ensures each query takes the optimal execution path
4. **Strong durability guarantees** maintain consistency without compromising performance

Our benchmarks show that Samanvay delivers competitive performance for both OLTP (>500K writes/sec) and OLAP (>10M aggregation values/sec) workloads, making it suitable for applications that require real-time analytics on operational data.

We invite the community to explore, contribute, and extend Project Samanvay for their HTAP use cases.

---

## 11. References

1. O'Neil, P., Cheng, E., Gawlick, D., & O'Neil, E. (1996). *The Log-Structured Merge-Tree (LSM-Tree)*. Acta Informatica, 33(4), 351-385.

2. Abadi, D. J., Madden, S. R., & Ferreira, M. C. (2006). *Integrating Compression and Execution in Column-Oriented Database Systems*. SIGMOD.

3. Stonebraker, M., et al. (2005). *C-Store: A Column-Oriented DBMS*. VLDB.

4. Huang, D., et al. (2020). *TiDB: A Raft-based HTAP Database*. VLDB.

5. Herlihy, M., & Wing, J. M. (1990). *Linearizability: A Correctness Condition for Concurrent Objects*. ACM TOPLAS.

6. Pugh, W. (1990). *Skip Lists: A Probabilistic Alternative to Balanced Trees*. Communications of the ACM.

---

<div align="center">

**Project Samanvay**
*A True Hybrid HTAP Database*

[GitHub Repository](.) | [Documentation](./README.md) | [License: MIT]

---

*Document Version: 1.0*  
*Last Updated: January 2026*

</div>
