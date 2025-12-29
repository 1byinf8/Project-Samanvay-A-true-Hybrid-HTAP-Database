# Project Samanvay - HTAP Database Validation Guide

> Comprehensive testing and validation documentation for all components and architectures

---

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Component Categories](#component-categories)
- [Quick Start - Run All Tests](#quick-start---run-all-tests)
- [Component-wise Validation](#component-wise-validation)
  - [1. Skiplist Memtable](#1-skiplist-memtable)
  - [2. LSM-Tree Level Manager](#2-lsm-tree-level-manager)
  - [3. SSTable (Sorted String Table)](#3-sstable-sorted-string-table)
  - [4. Write-Ahead Log (WAL)](#4-write-ahead-log-wal)
  - [5. Compaction Engine](#5-compaction-engine)
  - [6. Columnar Storage](#6-columnar-storage)
  - [7. Range Query Executor](#7-range-query-executor)
  - [8. Hybrid Query Router](#8-hybrid-query-router)
  - [9. Storage Engine Facade](#9-storage-engine-facade)
  - [10. Memtable Manager](#10-memtable-manager)
- [Stress Test & Benchmark Suite](#stress-test--benchmark-suite)
- [Performance Targets](#performance-targets)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Storage Engine (Facade)                         │
├─────────────────────────────────────────────────────────────────────┤
│                    Hybrid Query Router                              │
│              ┌──────────────────┬──────────────────┐                │
│              │   OLTP Path      │   OLAP Path      │                │
├──────────────┼──────────────────┼──────────────────┤                │
│              │                  │                  │                │
│  ┌───────────▼──────────┐  ┌────▼──────────────────▼────┐           │
│  │ Memtable (Skiplist)  │  │  Columnar Storage Format   │           │
│  │   - Lock-free        │  │  - RLE Encoding            │           │
│  │   - Concurrent R/W   │  │  - Dictionary Encoding     │           │
│  └──────────┬───────────┘  │  - Column Statistics       │           │
│             │              └────────────────────────────┘           │
│             ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐        │
│  │              LSM-Tree Level Manager                     │        │
│  │  Level 0 (Row) → Level 1 → ... → Level 4+ (Columnar)    │        │
│  └─────────────────────────────────────────────────────────┘        │
│             │                              │                        │
│             ▼                              ▼                        │
│  ┌──────────────────┐           ┌──────────────────────┐            │
│  │    SSTable       │           │   Columnar Files     │            │
│  │  - Bloom Filter  │           │   - Row Groups       │            │
│  │  - Sparse Index  │           │   - Column Blocks    │            │
│  └──────────────────┘           └──────────────────────┘            │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐   ┌─────────────────────┐                     │
│  │       WAL        │   │  Compaction Engine  │                     │
│  │  - Durability    │   │  - Multi-way Merge  │                     │
│  │  - Recovery      │   │  - Columnar Conv    │                     │
│  └──────────────────┘   └─────────────────────┘                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component Categories

| Category | Components | Purpose |
|----------|------------|---------|
| **In-Memory** | Skiplist, Memtable Manager | Fast concurrent OLTP operations |
| **Persistent Row Store** | SSTable, WAL | Durability and row-oriented storage |
| **LSM Architecture** | LSM Levels, Compaction | Tiered storage with write optimization |
| **Columnar OLAP** | Columnar Format, Columnar File | Analytics and aggregation workloads |
| **Query Layer** | Range Query Executor, Hybrid Router | Unified query interface |
| **Facade** | Storage Engine | Unified API for all operations |

---

## Quick Start - Run All Tests

### Compile All Tests

```bash
cd /Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine

# Define the thread_local static for skiplist in test files that need it
# (Already included in stress_test_benchmark.cpp)

# 1. Compile Skiplist Unit Tests (requires thread_local fix)
# Note: Add -DSKIPLIST_THREAD_LOCAL_DEF or add this line before main():
#   thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());
g++ -std=c++17 -O2 -pthread -o test_skiplist test/test_skiplist.cpp \
    -include test/stress_test_benchmark.cpp 2>/dev/null || \
    echo "Note: test_skiplist requires thread_local definition - see Troubleshooting section"

# 2. Compile Skiplist Stress Tests (1M operations)
# Same thread_local requirement as above
g++ -std=c++17 -O2 -pthread -o stress_skiplist test/stress_skiplist.cpp \
    -include <(echo "thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());") 2>/dev/null || true

# 3. Compile HTAP Features Test Suite
g++ -std=c++17 -O2 -o test_htap test/test_htap_features.cpp

# 4. Compile Memtable Test Suite (10M operations)
g++ -std=c++17 -O2 -pthread -o test_memtable test/test_almost.cpp

# 5. Compile Read-Heavy Benchmark
g++ -std=c++17 -O2 -pthread -o simple_bench test/simple.cpp

# 6. Compile Full Stress Test Benchmark
g++ -std=c++17 -O3 -pthread -o stress_test test/stress_test_benchmark.cpp
```

### Run All Tests (Quick Validation)

```bash
# Run all compiled tests
./test_skiplist         # ~2-5 seconds
./test_htap             # ~1-2 seconds
./stress_skiplist       # ~30-60 seconds (1M operations)
./stress_test 100000 4  # Quick benchmark with 100K records, 4 threads
```

### Run Full Benchmark Suite

```bash
# Full stress test (1M records, 8 threads)
./stress_test 1000000 8

# Extended stress test (10M records)
./stress_test 10000000 16

# Memtable stress test (10M operations)
./test_memtable

# Read-heavy benchmark (50M reads on 10M preloaded keys)
./simple_bench
```

---

## Component-wise Validation

### 1. Skiplist Memtable

**Source:** [skiplist.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/skiplist.hpp)

**Description:** Lock-free concurrent skiplist serving as the in-memory memtable. Supports atomic inserts, lookups, deletes, and range queries.

**Key Features:**
- Lock-free concurrent operations using `std::atomic`
- Marked deletion with logical tombstones
- Iterator support with `lowerBound()` for range queries
- Sequence number tracking for MVCC

#### Unit Tests

```bash
# Compile
g++ -std=c++17 -O2 -pthread -o test_skiplist test/test_skiplist.cpp

# Run
./test_skiplist
```

**Test Coverage:**
| Test Name | Description |
|-----------|-------------|
| `testInsertAndSearch` | Basic CRUD operations |
| `testDelete` | Deletion and tombstone handling |
| `testDuplicates` | Duplicate key handling |
| `testSequentialInsert/Delete` | Ordered operations |
| `testTombstoneHandling` | Tombstone lifecycle |
| `testSequenceNumber` | MVCC sequence tracking |
| `testConcurrentInserts` | Multi-threaded inserts (8 threads) |
| `testConcurrentMixed` | Mixed insert/read/delete |
| `testHighContentionScenario` | High contention on small key set |
| `testMemoryConsistency` | Memory ordering correctness |
| `testRaceConditionDetection` | Concurrent insert to same key |
| `testLargeScaleOperations` | 10K sequential operations |
| `testHTAPMixedWorkload` | HTAP-style mixed workload |

#### Stress Tests (1M Operations)

```bash
# Compile
g++ -std=c++17 -O2 -pthread -o stress_skiplist test/stress_skiplist.cpp

# Run
./stress_skiplist
```

**Test Coverage:**
| Test Name | Operations | Description |
|-----------|------------|-------------|
| `testConcurrentMillionOperations` | 1M | Sequential phases: Insert → Search → Delete |
| `testConcurrentMixedMillionOperations` | 1M | Random mixed operations |

---

### 2. LSM-Tree Level Manager

**Source:** [lsm_levels.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/lsm_levels.hpp)

**Description:** Manages the tiered LSM-tree structure with configurable levels. Handles SSTable metadata, compaction triggers, and columnar level transitions.

**Key Features:**
- 7-level default configuration
- Level 0-3: Row-oriented SSTables
- Level 4+: Columnar storage
- Compaction trigger thresholds
- SSTable metadata tracking

#### Validation (via HTAP Test Suite)

```bash
# Compile
g++ -std=c++17 -O2 -o test_htap test/test_htap_features.cpp

# Run
./test_htap
```

**Test Coverage (in `testLSMTreeManager`):**
| Test | Validates |
|------|-----------|
| Add SSTable to Level 0 | `addSSTable()` |
| Get SSTables for key | `getSSTablesForKey()` |
| Key outside range | Empty result handling |
| Range query overlap | `getSSTablesForRange()` |
| Compaction trigger | `shouldTriggerCompaction()` |
| Level stats | `getLevelStats()` |
| Columnar level check | `isColumnarLevel()` |
| Remove SSTable | `removeSSTable()` |

#### Benchmark

```bash
# LSM Manager operations benchmark
./stress_test 1000000 8
# Look for "[7/8] LSM Manager Operations..." output
```

---

### 3. SSTable (Sorted String Table)

**Source:** [sstable.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/sstable.hpp)

**Description:** Immutable on-disk sorted key-value storage with Bloom filters and sparse indexes.

**Key Features:**
- Bloom filter for fast negative lookups
- Sparse index for efficient seeks
- Range query support
- Binary serialization format

#### Validation (via HTAP Test Suite)

```bash
./test_htap
```

**Test Coverage (in `testSSTableRangeQuery`):**
| Test | Validates |
|------|-----------|
| Create SSTable | Write 100 sorted entries |
| Load metadata | `load()` SSTable from disk |
| Range query | `rangeQuery("key_010", "key_020")` returns 11 entries |
| Result ordering | Verify sorted results |
| Point lookup | `get("key_050")` |
| Point lookup miss | `get("key_999")` returns empty |

#### Benchmark

```bash
./stress_test 1000000 8
# Look for "[3/8] SSTable Sequential Write..." output
```

---

### 4. Write-Ahead Log (WAL)

**Source:** [wal.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/wal.hpp)

**Description:** Durable write-ahead log for crash recovery. Supports append, sync, truncation, and recovery.

**Key Features:**
- Binary serialization format
- Fsync durability
- Truncation for space reclamation
- Full recovery on restart

#### Validation (via HTAP Test Suite)

```bash
./test_htap
```

**Test Coverage (in `testWAL`):**
| Test | Validates |
|------|-----------|
| WAL append | 10 INSERT operations |
| WAL recovery count | Recover 10 entries |
| WAL recovery order | First entry seq=0 |
| WAL recovery content | Entry[5].key == "key_5" |
| WAL max sequence | `getMaxSequence()` == 9 |
| WAL truncation | Truncate up to seq=5, 4 entries remain |
| WAL truncation first | First remaining seq=6 |

#### Benchmark

```bash
./stress_test 100000 8
# Look for "[8/8] WAL Write (limited to 100000 for fsync)..." output
# Note: WAL is limited to 100K to avoid long fsync times
```

---

### 5. Compaction Engine

**Source:** [compaction.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/compaction.hpp)

**Description:** Multi-way merge engine for SSTable compaction with priority scheduling and columnar conversion.

**Key Features:**
- k-way merge sort via priority queue
- SSTable iterator abstraction
- Minor (level) and Major compaction
- Columnar conversion at deep levels
- Tombstone removal

#### Validation

Manual validation through HTAP test suite (implicitly tested via LSM manager compaction triggers).

---

### 6. Columnar Storage

**Sources:**
- [columnar_format.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/columnar_format.hpp) - In-memory format
- [columnar_file.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/columnar_file.hpp) - On-disk format

**Description:** Columnar storage for OLAP workloads with compression and statistics.

**Key Features:**
- Column types: INT64, DOUBLE, STRING, BOOLEAN, TIMESTAMP
- RLE encoding for repetitive data
- Dictionary encoding for low-cardinality strings
- Per-column statistics (min, max, count, null count)
- Row group organization

#### Validation (via HTAP Test Suite)

```bash
./test_htap
```

**Test Coverage (in `testColumnarFormat`):**
| Test | Validates |
|------|-----------|
| ColumnBlock row count | 100 int64 rows |
| ColumnBlock sum | `sumInt64()` == 49500 |
| ColumnBlock min | `minInt64()` == 0 |
| ColumnBlock max | `maxInt64()` == 990 |
| ColumnBlock count | `count()` == 100 |
| String column | 3 strings, verify index access |
| RLE encoding/decoding | Encode + decode repetitive data |
| RLE compression ratio | Verify size reduction |
| Dictionary encoding | 3 unique strings from 6 values |
| Dictionary size | 3 unique entries |
| TableSchema column count | 3 columns |
| TableSchema column lookup | `getColumnIndex("amount")` == 2 |
| TableSchema missing | `getColumnIndex("missing")` == -1 |

**Test Coverage (in `testColumnarFile`):**
| Test | Validates |
|------|-----------|
| Create columnar file | Write with 100 rows/group |
| Add 250 rows | Multiple row groups |
| File written | Verify file exists |
| Open columnar file | `ColumnarFile::open()` |
| Row group count | ≥2 row groups for 250 rows |

#### Benchmark

```bash
./stress_test 1000000 8
# Look for:
# "[4/8] Columnar Write..."
# "[5/8] Columnar Aggregation..."
# "[6/8] RLE Compression..."
```

---

### 7. Range Query Executor

**Source:** [range_query_executor.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/range_query_executor.hpp)

**Description:** Multi-layer range query execution across memtable and all SSTable levels.

**Key Features:**
- Merge results from all layers
- Tombstone filtering
- Sequence number conflict resolution
- Streaming iterator for large results
- Batch loading optimization

#### Validation

Validated through SSTable range query tests and storage engine integration.

---

### 8. Hybrid Query Router

**Source:** [hybrid_query_router.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/hybrid_query_router.hpp)

**Description:** Intelligent query routing between row-store (OLTP) and columnar (OLAP) paths.

**Key Features:**
- Point lookup → Row path
- Aggregations → Columnar path
- Full scans → Both paths
- Row count estimation for routing decisions
- Query plan explanation (EXPLAIN)

#### Validation (via HTAP Test Suite)

```bash
./test_htap
```

**Test Coverage (in `testHybridQueryRouter`):**
| Test | Validates |
|------|-----------|
| Point lookup uses row store | `useRowPath && !useColumnarPath` |
| Point lookup scans memtable | `scanMemtable == true` |
| Aggregation prefers columnar | `useColumnarPath == true` |
| Aggregation columnar levels | Non-empty columnar levels |
| Full scan uses both paths | `useRowPath && useColumnarPath` |
| Explain plan works | Non-empty explanation string |

---

### 9. Storage Engine Facade

**Source:** [storage_engine.hpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/includes/storage_engine.hpp)

**Description:** Unified facade providing a single API for all storage operations.

**Key Features:**
- Unified read/write interface
- SSTable registration
- Compaction control (trigger, pause, resume)
- Statistics and status monitoring
- Recovery support

#### Validation (via HTAP Test Suite)

```bash
./test_htap
```

**Test Coverage (in `testStorageEngine`):**
| Test | Validates |
|------|-----------|
| Get stats | 7 level stats returned |
| Explain query | Non-empty query plan |
| Print status | Console output |

---

### 10. Memtable Manager

**Source:** [memTable.cpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/src/memTable.cpp)

**Description:** Manages active and frozen memtables with automatic flushing.

**Key Features:**
- Configurable memtable size limit
- Automatic freezing when size threshold reached
- Background flush to SSTable
- Multi-memtable search (active + frozen)
- Range query across all memtables

#### Validation (Full Test Suite)

```bash
# Compile
g++ -std=c++17 -O2 -pthread -o test_memtable test/test_almost.cpp

# Run
./test_memtable
```

**Test Coverage:**
| Test | Description | Operations |
|------|-------------|------------|
| `testBasicOperations` | Insert, Search, Delete, Reinsert | ~10 |
| `testTombstoneHandling` | Tombstone creation and override | ~8 |
| `testConcurrentInserts` | 8 threads × 10K ops | 80K |
| `testConcurrentReads` | 8 threads × 10K reads | 80K |
| `testConcurrentMixedOperations` | 8 threads mixed ops | 80K |
| `testRangeQueries` | Insert 1K, query range | 1K + query |
| `testMemTableFlushing` | Trigger multiple flushes | 1K × 1KB |
| `stressTest` | 16 threads stress test | **10M** |

#### Read-Heavy Benchmark

```bash
# Compile
g++ -std=c++17 -O2 -pthread -o simple_bench test/simple.cpp

# Run (preloads 10M keys, runs 50M random lookups)
./simple_bench
```

---

## Stress Test & Benchmark Suite

### Full Benchmark Suite

**Source:** [stress_test_benchmark.cpp](file:///Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine/test/stress_test_benchmark.cpp)

```bash
# Compile with maximum optimization
g++ -std=c++17 -O3 -pthread -o stress_test test/stress_test_benchmark.cpp

# Run with default parameters (1M records, 8 threads)
./stress_test

# Run with custom parameters
./stress_test [num_records] [num_threads]

# Examples:
./stress_test 100000 4      # Quick test: 100K records, 4 threads
./stress_test 1000000 8     # Standard test: 1M records, 8 threads
./stress_test 10000000 16   # Extended test: 10M records, 16 threads
```

### Benchmark Matrix

| Benchmark | Operations | Measures |
|-----------|------------|----------|
| Skiplist Concurrent Writes | N records | ops/sec, MB/s |
| Skiplist Concurrent Reads | N records | ops/sec |
| SSTable Sequential Write | N records | ops/sec, MB/s |
| Columnar Write (4 columns) | N records | ops/sec, MB/s |
| Columnar Aggregation | N × 100 × 4 | ops/sec, MB/s |
| RLE Encode+Decode | N values | ops/sec, compression ratio |
| LSM Manager | N/1000 + 100K queries | ops/sec |
| WAL Write | 100K (fsync limited) | ops/sec, MB/s |
| Mixed OLTP+OLAP | Variable | ops/sec breakdown |

### Expected Output

```
╔═══════════════════════════════════════════════════════════════════════════════════════════════╗
║                     HTAP Database Stress Test & Benchmark Suite                               ║
╠═══════════════════════════════════════════════════════════════════════════════════════════════╣
║  Records:      1000000   |   Threads:   8                                                     ║
╚═══════════════════════════════════════════════════════════════════════════════════════════════╝

[1/8] Skiplist Concurrent Writes...
[2/8] Skiplist Concurrent Reads...
...

╔═══════════════════════════════════════════════════════════════════════════════════════════════╗
║                                    BENCHMARK SUMMARY                                          ║
╠═══════════════════════════════════════════════════════════════════════════════════════════════╣
║ Benchmark                             |   Operations |   Time (ms) |      Ops/sec |     MB/s ║
╠═══════════════════════════════════════════════════════════════════════════════════════════════╣
║ Skiplist Concurrent Writes (8 thds)   |      1000000 |      xxx.xx |      xxxxxxx |    xx.xx ║
║ ...                                   |              |             |              |          ║
╚═══════════════════════════════════════════════════════════════════════════════════════════════╝

🏆 Highest Throughput: [component] (X ops/sec)
```

---

## Performance Targets

| Component | Operation | Target |
|-----------|-----------|--------|
| Skiplist | Concurrent writes (8 threads) | >500K ops/sec |
| Skiplist | Concurrent reads (8 threads) | >1M ops/sec |
| SSTable | Sequential write | >100K entries/sec |
| Columnar Aggregation | SUM/MIN/MAX/COUNT | >10M values/sec |
| LSM Manager | Key lookup | <100μs per query |
| WAL | Append with fsync | >10K ops/sec |
| Range Query | 100 keys | <1ms |

---

## Troubleshooting

### Compilation Errors

**Missing `thread_local` declaration:**
```cpp
// Add to the top of test file if not present:
thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());
```

**Linker errors with `std::filesystem`:**
```bash
# Add -lstdc++fs on older compilers
g++ -std=c++17 -O2 -pthread -lstdc++fs -o test_file test/test_file.cpp
```

### Runtime Issues

**Segmentation fault in concurrent tests:**
- Ensure adequate stack size for threads
- Check for iterator invalidation

**Test data files not cleaned up:**
```bash
rm -f *.sst *.col *.log test_data/* benchmark_*
```

### Performance Issues

**Slow WAL writes:**
- Normal with fsync per operation
- Benchmark limits to 100K operations

**Memory issues with large tests:**
- Reduce `numRecords` parameter
- Increase memtable size limit in test

---

## Summary of Test Commands

```bash
# Navigate to StorageEngine directory
cd /Users/1byinf8/Project-Samanvay-A-true-Hybrid-HTAP-Database/StorageEngine

# Compile all tests
g++ -std=c++17 -O2 -pthread -o test_skiplist test/test_skiplist.cpp
g++ -std=c++17 -O2 -pthread -o stress_skiplist test/stress_skiplist.cpp
g++ -std=c++17 -O2 -o test_htap test/test_htap_features.cpp
g++ -std=c++17 -O2 -pthread -o test_memtable test/test_almost.cpp
g++ -std=c++17 -O2 -pthread -o simple_bench test/simple.cpp
g++ -std=c++17 -O3 -pthread -o stress_test test/stress_test_benchmark.cpp

# Quick validation (~1 min)
./test_skiplist && ./test_htap

# Standard validation (~5 min)
./test_skiplist && ./test_htap && ./stress_skiplist && ./stress_test 100000 4

# Full validation (~15+ min)
./test_skiplist && ./test_htap && ./stress_skiplist && \
./test_memtable && ./stress_test 1000000 8

# Extended benchmark (~30+ min)
./stress_test 10000000 16
./simple_bench
```

---

*Document generated: 2025-12-29*
*Project Samanvay - A True Hybrid HTAP Database*
