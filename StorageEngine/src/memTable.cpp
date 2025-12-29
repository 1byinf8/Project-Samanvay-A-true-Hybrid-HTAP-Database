/**
 * @file memTable.cpp
 * @brief In-Memory Table (Memtable) Implementation for LSM-Tree Storage Engine
 * 
 * This file implements the memtable component of an LSM-tree (Log-Structured Merge-tree)
 * based storage engine for the Samanvay HTAP Database. The memtable serves as the 
 * write-optimized, in-memory buffer that temporarily stores recent writes before they
 * are flushed to persistent storage (SSTables) on disk.
 * 
 * ARCHITECTURE OVERVIEW:
 * =====================
 * The memtable system uses a multi-version concurrency control (MVCC) approach with
 * sequence numbers to support snapshot isolation. Each write operation is assigned a
 * monotonically increasing sequence number, enabling:
 * - Point-in-time reads
 * - Conflict-free concurrent reads and writes
 * - Proper ordering during compaction
 * 
 * KEY COMPONENTS:
 * ===============
 * 
 * 1. Memtable Class:
 *    - Single in-memory buffer backed by a lock-free skiplist data structure
 *    - Tracks its state (ACTIVE, FROZEN, or FLUSHED)
 *    - Maintains sequence number ranges for efficient queries
 *    - Provides O(log n) insert, search, and range query operations
 * 
 * 2. MemtableManager Class:
 *    - Manages multiple memtable instances
 *    - Coordinates write-ahead logging (WAL) for durability
 *    - Handles automatic memtable rotation when size threshold is reached
 *    - Orchestrates background flushing of frozen memtables to SSTables
 *    - Provides crash recovery through WAL replay
 * 
 * STATE MACHINE:
 * ==============
 * Each memtable transitions through three states:
 * 
 * ACTIVE -> FROZEN -> FLUSHED
 * 
 * - ACTIVE: Accepts new writes, size below threshold
 * - FROZEN: Read-only, queued for background flush to disk
 * - FLUSHED: Successfully persisted to SSTable, ready for cleanup
 * 
 * WRITE PATH:
 * ===========
 * 1. Write operation arrives (insert or delete)
 * 2. Entry logged to WAL for durability (write-ahead logging)
 * 3. Entry inserted into active memtable with sequence number
 * 4. If memtable size exceeds threshold:
 *    a. Mark current memtable as FROZEN
 *    b. Create new ACTIVE memtable
 *    c. Notify background flush thread
 * 5. Background thread flushes FROZEN memtable to SSTable
 * 6. Mark as FLUSHED and remove from memory
 * 
 * READ PATH:
 * ==========
 * 1. Search newest memtable first (most recent data)
 * 2. If not found, search older memtables in reverse chronological order
 * 3. Handle tombstones (deletion markers) appropriately
 * 4. If still not found, delegate to SSTable layer (TODO)
 * 
 * DURABILITY GUARANTEES:
 * ======================
 * - Write-Ahead Log (WAL): All writes are logged before being applied
 * - On crash, WAL can be replayed to reconstruct in-memory state
 * - Background flushing ensures bounded memory usage
 * - SSTables provide persistent storage with bloom filters and indexing
 * 
 * CONCURRENCY CONTROL:
 * ====================
 * - Lock-free skiplist for high concurrent read/write throughput
 * - Mutex protection for manager-level operations (rotation, flush coordination)
 * - Atomic operations for state transitions and size tracking
 * - MVCC with sequence numbers for snapshot isolation
 * 
 * PERFORMANCE CHARACTERISTICS:
 * ============================
 * - Insert: O(log n) amortized with skiplist
 * - Point lookup: O(log n) per memtable
 * - Range query: O(log n + k) where k is result size
 * - Memory overhead: Skiplist pointer structure + entry data
 * - Background flush minimizes write stalls
 */

#pragma once
#ifndef MEMTABLE_HPP
#define MEMTABLE_HPP

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <memory>
#include <optional>
#include <thread>
#include <condition_variable>
#include <chrono>
#include <iostream>
#include <map>
#include <algorithm>
#include "../includes/skiplist.hpp"
#include "../includes/sstable.hpp"
#include "../includes/wal.hpp"

namespace memTable {

/**
 * @enum State
 * @brief Lifecycle states for a memtable instance
 * 
 * ACTIVE:  Memtable is accepting new writes. Size is below the configured threshold.
 * FROZEN:  Memtable is read-only and queued for background flush. No new writes accepted.
 * FLUSHED: Memtable has been successfully persisted to an SSTable and can be garbage collected.
 */
enum class State {ACTIVE, FROZEN, FLUSHED};

/**
 * @class Memtable
 * @brief Single in-memory table backed by a concurrent skiplist
 * 
 * The Memtable class represents a single generation of in-memory writes. It uses a
 * lock-free skiplist for concurrent access and tracks metadata like creation time,
 * sequence number ranges, and approximate size for efficient management.
 * 
 * KEY FEATURES:
 * - Lock-free skiplist for high concurrency
 * - Tracks min/max sequence numbers for query optimization
 * - Supports tombstones for delete operations
 * - Atomic state transitions (ACTIVE -> FROZEN -> FLUSHED)
 * - Approximate size tracking for memory management
 * 
 * THREAD SAFETY:
 * - Insert operations are lock-free via skiplist
 * - State is atomic and can be checked/modified safely
 * - Size tracking uses atomic operations
 */
class Memtable {
public:
    std::atomic<State> state;           ///< Current lifecycle state (ACTIVE, FROZEN, or FLUSHED)
    std::atomic<size_t> approxSize;     ///< Approximate memory usage in bytes
    skiplist::Skiplist table;           ///< Lock-free skiplist storing the actual entries
    uint64_t creationTime;              ///< Timestamp when this memtable was created (for SSTable naming)
    uint64_t minSequence;               ///< Minimum sequence number in this memtable (for range optimization)
    uint64_t maxSequence;               ///< Maximum sequence number in this memtable (for range optimization)

    /**
     * @brief Default constructor - creates an ACTIVE memtable
     * 
     * Initializes the memtable in ACTIVE state with zero size, current timestamp,
     * and unbounded sequence range (will be updated as entries are inserted).
     */
    Memtable(): state(State::ACTIVE), approxSize(0), 
                creationTime(getCurrentTimestamp()), 
                minSequence(UINT64_MAX), maxSequence(0) {}

    /**
     * @brief Insert a key-value entry into the memtable
     * 
     * Inserts an entry into the skiplist if the memtable is in ACTIVE state.
     * Updates sequence number ranges and approximate size tracking.
     * 
     * @param key        The key to insert
     * @param val        The value to associate with the key
     * @param seq        Sequence number for MVCC versioning
     * @param isDeleted  True if this is a tombstone (deletion marker), false otherwise
     * @return true if insertion succeeded, false if memtable is not ACTIVE
     * 
     * TIME COMPLEXITY: O(log n) amortized
     * THREAD SAFETY: Lock-free, safe for concurrent inserts
     */
    bool insert(const std::string &key, const std::string &val, uint64_t seq, bool isDeleted = false) {
        if(state == State::ACTIVE) {
            skiplist::Entry entry(key, val, seq, isDeleted);
            if(table.add(entry)) {
                // Update sequence range for query optimization
                minSequence = std::min(minSequence, seq);
                maxSequence = std::max(maxSequence, seq);
                
                if (isDeleted) {
                    approxSize += key.size(); // tombstone, only count key
                } else {
                    approxSize += key.size() + val.size();
                }
                return true;
            }
        }
        return false;
    }

    /**
     * @brief Retrieve the value associated with a key
     * 
     * Searches for the key in the skiplist. Returns the value if found and not deleted.
     * 
     * @param key The key to search for
     * @return Optional containing the value if found and not deleted, nullopt otherwise
     * 
     * TIME COMPLEXITY: O(log n)
     * THREAD SAFETY: Lock-free, safe for concurrent access
     */
    std::optional<std::string> get(const std::string &key) {
        skiplist::Entry target(key, "", 0);
        skiplist::Entry result;
        bool found = table.search(target, &result);

        if (found && !result.isDeleted) {
            return result.value;
        }
        return std::nullopt;
    }

    /**
     * @brief Check if a key has been deleted (has a tombstone)
     * 
     * Determines if the key exists in the memtable as a deletion marker.
     * This is important for the read path to correctly handle deletes.
     * 
     * @param key The key to check
     * @return true if the key exists as a tombstone, false otherwise
     * 
     * TIME COMPLEXITY: O(log n)
     */
    bool containsTombstone(const std::string &key) {
        skiplist::Entry target(key, "", 0);
        skiplist::Entry result;
        bool found = table.search(target, &result);
        return found && result.isDeleted;
    }

    /**
     * @brief Get all entries in sorted order for flushing to SSTable
     * 
     * Extracts all entries from the skiplist in sorted key order. Used during
     * the flush process to write entries to an SSTable.
     * 
     * @return Vector of (key, Entry) pairs sorted by key
     * 
     * TIME COMPLEXITY: O(n) where n is the number of entries
     */
    std::vector<std::pair<std::string, skiplist::Entry>> getAllEntries() {
        std::vector<std::pair<std::string, skiplist::Entry>> entries;
        
        for (auto it = table.begin(); it != table.end(); ++it) {
            entries.emplace_back(it->key, *it);
        }
        
        // Entries should already be sorted due to skiplist ordering
        return entries;
    }

    /**
     * @brief Execute a range query between two keys
     * 
     * Returns all non-deleted entries with keys in the range [startKey, endKey].
     * 
     * @param startKey The inclusive start of the range
     * @param endKey   The inclusive end of the range
     * @return Vector of (key, value) pairs in the range
     * 
     * TIME COMPLEXITY: O(log n + k) where k is the result size
     */
    std::vector<std::pair<std::string, std::string>> rangeQuery(const std::string &startKey, const std::string &endKey) {
        return table.rangeQuery(startKey, endKey);
    }

    /**
     * @brief Scan all non-deleted entries in the memtable
     * 
     * @return Vector of all (key, value) pairs
     */
    std::vector<std::pair<std::string, std::string>> scanAll() {
        return table.scanAll();
    }

    /**
     * @brief Transition memtable to FROZEN state
     * 
     * Marks the memtable as read-only and ready for background flushing.
     * After this call, no new writes will be accepted.
     */
    void markFrozen() {
        state = State::FROZEN;
    }

    /**
     * @brief Transition memtable to FLUSHED state
     * 
     * Marks the memtable as successfully persisted to disk.
     * After this call, the memtable can be garbage collected.
     */
    void markFlushed() {
        state = State::FLUSHED;
    }
    
private:
    /**
     * @brief Get current timestamp in milliseconds since epoch
     * @return Timestamp in milliseconds
     */
    uint64_t getCurrentTimestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
};

/**
 * @class MemtableManager
 * @brief Manages multiple memtable instances and coordinates writes, flushes, and recovery
 * 
 * The MemtableManager orchestrates the lifecycle of memtables in an LSM-tree storage engine.
 * It handles:
 * - Multiple concurrent memtables (active + frozen)
 * - Write-ahead logging (WAL) for durability
 * - Automatic memtable rotation based on size threshold
 * - Background flushing to SSTables
 * - Crash recovery via WAL replay
 * - Multi-version concurrency control (MVCC) with sequence numbers
 * 
 * ARCHITECTURE:
 * =============
 * The manager maintains a vector of memtables:
 * - Last element: Active memtable (accepts writes)
 * - Earlier elements: Frozen memtables (queued for flush)
 * - Flushed memtables are removed after persistence
 * 
 * BACKGROUND FLUSH THREAD:
 * ========================
 * A dedicated background thread continuously monitors for frozen memtables
 * and flushes them to SSTables. This prevents blocking the write path and
 * ensures bounded memory usage.
 * 
 * DURABILITY:
 * ===========
 * All writes are logged to WAL before being applied to the memtable.
 * On crash recovery, the WAL is replayed to reconstruct lost in-memory state.
 * 
 * THREAD SAFETY:
 * ==============
 * - Mutex protects manager-level operations (rotation, table list modifications)
 * - Individual memtables use lock-free operations
 * - Background flush thread coordinates via condition variable
 */
class MemtableManager {
private:
    std::mutex mtx;                                  ///< Protects tables vector and manager state
    std::vector<std::shared_ptr<Memtable>> tables;   ///< Active and frozen memtables (newest last)
    size_t sizeLimit;                                ///< Size threshold for memtable rotation (bytes)
    std::atomic<uint64_t> sequenceNumber{0};         ///< Monotonic sequence number for MVCC
    std::unique_ptr<wal::WAL> walLog;                ///< Write-ahead log for durability
    
    // Background flushing infrastructure
    std::thread flushThread;                         ///< Background thread for flushing frozen memtables
    std::atomic<bool> shouldStop{false};             ///< Signal to stop background thread
    std::condition_variable flushCondition;          ///< Notifies flush thread of work
    std::mutex flushMutex;                           ///< Synchronizes flush thread wakeup
    
    /**
     * @brief Write an operation to the write-ahead log
     * 
     * Logs the operation to WAL before applying it to the memtable.
     * This ensures durability - operations can be replayed on crash recovery.
     * 
     * @param key       The key being modified
     * @param val       The value (empty for deletes)
     * @param isDeleted True if this is a delete operation
     */
    void writeToWAL(const std::string &key, const std::string &val, bool isDeleted) {
        if (walLog) {
            wal::Operation op = isDeleted ? wal::Operation::DELETE : wal::Operation::INSERT;
            uint64_t seq = sequenceNumber.load();
            walLog->append(seq, op, key, val);
        }
    }
    
    /**
     * @brief Background worker thread that flushes frozen memtables to SSTables
     * 
     * This thread runs continuously, waiting for frozen memtables to appear.
     * When notified:
     * 1. Retrieves list of frozen memtables
     * 2. Flushes each to an SSTable file
     * 3. Marks them as FLUSHED
     * 4. Triggers cleanup to remove flushed memtables from memory
     * 
     * This design prevents write stalls by offloading flush I/O to background.
     */
    void backgroundFlushWorker() {
        while (!shouldStop.load()) {
            std::unique_lock<std::mutex> lock(flushMutex);
            // Wait for work (frozen tables) or stop signal
            flushCondition.wait(lock, [this] { 
                return shouldStop.load() || hasFrozenTables(); 
            });
            
            if (shouldStop.load()) break;
            
            // Get frozen tables to flush
            auto frozenTables = getFrozenTables();
            lock.unlock();
            
            // Flush each frozen table (I/O intensive, outside lock)
            for (auto& table : frozenTables) {
                flushTableToSSTable(table);
            }
            
            // Clean up flushed tables from memory
            cleanupFlushedTables();
        }
    }
    
    /**
     * @brief Check if there are any frozen memtables waiting to be flushed
     * @return true if at least one frozen memtable exists
     */
    bool hasFrozenTables() {
        std::lock_guard<std::mutex> lock(mtx);
        for (const auto& table : tables) {
            if (table->state == State::FROZEN) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * @brief Flush a frozen memtable to an SSTable file
     * 
     * Creates an SSTable file containing all entries from the frozen memtable.
     * The SSTable filename encodes creation time and sequence range for efficient
     * query routing and compaction.
     * 
     * SSTable Format: sstable_{timestamp}_{minSeq}_{maxSeq}.sst
     * 
     * @param table The frozen memtable to flush
     */
    void flushTableToSSTable(std::shared_ptr<Memtable> table) {
        if (table->state != State::FROZEN) {
            return;
        }
        
        // Generate SSTable filename with metadata for query optimization
        std::string sstableFilename = "sstable_" + 
            std::to_string(table->creationTime) + "_" +
            std::to_string(table->minSequence) + "_" +
            std::to_string(table->maxSequence) + ".sst";
        
        // Get all entries from memtable in sorted order
        auto entries = table->getAllEntries();
        
        if (!entries.empty()) {
            // Create SSTable on disk
            sstable::SSTable sstable(sstableFilename);
            
            if (sstable.create(entries)) {
                // Mark table as flushed (can be garbage collected)
                table->markFlushed();
                
                // TODO: Update LSM levels manager for compaction
                // TODO: Truncate WAL up to this sequence number
                
                std::cout << "Successfully flushed memtable to " << sstableFilename 
                         << " with " << entries.size() << " entries" << std::endl;
            } else {
                std::cerr << "Failed to flush memtable to SSTable: " << sstableFilename << std::endl;
            }
        }
    }

public:
    /**
     * @brief Construct a new MemtableManager
     * 
     * Initializes the manager with an initial active memtable, WAL for durability,
     * and starts the background flush thread.
     * 
     * @param limit    Size threshold in bytes for memtable rotation (default: 64MB)
     * @param walPath  Path to the write-ahead log file (default: "wal.log")
     * 
     * INITIALIZATION SEQUENCE:
     * 1. Create first active memtable
     * 2. Open/create WAL file
     * 3. Start background flush thread
     */
    MemtableManager(size_t limit = 64 * 1000 * 1000, const std::string& walPath = "wal.log") 
        : sizeLimit(limit) {
        
        tables.push_back(std::make_shared<Memtable>());
        
        // Initialize WAL
        try {
            walLog = std::make_unique<wal::WAL>(walPath);
        } catch (const std::exception& e) {
            std::cerr << "Failed to initialize WAL: " << e.what() << std::endl;
            // Continue without WAL (not recommended for production)
        }
        
        // Start background flush thread
        flushThread = std::thread(&MemtableManager::backgroundFlushWorker, this);
    }
    
    /**
     * @brief Destructor - ensures clean shutdown
     * 
     * Stops the background flush thread gracefully and flushes any remaining
     * frozen memtables to ensure data is not lost.
     * 
     * SHUTDOWN SEQUENCE:
     * 1. Signal background thread to stop
     * 2. Wait for thread to exit
     * 3. Flush any remaining frozen memtables
     */
    ~MemtableManager() {
        // Stop background thread
        shouldStop = true;
        flushCondition.notify_all();
        
        if (flushThread.joinable()) {
            flushThread.join();
        }
        
        // Flush any remaining frozen tables
        auto frozenTables = getFrozenTables();
        for (auto& table : frozenTables) {
            flushTableToSSTable(table);
        }
    }

    /**
     * @brief Insert a key-value pair into the memtable
     * 
     * WRITE PATH:
     * 1. Log operation to WAL (for durability)
     * 2. Assign monotonic sequence number
     * 3. Insert into active memtable
     * 4. Check if memtable size exceeds threshold
     * 5. If threshold exceeded:
     *    a. Freeze current memtable
     *    b. Create new active memtable
     *    c. Notify background flush thread
     * 
     * @param key   The key to insert
     * @param val   The value to associate with the key
     * @return true if insertion succeeded, false otherwise
     * 
     * DURABILITY: Operation is logged to WAL before being applied
     * THREAD SAFETY: Safe for concurrent inserts
     */
    bool insert(const std::string &key, const std::string &val) {
        // Log to WAL first for durability
        writeToWAL(key, val, false);
        
        std::lock_guard<std::mutex> lock(mtx);
        auto current_table = tables.back();
        uint64_t seq = sequenceNumber.fetch_add(1);
        
        if(current_table->insert(key, val, seq, false)) {
            if(current_table->approxSize >= sizeLimit) {
                current_table->markFrozen();
                tables.push_back(std::make_shared<Memtable>());
                
                // Notify flush thread
                flushCondition.notify_one();
            }
            return true;
        }
        return false;
    }

    /**
     * @brief Search for a key across all memtables
     * 
     * READ PATH:
     * 1. Search from newest to oldest memtable (most recent wins)
     * 2. Skip flushed memtables (already on disk)
     * 3. Check for live value first
     * 4. Check for tombstone (deletion marker)
     * 5. If not found in any memtable, TODO: search SSTables
     * 
     * @param key The key to search for
     * @return Optional containing the value if found and not deleted, nullopt otherwise
     * 
     * MVCC SEMANTICS: Returns the most recent version (highest sequence number)
     * THREAD SAFETY: Safe for concurrent reads and writes
     */
    std::optional<std::string> search(const std::string &key) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Search from newest to oldest (most recent wins)
        for (auto it = tables.rbegin(); it != tables.rend(); ++it) {
            if ((*it)->state == State::FLUSHED) {
                continue; // Skip flushed tables
            }
            
            // First check for live value
            auto result = (*it)->get(key);
            if (result.has_value()) {
                return result;
            }
            
            // Then check for tombstone (deletion marker)
            if ((*it)->containsTombstone(key)) {
                return std::nullopt; // Key was deleted
            }
        }
        
        // TODO: Search in SSTables when LSM levels are implemented
        return std::nullopt;
    }

    /**
     * @brief Delete a key by inserting a tombstone
     * 
     * In LSM-trees, deletes are handled by inserting a special "tombstone" marker.
     * The actual data is removed during compaction.
     * 
     * DELETE PATH:
     * 1. Log delete operation to WAL
     * 2. Insert tombstone (entry with isDeleted=true) into active memtable
     * 3. Check size threshold and rotate if needed
     * 
     * @param key The key to delete
     * @return true if tombstone insertion succeeded, false otherwise
     * 
     * NOTE: The key may still exist in older memtables or SSTables until compaction
     */
    bool erase(const std::string &key) {
        // Log to WAL first
        writeToWAL(key, "", true);
        
        std::lock_guard<std::mutex> lock(mtx);
        auto current_table = tables.back();
        uint64_t seq = sequenceNumber.fetch_add(1);
        
        if(current_table->insert(key, "", seq, true)) { // Create tombstone
            if(current_table->approxSize >= sizeLimit) {
                current_table->markFrozen();
                tables.push_back(std::make_shared<Memtable>());
                
                // Notify flush thread
                flushCondition.notify_one();
            }
            return true;
        }
        return false;
    }

    /**
     * @brief Execute a range query across all memtables
     * 
     * Returns all key-value pairs where startKey <= key <= endKey.
     * Merges results from multiple memtables, with newer entries taking precedence.
     * Handles tombstones correctly by excluding deleted keys.
     * 
     * MERGE ALGORITHM:
     * 1. Iterate through memtables from oldest to newest
     * 2. For each entry in range, check if newer version exists
     * 3. If entry is tombstone, remove from results
     * 4. If entry is live and most recent, include in results
     * 
     * @param startKey Inclusive start of range
     * @param endKey   Inclusive end of range
     * @return Vector of (key, value) pairs in sorted order
     * 
     * TIME COMPLEXITY: O(k * m * log n) where k=result size, m=num memtables, n=entries per table
     */
    std::vector<std::pair<std::string, std::string>> rangeQuery(
        const std::string &startKey, const std::string &endKey) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Use a map to merge results from multiple memtables (newer wins)
        std::map<std::string, std::pair<std::string, uint64_t>> mergedResults;
        
        // Process tables from oldest to newest (so newer entries overwrite older ones)
        for (const auto& table : tables) {
            if (table->state == State::FLUSHED) {
                continue;
            }
            
            // Get range query results from this table
            for (auto it = table->table.lowerBound(startKey); 
                 it != table->table.end() && it.isValid() && it->key <= endKey; ++it) {
                
                // Skip if this key already exists with a newer sequence number
                auto existing = mergedResults.find(it->key);
                if (existing != mergedResults.end() && existing->second.second >= it->seq) {
                    continue;
                }
                
                if (it->isDeleted) {
                    // This is a tombstone - remove from results
                    mergedResults.erase(it->key);
                } else {
                    // This is a live value
                    mergedResults[it->key] = {it->value, it->seq};
                }
            }
        }
        
        // Convert map to vector
        std::vector<std::pair<std::string, std::string>> results;
        for (const auto& [key, valueSeqPair] : mergedResults) {
            results.emplace_back(key, valueSeqPair.first);
        }
        
        return results;
    }

    /**
     * @brief Get all frozen memtables waiting to be flushed
     * @return Vector of frozen memtable instances
     */
    std::vector<std::shared_ptr<Memtable>> getFrozenTables() {
        std::lock_guard<std::mutex> lock(mtx);
        std::vector<std::shared_ptr<Memtable>> frozen;
        
        for (auto& table : tables) {
            if (table->state == State::FROZEN) {
                frozen.push_back(table);
            }
        }
        return frozen;
    }

    /**
     * @brief Remove flushed memtables from memory
     * 
     * Removes all memtables in FLUSHED state from the tables vector,
     * freeing memory. Called after successful flush to SSTable.
     */
    void cleanupFlushedTables() {
        std::lock_guard<std::mutex> lock(mtx);
        tables.erase(
            std::remove_if(tables.begin(), tables.end(),
                [](const std::shared_ptr<Memtable>& table) {
                    return table->state == State::FLUSHED;
                }),
            tables.end()
        );
    }

    /**
     * @brief Get total memory usage across all active and frozen memtables
     * @return Total approximate memory usage in bytes
     */
    size_t getCurrentMemoryUsage() {
        std::lock_guard<std::mutex> lock(mtx);
        size_t total = 0;
        for (const auto& table : tables) {
            if (table->state != State::FLUSHED) {
                total += table->approxSize.load();
            }
        }
        return total;
    }
    
    /**
     * @brief Count active memtables
     * @return Number of memtables in ACTIVE state
     */
    size_t getActiveTableCount() {
        std::lock_guard<std::mutex> lock(mtx);
        return std::count_if(tables.begin(), tables.end(),
            [](const std::shared_ptr<Memtable>& table) {
                return table->state == State::ACTIVE;
            });
    }
    
    /**
     * @brief Count frozen memtables waiting for flush
     * @return Number of memtables in FROZEN state
     */
    size_t getFrozenTableCount() {
        std::lock_guard<std::mutex> lock(mtx);
        return std::count_if(tables.begin(), tables.end(),
            [](const std::shared_ptr<Memtable>& table) {
                return table->state == State::FROZEN;
            });
    }
    
    /**
     * @brief Force flush of active memtable
     * 
     * Manually triggers memtable rotation and flush, regardless of size threshold.
     * Useful for:
     * - Clean shutdown
     * - Testing
     * - Reducing memory pressure
     * - Ensuring data durability before risky operations
     */
    void forceFlush() {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Mark current active table as frozen if it has data
        if (!tables.empty() && tables.back()->state == State::ACTIVE && 
            tables.back()->approxSize > 0) {
            tables.back()->markFrozen();
            tables.push_back(std::make_shared<Memtable>());
        }
        
        // Notify flush thread
        flushCondition.notify_one();
    }
    
    /**
     * @brief Synchronize WAL to disk
     * 
     * Forces the WAL to flush buffered writes to disk. Ensures durability
     * of all logged operations up to this point.
     */
    void syncWAL() {
        if (walLog) {
            walLog->sync();
        }
    }
    
    /**
     * @brief Recover state from write-ahead log after a crash
     * 
     * RECOVERY PROCESS:
     * 1. Read all entries from WAL
     * 2. Replay each operation in order
     * 3. Rebuild memtable(s) with correct sequence numbers
     * 4. Update sequence counter to resume from correct point
     * 
     * This ensures that all writes logged to WAL before the crash are restored,
     * maintaining durability guarantees (the 'D' in ACID).
     * 
     * @return true if recovery succeeded, false if WAL is not available
     * 
     * CALL THIS: On database startup before accepting any writes
     */
    bool recover() {
        if (!walLog) {
            return false;
        }
        
        auto walEntries = walLog->recover();
        
        for (const auto& entry : walEntries) {
            if (entry.operation == wal::Operation::INSERT) {
                // Don't log to WAL during recovery (avoid duplicate entries)
                std::lock_guard<std::mutex> lock(mtx);
                auto current_table = tables.back();
                
                if (!current_table->insert(entry.key, entry.value, entry.sequenceNumber, false)) {
                    // If insert fails, create new table and try again
                    current_table->markFrozen();
                    tables.push_back(std::make_shared<Memtable>());
                    current_table = tables.back();
                    current_table->insert(entry.key, entry.value, entry.sequenceNumber, false);
                }
                
                // Update sequence number to resume from correct point
                sequenceNumber = std::max(sequenceNumber.load(), entry.sequenceNumber + 1);
                
            } else if (entry.operation == wal::Operation::DELETE) {
                // Handle delete during recovery
                std::lock_guard<std::mutex> lock(mtx);
                auto current_table = tables.back();
                
                if (!current_table->insert(entry.key, "", entry.sequenceNumber, true)) {
                    current_table->markFrozen();
                    tables.push_back(std::make_shared<Memtable>());
                    current_table = tables.back();
                    current_table->insert(entry.key, "", entry.sequenceNumber, true);
                }
                
                sequenceNumber = std::max(sequenceNumber.load(), entry.sequenceNumber + 1);
            }
        }
        
        return true;
    }
};

} // namespace memTable

/**
 * Thread-local random number generator for skiplist
 * 
 * The skiplist uses a random number generator to determine the level of new nodes.
 * Thread-local storage ensures each thread has its own generator for thread safety.
 */
thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());

#endif // MEMTABLE_HPP