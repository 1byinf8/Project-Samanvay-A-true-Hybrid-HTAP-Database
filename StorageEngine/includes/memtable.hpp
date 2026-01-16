// memtable.hpp - Memtable and MemtableManager for HTAP Database
#pragma once
#ifndef MEMTABLE_HPP
#define MEMTABLE_HPP

#include "lsm_levels.hpp"
#include "skiplist.hpp"
#include "sstable.hpp"
#include "wal.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace memTable {

enum class State { ACTIVE, FROZEN, FLUSHED };

class Memtable {
public:
  std::atomic<State> state;
  std::atomic<size_t> approxSize;
  skiplist::Skiplist table;
  uint64_t creationTime;
  uint64_t minSequence;
  uint64_t maxSequence;

  Memtable()
      : state(State::ACTIVE), approxSize(0),
        creationTime(getCurrentTimestamp()), minSequence(UINT64_MAX),
        maxSequence(0) {}

  bool insert(const std::string &key, const std::string &val, uint64_t seq,
              bool isDeleted = false) {
    if (state == State::ACTIVE) {
      skiplist::Entry entry(key, val, seq, isDeleted);
      if (table.add(entry)) {
        // Update sequence range
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

  std::optional<std::string> get(const std::string &key) {
    skiplist::Entry target(key, "", 0);
    skiplist::Entry result;
    bool found = table.search(target, &result);

    if (found && !result.isDeleted) {
      return result.value;
    }
    return std::nullopt;
  }

  bool containsTombstone(const std::string &key) {
    skiplist::Entry target(key, "", 0);
    skiplist::Entry result;
    bool found = table.search(target, &result);
    return found && result.isDeleted;
  }

  // Get all entries sorted by key for flushing
  std::vector<std::pair<std::string, skiplist::Entry>> getAllEntries() {
    std::vector<std::pair<std::string, skiplist::Entry>> entries;

    for (auto it = table.begin(); it != table.end(); ++it) {
      entries.emplace_back(it->key, *it);
    }

    // Entries should already be sorted due to skiplist ordering
    return entries;
  }

  std::vector<std::pair<std::string, std::string>>
  rangeQuery(const std::string &startKey, const std::string &endKey) {
    return table.rangeQuery(startKey, endKey);
  }

  std::vector<std::pair<std::string, std::string>> scanAll() {
    return table.scanAll();
  }

  void markFrozen() { state = State::FROZEN; }

  void markFlushed() { state = State::FLUSHED; }

private:
  uint64_t getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

class MemtableManager {
private:
  std::mutex mtx;
  std::vector<std::shared_ptr<Memtable>> tables;
  size_t sizeLimit;
  std::atomic<uint64_t> sequenceNumber{0};
  std::unique_ptr<wal::WAL> walLog;
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;
  std::string dataDirectory_;

  // Background flushing
  std::thread flushThread;
  std::atomic<bool> shouldStop{false};
  std::condition_variable flushCondition;
  std::mutex flushMutex;

  void writeToWAL(const std::string &key, const std::string &val,
                  bool isDeleted) {
    if (walLog) {
      wal::Operation op =
          isDeleted ? wal::Operation::DELETE : wal::Operation::INSERT;
      uint64_t seq = sequenceNumber.load();
      walLog->append(seq, op, key, val);
    }
  }

  void backgroundFlushWorker() {
    while (!shouldStop.load()) {
      std::unique_lock<std::mutex> lock(flushMutex);
      flushCondition.wait(
          lock, [this] { return shouldStop.load() || hasFrozenTables(); });

      if (shouldStop.load())
        break;

      // Get frozen tables to flush
      auto frozenTables = getFrozenTables();
      lock.unlock();

      // Flush each frozen table
      for (auto &table : frozenTables) {
        flushTableToSSTable(table);
      }

      // Clean up flushed tables
      cleanupFlushedTables();
    }
  }

  bool hasFrozenTables() {
    std::lock_guard<std::mutex> lock(mtx);
    for (const auto &table : tables) {
      if (table->state == State::FROZEN) {
        return true;
      }
    }
    return false;
  }

  void flushTableToSSTable(std::shared_ptr<Memtable> table) {
    if (table->state != State::FROZEN) {
      return;
    }

    // Get all entries from memtable
    auto entries = table->getAllEntries();
    if (entries.empty()) {
      table->markFlushed();
      return;
    }

    // Generate SSTable filename using LSM manager if available
    std::string sstableFilename;
    if (lsmManager_) {
      sstableFilename =
          lsmManager_->generateSSTablePath(0); // Level 0 for fresh flushes
    } else {
      sstableFilename = dataDirectory_ + "/sstable_" +
                        std::to_string(table->creationTime) + "_" +
                        std::to_string(table->minSequence) + "_" +
                        std::to_string(table->maxSequence) + ".sst";
    }

    // Create SSTable
    sstable::SSTable sst(sstableFilename);
    if (sst.create(entries)) {
      // Mark table as flushed
      table->markFlushed();

      // Register with LSM manager (fixes critical issue #3)
      if (lsmManager_) {
        // Create SSTable metadata
        lsm::SSTableMeta meta;
        meta.filePath = sstableFilename;
        meta.minKey = entries.front().first;
        meta.maxKey = entries.back().first;
        meta.minSequence = table->minSequence;
        meta.maxSequence = table->maxSequence;
        meta.fileSize = 0; // Will be updated by LSM manager if needed
        meta.entryCount = entries.size();
        meta.level = 0;
        meta.creationTime = table->creationTime;
        meta.isColumnar = false;

        lsmManager_->addSSTable(meta);
        std::cout << "Registered SSTable with LSM manager at Level 0"
                  << std::endl;
      }

      // Truncate WAL up to this sequence number (fixes critical issue #2)
      if (walLog && table->maxSequence > 0) {
        walLog->truncate(table->maxSequence);
        std::cout << "Truncated WAL up to sequence " << table->maxSequence
                  << std::endl;
      }

      std::cout << "Successfully flushed memtable to " << sstableFilename
                << " with " << entries.size() << " entries" << std::endl;
    } else {
      std::cerr << "Failed to flush memtable to SSTable: " << sstableFilename
                << std::endl;
    }
  }

public:
  // Constructor with LSM manager integration for proper SSTable registration
  MemtableManager(size_t limit = 64 * 1000 * 1000,
                  const std::string &walPath = "wal.log",
                  std::shared_ptr<lsm::LSMTreeManager> lsmManager = nullptr)
      : sizeLimit(limit), lsmManager_(lsmManager), dataDirectory_("./data") {

    tables.push_back(std::make_shared<Memtable>());

    // Get data directory from LSM config if available
    if (lsmManager_) {
      dataDirectory_ = lsmManager_->getConfig().dataDirectory;
    }

    // Initialize WAL
    try {
      walLog = std::make_unique<wal::WAL>(walPath);
    } catch (const std::exception &e) {
      std::cerr << "Failed to initialize WAL: " << e.what() << std::endl;
      // Continue without WAL (not recommended for production)
    }

    // Start background flush thread
    flushThread = std::thread(&MemtableManager::backgroundFlushWorker, this);
  }

  ~MemtableManager() {
    // Stop background thread
    shouldStop = true;
    flushCondition.notify_all();

    if (flushThread.joinable()) {
      flushThread.join();
    }

    // Flush any remaining frozen tables
    auto frozenTables = getFrozenTables();
    for (auto &table : frozenTables) {
      flushTableToSSTable(table);
    }
  }

  bool insert(const std::string &key, const std::string &val) {
    // Log to WAL first for durability
    writeToWAL(key, val, false);

    std::lock_guard<std::mutex> lock(mtx);
    auto current_table = tables.back();
    uint64_t seq = sequenceNumber.fetch_add(1);

    if (current_table->insert(key, val, seq, false)) {
      if (current_table->approxSize >= sizeLimit) {
        current_table->markFrozen();
        tables.push_back(std::make_shared<Memtable>());

        // Notify flush thread
        flushCondition.notify_one();
      }
      return true;
    }
    return false;
  }

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

  bool erase(const std::string &key) {
    // Log to WAL first
    writeToWAL(key, "", true);

    std::lock_guard<std::mutex> lock(mtx);
    auto current_table = tables.back();
    uint64_t seq = sequenceNumber.fetch_add(1);

    if (current_table->insert(key, "", seq, true)) { // Create tombstone
      if (current_table->approxSize >= sizeLimit) {
        current_table->markFrozen();
        tables.push_back(std::make_shared<Memtable>());

        // Notify flush thread
        flushCondition.notify_one();
      }
      return true;
    }
    return false;
  }

  // Range query across all active/frozen memtables
  std::vector<std::pair<std::string, std::string>>
  rangeQuery(const std::string &startKey, const std::string &endKey) {
    std::lock_guard<std::mutex> lock(mtx);

    // Use a map to merge results from multiple memtables (newer wins)
    std::map<std::string, std::pair<std::string, uint64_t>> mergedResults;

    // Process tables from oldest to newest (so newer entries overwrite older
    // ones)
    for (const auto &table : tables) {
      if (table->state == State::FLUSHED) {
        continue;
      }

      // Get range query results from this table
      for (auto it = table->table.lowerBound(startKey);
           it != table->table.end() && it.isValid() && it->key <= endKey;
           ++it) {

        // Skip if this key already exists with a newer sequence number
        auto existing = mergedResults.find(it->key);
        if (existing != mergedResults.end() &&
            existing->second.second >= it->seq) {
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
    for (const auto &[key, valueSeqPair] : mergedResults) {
      results.emplace_back(key, valueSeqPair.first);
    }

    return results;
  }

  // Get all active and frozen tables for flushing
  std::vector<std::shared_ptr<Memtable>> getFrozenTables() {
    std::lock_guard<std::mutex> lock(mtx);
    std::vector<std::shared_ptr<Memtable>> frozen;

    for (auto &table : tables) {
      if (table->state == State::FROZEN) {
        frozen.push_back(table);
      }
    }
    return frozen;
  }

  // Clean up flushed tables
  void cleanupFlushedTables() {
    std::lock_guard<std::mutex> lock(mtx);
    tables.erase(std::remove_if(tables.begin(), tables.end(),
                                [](const std::shared_ptr<Memtable> &table) {
                                  return table->state == State::FLUSHED;
                                }),
                 tables.end());
  }

  size_t getCurrentMemoryUsage() {
    std::lock_guard<std::mutex> lock(mtx);
    size_t total = 0;
    for (const auto &table : tables) {
      if (table->state != State::FLUSHED) {
        total += table->approxSize.load();
      }
    }
    return total;
  }

  size_t getActiveTableCount() {
    std::lock_guard<std::mutex> lock(mtx);
    return std::count_if(tables.begin(), tables.end(),
                         [](const std::shared_ptr<Memtable> &table) {
                           return table->state == State::ACTIVE;
                         });
  }

  size_t getFrozenTableCount() {
    std::lock_guard<std::mutex> lock(mtx);
    return std::count_if(tables.begin(), tables.end(),
                         [](const std::shared_ptr<Memtable> &table) {
                           return table->state == State::FROZEN;
                         });
  }

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

  void syncWAL() {
    if (walLog) {
      walLog->sync();
    }
  }

  // Recovery from WAL on startup
  bool recover() {
    if (!walLog) {
      return false;
    }

    auto walEntries = walLog->recover();

    for (const auto &entry : walEntries) {
      if (entry.operation == wal::Operation::INSERT) {
        // Don't log to WAL during recovery
        std::lock_guard<std::mutex> lock(mtx);
        auto current_table = tables.back();

        if (!current_table->insert(entry.key, entry.value, entry.sequenceNumber,
                                   false)) {
          // If insert fails, create new table and try again
          current_table->markFrozen();
          tables.push_back(std::make_shared<Memtable>());
          current_table = tables.back();
          current_table->insert(entry.key, entry.value, entry.sequenceNumber,
                                false);
        }

        // Update sequence number
        sequenceNumber =
            std::max(sequenceNumber.load(), entry.sequenceNumber + 1);

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

        sequenceNumber =
            std::max(sequenceNumber.load(), entry.sequenceNumber + 1);
      }
    }

    return true;
  }
};

} // namespace memTable

#endif // MEMTABLE_HPP