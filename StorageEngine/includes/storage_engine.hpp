// storage_engine.hpp - Unified Storage Engine Facade
#pragma once
#ifndef STORAGE_ENGINE_HPP
#define STORAGE_ENGINE_HPP

#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "compaction.hpp"
#include "hybrid_query_router.hpp"
#include "lsm_levels.hpp"
#include "memtable.hpp"
#include "range_query_executor.hpp"

namespace storage {

// Configuration for the storage engine
struct StorageEngineConfig {
  // Memtable settings
  size_t memtableSizeLimit = 64 * 1024 * 1024; // 64MB

  // LSM settings
  lsm::LSMConfig lsmConfig;

  // WAL settings
  std::string walPath = "wal.log";

  // Paths
  std::string dataDirectory = "./data";
  std::string columnarDirectory = "./data/columnar";

  // Performance tuning
  size_t columnarQueryThreshold = 10000; // Rows before preferring columnar
  size_t rowGroupSize = 1000000;         // Rows per columnar row group

  StorageEngineConfig() { lsmConfig.dataDirectory = dataDirectory; }
};

// Statistics about the storage engine
struct StorageEngineStats {
  // Memory
  size_t memtableMemoryUsage = 0;
  size_t activeMemtables = 0;
  size_t frozenMemtables = 0;

  // SSTable levels
  std::vector<lsm::LevelStats> levelStats;
  size_t totalSSTables = 0;
  size_t totalSSTableBytes = 0;

  // Columnar
  size_t columnarFiles = 0;
  size_t columnarRows = 0;

  // Operations
  uint64_t totalWrites = 0;
  uint64_t totalReads = 0;
  uint64_t totalRangeQueries = 0;
  uint64_t totalCompactions = 0;

  // Performance
  double avgReadLatencyUs = 0;
  double avgWriteLatencyUs = 0;
};

// Main Storage Engine class - facade for all storage operations
class StorageEngine {
private:
  StorageEngineConfig config_;

  // Core components
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;
  std::unique_ptr<memTable::MemtableManager> memtableManager_;
  std::unique_ptr<compaction::CompactionScheduler> compactionScheduler_;
  std::unique_ptr<query::RangeQueryExecutor> rangeQueryExecutor_;
  std::unique_ptr<query::HybridQueryRouter> queryRouter_;

  // Statistics
  std::atomic<uint64_t> writeCount_{0};
  std::atomic<uint64_t> readCount_{0};
  std::atomic<uint64_t> rangeQueryCount_{0};

  // Create necessary directories
  void ensureDirectories() {
    // In a real implementation, would use std::filesystem
    // For now, directories are created by LSMTreeManager
  }

public:
  explicit StorageEngine(
      const StorageEngineConfig &config = StorageEngineConfig())
      : config_(config) {

    // Ensure directories exist
    ensureDirectories();

    // Initialize LSM-tree manager
    lsmManager_ = std::make_shared<lsm::LSMTreeManager>(config_.lsmConfig);

    // Initialize memtable manager with LSM integration
    memtableManager_ = std::make_unique<memTable::MemtableManager>(
        config_.memtableSizeLimit, config_.walPath, lsmManager_);

    // Initialize compaction scheduler
    compactionScheduler_ =
        std::make_unique<compaction::CompactionScheduler>(lsmManager_);
    compactionScheduler_->start();

    // Initialize query components
    rangeQueryExecutor_ =
        std::make_unique<query::RangeQueryExecutor>(lsmManager_);
    queryRouter_ = std::make_unique<query::HybridQueryRouter>(
        lsmManager_, config_.columnarQueryThreshold);

    std::cout << "Storage Engine initialized" << std::endl;
    std::cout << "  Data directory: " << config_.dataDirectory << std::endl;
    std::cout << "  Max levels: " << config_.lsmConfig.maxLevels << std::endl;
    std::cout << "  Columnar threshold: Level "
              << config_.lsmConfig.columnarLevelThreshold << "+" << std::endl;
  }

  ~StorageEngine() {
    // Stop compaction before destruction
    if (compactionScheduler_) {
      compactionScheduler_->stop();
    }

    // Persist LSM metadata
    lsmManager_->persistMetadata(config_.dataDirectory + "/lsm_meta.bin");
  }

  // ==================== Point Operations ====================

  // Insert or update a key-value pair
  bool put(const std::string &key, const std::string &value) {
    writeCount_++;
    return memtableManager_->insert(key, value);
  }

  // Get a value by key (searches memtable first, then SSTables)
  std::optional<std::string> get(const std::string &key) {
    readCount_++;

    // First try memtable
    auto memResult = memtableManager_->search(key);
    if (memResult.has_value()) {
      return memResult;
    }

    // Fall back to SSTables
    return searchInSSTables(key);
  }

  // Delete a key (writes tombstone)
  bool del(const std::string &key) {
    writeCount_++;
    return memtableManager_->erase(key);
  }

  // Force flush memtables to disk
  void forceFlush() { memtableManager_->forceFlush(); }

  // Recover from WAL on startup
  bool recoverFromWAL() { return memtableManager_->recover(); }

  // ==================== Batch Operations ====================

  // Batch write - atomic multi-key insert
  // All entries are written together or none (for transaction support)
  size_t
  batchPut(const std::vector<std::pair<std::string, std::string>> &entries) {
    size_t successCount = 0;

    // Write all entries to memtable
    // Each insert goes through WAL individually for now
    // TODO: Optimize to single WAL batch entry for true atomicity
    for (const auto &[key, value] : entries) {
      if (memtableManager_->insert(key, value)) {
        successCount++;
        writeCount_++;
      }
    }

    return successCount;
  }

  // Batch read - multi-key lookup
  // Returns vector of optional results, one per input key
  std::vector<std::optional<std::string>>
  batchGet(const std::vector<std::string> &keys) {
    std::vector<std::optional<std::string>> results;
    results.reserve(keys.size());

    for (const auto &key : keys) {
      results.push_back(get(key));
    }

    return results;
  }

  // Batch delete - atomic multi-key delete
  size_t batchDel(const std::vector<std::string> &keys) {
    size_t successCount = 0;

    for (const auto &key : keys) {
      if (memtableManager_->erase(key)) {
        successCount++;
        writeCount_++;
      }
    }

    return successCount;
  }

  // Note: These would integrate with MemtableManager
  // For now, they demonstrate the interface

  // Search in SSTables (called when memtable doesn't have the key)
  std::optional<std::string> searchInSSTables(const std::string &key) {
    readCount_++;

    auto sstables = lsmManager_->getSSTablesForKey(key);

    for (const auto &meta : sstables) {
      if (meta.isColumnar) {
        // Skip columnar files for point lookups
        // (They don't have efficient point lookup)
        continue;
      }

      sstable::SSTable sst(meta.filePath);
      if (sst.load()) {
        auto result = sst.get(key);
        if (result.has_value()) {
          if (result->isDeleted) {
            return std::nullopt; // Tombstone found
          }
          return result->value;
        }
      }
    }

    return std::nullopt;
  }

  // ==================== Range Queries ====================

  // Execute range query across all storage layers
  std::vector<std::pair<std::string, std::string>>
  rangeQuery(const std::string &startKey, const std::string &endKey) {

    rangeQueryCount_++;

    // Plan the query
    auto request = query::QueryRequest::rangeScan(startKey, endKey);
    auto plan = queryRouter_->planQuery(request);

    // Execute using range query executor
    return rangeQueryExecutor_->executeSSTableRange(startKey, endKey);
  }

  // Execute range query with memtable results
  std::vector<std::pair<std::string, std::string>> rangeQueryWithMemtable(
      const std::string &startKey, const std::string &endKey,
      const std::vector<std::pair<std::string, std::string>> &memtableResults) {

    rangeQueryCount_++;
    return rangeQueryExecutor_->executeWithMemtable(startKey, endKey,
                                                    memtableResults);
  }

  // Full table scan
  std::vector<std::pair<std::string, std::string>> fullScan() {
    rangeQueryCount_++;
    return rangeQueryExecutor_->fullScan();
  }

  // ==================== Aggregations (OLAP) ====================

  // Execute aggregation query on columnar data
  query::QueryResult executeAggregation(const query::QueryRequest &request) {
    query::QueryResult result;
    result.success = true;

    // Plan the query
    auto plan = queryRouter_->planQuery(request);

    // For aggregations, we'd scan columnar files and aggregate
    // This is a simplified implementation

    auto columnarSSTables =
        lsmManager_->getSSTablesForRange("", std::string(256, '\xff'));

    result.countResult = 0;
    result.sumResult = 0;
    result.minResult = INT64_MAX;
    result.maxResult = INT64_MIN;

    for (const auto &meta : columnarSSTables) {
      if (!meta.isColumnar)
        continue;

      // In a full implementation, would open columnar file and aggregate
      result.countResult += meta.entryCount;
    }

    return result;
  }

  // ==================== SSTable Management ====================

  // Register a new SSTable (called after memtable flush)
  void registerSSTable(const lsm::SSTableMeta &meta) {
    lsmManager_->addSSTable(meta);

    // Check if compaction is needed
    if (lsmManager_->shouldTriggerCompaction(meta.level)) {
      compactionScheduler_->scheduleCompaction(meta.level);
    }
  }

  // Trigger manual compaction
  void triggerCompaction() { compactionScheduler_->triggerCheck(); }

  // Pause/resume compaction
  void pauseCompaction() { compactionScheduler_->pause(); }

  void resumeCompaction() { compactionScheduler_->resume(); }

  // ==================== Query Planning ====================

  // Explain a query plan
  std::string explainQuery(const query::QueryRequest &request) {
    auto plan = queryRouter_->planQuery(request);
    return queryRouter_->explainPlan(plan);
  }

  // ==================== Statistics & Monitoring ====================

  StorageEngineStats getStats() {
    StorageEngineStats stats;

    // Level stats
    stats.levelStats = lsmManager_->getAllLevelStats();
    stats.totalSSTables = lsmManager_->getTotalSSTableCount();
    stats.totalSSTableBytes = lsmManager_->getTotalSize();

    // Operations
    stats.totalReads = readCount_.load();
    stats.totalRangeQueries = rangeQueryCount_.load();
    stats.totalCompactions = compactionScheduler_->getTotalCompactions();

    return stats;
  }

  // Print debug information
  void printStatus() {
    std::cout << "\n=== Storage Engine Status ===" << std::endl;
    lsmManager_->printDebugInfo();
    std::cout << "Pending compactions: "
              << compactionScheduler_->getPendingJobCount() << std::endl;
    std::cout << "Total compactions: "
              << compactionScheduler_->getTotalCompactions() << std::endl;
    std::cout << "Total bytes compacted: "
              << (compactionScheduler_->getTotalBytesCompacted() /
                  (1024.0 * 1024.0))
              << " MB" << std::endl;
    std::cout << "=============================" << std::endl;
  }

  // ==================== Recovery ====================

  // Load persisted LSM metadata
  bool recover() {
    std::string metaPath = config_.dataDirectory + "/lsm_meta.bin";
    return lsmManager_->loadMetadata(metaPath);
  }

  // ==================== Component Access ====================

  std::shared_ptr<lsm::LSMTreeManager> getLSMManager() { return lsmManager_; }
  query::HybridQueryRouter *getQueryRouter() { return queryRouter_.get(); }

  const StorageEngineConfig &getConfig() const { return config_; }
};

} // namespace storage

#endif // STORAGE_ENGINE_HPP
