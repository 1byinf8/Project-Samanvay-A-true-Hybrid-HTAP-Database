// range_query_executor.hpp - Multi-layer range query executor
#pragma once
#ifndef RANGE_QUERY_EXECUTOR_HPP
#define RANGE_QUERY_EXECUTOR_HPP

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "lsm_levels.hpp"
#include "sstable.hpp"

namespace query {

// Result of a range query with sequence tracking for conflict resolution
struct RangeQueryResult {
  std::string key;
  std::string value;
  uint64_t sequence;
  bool isDeleted;

  RangeQueryResult(const std::string &k, const std::string &v, uint64_t seq,
                   bool del = false)
      : key(k), value(v), sequence(seq), isDeleted(del) {}
};

// Executes range queries across memtable and all SSTable levels
class RangeQueryExecutor {
private:
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;

  // Merge entry into the result map
  // Returns true if the entry should be included (newer than existing or first
  // occurrence)
  void mergeEntry(std::map<std::string, RangeQueryResult> &merged,
                  const std::string &key, const std::string &value,
                  uint64_t sequence, bool isDeleted) {

    auto it = merged.find(key);
    if (it == merged.end()) {
      // First occurrence of this key
      merged.emplace(key, RangeQueryResult(key, value, sequence, isDeleted));
    } else if (it->second.sequence < sequence) {
      // Newer version found
      it->second = RangeQueryResult(key, value, sequence, isDeleted);
    }
    // If existing has higher or equal sequence, keep it
  }

public:
  explicit RangeQueryExecutor(std::shared_ptr<lsm::LSMTreeManager> lsmManager)
      : lsmManager_(lsmManager) {}

  // Execute a range query from startKey to endKey (inclusive)
  // Merges results from all SSTable levels
  // Returns only non-deleted entries
  std::vector<std::pair<std::string, std::string>>
  executeSSTableRange(const std::string &startKey, const std::string &endKey) {

    // Map for merging: key -> (value, sequence, isDeleted)
    std::map<std::string, RangeQueryResult> merged;

    // Get all SSTables that might contain keys in range
    auto sstables = lsmManager_->getSSTablesForRange(startKey, endKey);

    // Query each SSTable (already sorted by level then by creation time)
    for (const auto &sstMeta : sstables) {
      sstable::SSTable sst(sstMeta.filePath);
      if (!sst.load()) {
        continue; // Skip if can't load
      }

      auto results = sst.rangeQuery(startKey, endKey);
      for (const auto &[key, entry] : results) {
        mergeEntry(merged, key, entry.value, entry.seq, entry.isDeleted);
      }
    }

    // Convert to final result (filtering out deleted entries)
    std::vector<std::pair<std::string, std::string>> finalResults;
    for (const auto &[key, result] : merged) {
      if (!result.isDeleted) {
        finalResults.emplace_back(key, result.value);
      }
    }

    return finalResults;
  }

  // Execute range query with additional memtable results
  // memtableResults should be pre-merged from the MemtableManager
  std::vector<std::pair<std::string, std::string>> executeWithMemtable(
      const std::string &startKey, const std::string &endKey,
      const std::vector<std::pair<std::string, std::string>> &memtableResults) {

    // Start with SSTable results
    std::map<std::string, RangeQueryResult> merged;
    auto sstables = lsmManager_->getSSTablesForRange(startKey, endKey);

    for (const auto &sstMeta : sstables) {
      sstable::SSTable sst(sstMeta.filePath);
      if (!sst.load())
        continue;

      auto results = sst.rangeQuery(startKey, endKey);
      for (const auto &[key, entry] : results) {
        mergeEntry(merged, key, entry.value, entry.seq, entry.isDeleted);
      }
    }

    // Memtable results are the most recent - they always win
    // Use a sequence number higher than any SSTable entry
    for (const auto &[key, value] : memtableResults) {
      // Memtable values are guaranteed to be non-deleted (filtered by
      // MemtableManager) Use UINT64_MAX to ensure memtable wins any conflict
      mergeEntry(merged, key, value, UINT64_MAX, false);
    }

    // Convert to final result
    std::vector<std::pair<std::string, std::string>> finalResults;
    for (const auto &[key, result] : merged) {
      if (!result.isDeleted) {
        finalResults.emplace_back(key, result.value);
      }
    }

    return finalResults;
  }

  // Execute range query with tombstone tracking
  // Returns both values and tombstones for more advanced merge scenarios
  std::pair<std::vector<std::pair<std::string, std::string>>,
            std::set<std::string>>
  executeWithTombstones(const std::string &startKey,
                        const std::string &endKey) {

    std::map<std::string, RangeQueryResult> merged;
    std::set<std::string> tombstones;

    auto sstables = lsmManager_->getSSTablesForRange(startKey, endKey);

    for (const auto &sstMeta : sstables) {
      sstable::SSTable sst(sstMeta.filePath);
      if (!sst.load())
        continue;

      auto results = sst.rangeQuery(startKey, endKey);
      for (const auto &[key, entry] : results) {
        mergeEntry(merged, key, entry.value, entry.seq, entry.isDeleted);
      }
    }

    // Build tombstone set and value vector
    std::vector<std::pair<std::string, std::string>> values;
    for (const auto &[key, result] : merged) {
      if (result.isDeleted) {
        tombstones.insert(key);
      } else {
        values.emplace_back(key, result.value);
      }
    }

    return {values, tombstones};
  }

  // Full table scan across all SSTables
  std::vector<std::pair<std::string, std::string>> fullScan() {
    // Use empty string to max string as range
    std::string minKey = "";
    std::string maxKey(256, '\xff'); // Very high key
    return executeSSTableRange(minKey, maxKey);
  }

  // Count entries in a range (useful for query planning)
  size_t countRange(const std::string &startKey, const std::string &endKey) {
    size_t count = 0;
    auto sstables = lsmManager_->getSSTablesForRange(startKey, endKey);

    for (const auto &sstMeta : sstables) {
      // Approximate: assume all entries in overlapping SSTables are in range
      // For exact count, we'd need to scan
      count += sstMeta.entryCount;
    }

    return count; // This is an upper bound estimate
  }
};

// Iterator for streaming large range query results
class RangeQueryIterator {
private:
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;
  std::string startKey_;
  std::string endKey_;
  std::string currentKey_;
  bool initialized_;
  bool exhausted_;

  // Current batch of results
  std::vector<std::pair<std::string, std::string>> currentBatch_;
  size_t batchIndex_;
  size_t batchSize_;

  void loadNextBatch() {
    currentBatch_.clear();
    batchIndex_ = 0;

    if (exhausted_)
      return;

    // Compute batch range
    std::string batchStartKey = currentKey_;
    std::string batchEndKey = endKey_; // Could limit batch size here

    // Use RangeQueryExecutor for batch
    RangeQueryExecutor executor(lsmManager_);
    currentBatch_ = executor.executeSSTableRange(batchStartKey, batchEndKey);

    // Filter to only include keys after currentKey (if any)
    if (!currentBatch_.empty() && !currentKey_.empty()) {
      auto it =
          std::find_if(currentBatch_.begin(), currentBatch_.end(),
                       [this](const std::pair<std::string, std::string> &p) {
                         return p.first > currentKey_;
                       });
      if (it != currentBatch_.begin()) {
        currentBatch_.erase(currentBatch_.begin(), it);
      }
    }

    if (currentBatch_.empty()) {
      exhausted_ = true;
    }
  }

public:
  RangeQueryIterator(std::shared_ptr<lsm::LSMTreeManager> lsmManager,
                     const std::string &startKey, const std::string &endKey,
                     size_t batchSize = 10000)
      : lsmManager_(lsmManager), startKey_(startKey), endKey_(endKey),
        currentKey_(startKey), initialized_(false), exhausted_(false),
        batchIndex_(0), batchSize_(batchSize) {}

  bool hasNext() {
    if (!initialized_) {
      loadNextBatch();
      initialized_ = true;
    }
    return batchIndex_ < currentBatch_.size();
  }

  std::pair<std::string, std::string> next() {
    if (!hasNext()) {
      return {"", ""};
    }

    auto result = currentBatch_[batchIndex_++];
    currentKey_ = result.first;

    // Load next batch if this one is exhausted
    if (batchIndex_ >= currentBatch_.size()) {
      loadNextBatch();
    }

    return result;
  }

  void reset() {
    currentKey_ = startKey_;
    initialized_ = false;
    exhausted_ = false;
    currentBatch_.clear();
    batchIndex_ = 0;
  }
};

} // namespace query

#endif // RANGE_QUERY_EXECUTOR_HPP
