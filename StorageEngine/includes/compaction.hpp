// compaction.hpp - Compaction Scheduler and Merge Engine
#pragma once
#ifndef COMPACTION_HPP
#define COMPACTION_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "lsm_levels.hpp"
#include "skiplist.hpp"
#include "sstable.hpp"

namespace compaction {

// Types of compaction operations
enum class CompactionType {
  MINOR,              // Level L to L+1 (typical compaction)
  MAJOR,              // Full merge within a level
  COLUMNAR_CONVERSION // Convert row-oriented to columnar at deep levels
};

// A compaction job to be executed
struct CompactionJob {
  CompactionType type;
  int sourceLevel;
  int targetLevel;
  std::vector<lsm::SSTableMeta> inputSSTables;
  uint64_t scheduledTime;
  uint64_t priority; // Lower value = higher priority

  CompactionJob()
      : type(CompactionType::MINOR), sourceLevel(0), targetLevel(1),
        scheduledTime(0), priority(0) {}

  // For priority queue comparison (min-heap by priority)
  bool operator>(const CompactionJob &other) const {
    return priority > other.priority;
  }
};

// Statistics for a completed compaction
struct CompactionStats {
  uint64_t startTime;
  uint64_t endTime;
  size_t inputBytes;
  size_t outputBytes;
  size_t inputEntries;
  size_t outputEntries;
  size_t tombstonesRemoved;
  size_t duplicatesRemoved;

  CompactionStats()
      : startTime(0), endTime(0), inputBytes(0), outputBytes(0),
        inputEntries(0), outputEntries(0), tombstonesRemoved(0),
        duplicatesRemoved(0) {}

  double getDurationMs() const {
    return static_cast<double>(endTime - startTime);
  }

  double getSpaceAmplification() const {
    return inputBytes > 0 ? static_cast<double>(outputBytes) / inputBytes : 0.0;
  }
};

// Iterator for reading entries from an SSTable sequentially
class SSTableIterator {
private:
  std::ifstream file_;
  uint64_t currentOffset_;
  uint64_t dataEndOffset_;
  skiplist::Entry currentEntry_;
  bool valid_;
  std::string filePath_;

  void readNextEntry() {
    if (!file_.good() ||
        file_.tellg() >= static_cast<std::streampos>(dataEndOffset_)) {
      valid_ = false;
      return;
    }

    uint32_t keyLen;
    if (!file_.read(reinterpret_cast<char *>(&keyLen), sizeof(keyLen))) {
      valid_ = false;
      return;
    }

    std::string key(keyLen, '\0');
    file_.read(&key[0], keyLen);

    uint32_t valueLen;
    file_.read(reinterpret_cast<char *>(&valueLen), sizeof(valueLen));

    std::string value(valueLen, '\0');
    file_.read(&value[0], valueLen);

    uint64_t seq;
    bool isDeleted;
    file_.read(reinterpret_cast<char *>(&seq), sizeof(seq));
    file_.read(reinterpret_cast<char *>(&isDeleted), sizeof(isDeleted));

    if (file_.good()) {
      currentEntry_ = skiplist::Entry(key, value, seq, isDeleted);
      valid_ = true;
    } else {
      valid_ = false;
    }
  }

public:
  SSTableIterator(const std::string &filePath)
      : filePath_(filePath), valid_(false), dataEndOffset_(0) {

    file_.open(filePath, std::ios::binary);
    if (!file_.is_open()) {
      return;
    }

    // Read header to find data section bounds
    // Header format from sstable.hpp:
    // SSTableHeader: version(4) + minSeq(8) + maxSeq(8) + entryCount(4) +
    // indexCount(4)
    //                + indexOffset(8) + bloomOffset(8) + minKeyLen(4) +
    //                maxKeyLen(4)
    // Total = 52 bytes

    struct SSTableHeader {
      uint32_t version;
      uint64_t minSequence;
      uint64_t maxSequence;
      uint32_t entryCount;
      uint32_t indexCount;
      uint64_t indexOffset;
      uint64_t bloomFilterOffset;
      uint32_t minKeyLen;
      uint32_t maxKeyLen;
    };

    SSTableHeader header;
    file_.read(reinterpret_cast<char *>(&header), sizeof(header));

    // Skip minKeyLen and maxKeyLen (already in header struct)
    // Then skip the actual min and max key strings
    uint32_t minKeyLen, maxKeyLen;
    file_.read(reinterpret_cast<char *>(&minKeyLen), sizeof(minKeyLen));
    file_.read(reinterpret_cast<char *>(&maxKeyLen), sizeof(maxKeyLen));

    // Skip the key strings
    file_.seekg(minKeyLen + maxKeyLen, std::ios::cur);

    // Now we're at the start of data section
    dataEndOffset_ = header.indexOffset;

    // Read first entry
    readNextEntry();
  }

  bool isValid() const { return valid_; }

  const skiplist::Entry &current() const { return currentEntry_; }

  void next() {
    if (valid_) {
      readNextEntry();
    }
  }

  // Seek to the first entry >= key
  void seek(const std::string &key) {
    // For now, just scan from current position
    // A more efficient implementation would use the sparse index
    while (valid_ && currentEntry_.key < key) {
      next();
    }
  }

  const std::string &getFilePath() const { return filePath_; }
};

// Entry in the merge heap
struct HeapEntry {
  skiplist::Entry entry;
  int iteratorIndex; // Which iterator this came from

  // For min-heap: smaller key first, if equal keys then larger sequence first
  bool operator>(const HeapEntry &other) const {
    if (entry.key != other.entry.key) {
      return entry.key > other.entry.key;
    }
    // Same key: prefer higher sequence number (more recent)
    return entry.seq < other.entry.seq;
  }
};

// Multi-way merge engine for compaction
class MergeEngine {
private:
  std::vector<std::unique_ptr<SSTableIterator>> iterators_;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>,
                      std::greater<HeapEntry>>
      heap_;

  void initializeHeap() {
    for (size_t i = 0; i < iterators_.size(); ++i) {
      if (iterators_[i]->isValid()) {
        heap_.push({iterators_[i]->current(), static_cast<int>(i)});
      }
    }
  }

public:
  MergeEngine() = default;

  // Add an SSTable to the merge
  void addSSTable(const std::string &filePath) {
    auto iter = std::make_unique<SSTableIterator>(filePath);
    if (iter->isValid()) {
      iterators_.push_back(std::move(iter));
    }
  }

  // Add multiple SSTables
  void addSSTables(const std::vector<lsm::SSTableMeta> &tables) {
    for (const auto &meta : tables) {
      addSSTable(meta.filePath);
    }
    initializeHeap();
  }

  // Check if there are more entries
  bool hasNext() const { return !heap_.empty(); }

  // Get the next entry in sorted order
  // If there are duplicate keys, returns the one with highest sequence number
  skiplist::Entry next() {
    if (heap_.empty()) {
      return skiplist::Entry();
    }

    HeapEntry top = heap_.top();
    heap_.pop();

    // Skip duplicates (same key with lower sequence numbers)
    while (!heap_.empty() && heap_.top().entry.key == top.entry.key) {
      HeapEntry dup = heap_.top();
      heap_.pop();

      // Advance the iterator that had the duplicate
      iterators_[dup.iteratorIndex]->next();
      if (iterators_[dup.iteratorIndex]->isValid()) {
        heap_.push(
            {iterators_[dup.iteratorIndex]->current(), dup.iteratorIndex});
      }
    }

    // Advance the iterator we're returning from
    iterators_[top.iteratorIndex]->next();
    if (iterators_[top.iteratorIndex]->isValid()) {
      heap_.push({iterators_[top.iteratorIndex]->current(), top.iteratorIndex});
    }

    return top.entry;
  }

  // Merge all input SSTables into a new SSTable at the target path
  // Returns metadata for the new SSTable
  lsm::SSTableMeta
  mergeAndFlush(const std::vector<lsm::SSTableMeta> &inputs, int targetLevel,
                const std::string &outputPath,
                bool gcTombstones = false, // True for bottom-level compaction
                CompactionStats *stats = nullptr) {

    // Clear and add all input SSTables
    iterators_.clear();
    while (!heap_.empty())
      heap_.pop();

    addSSTables(inputs);

    lsm::SSTableMeta outputMeta;
    outputMeta.filePath = outputPath;
    outputMeta.level = targetLevel;
    outputMeta.creationTime =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    if (stats) {
      stats->startTime = outputMeta.creationTime;
      for (const auto &input : inputs) {
        stats->inputBytes += input.fileSize;
        stats->inputEntries += input.entryCount;
      }
    }

    // Collect all merged entries
    std::vector<std::pair<std::string, skiplist::Entry>> mergedEntries;

    while (hasNext()) {
      skiplist::Entry entry = next();

      // Optionally remove tombstones at the bottom level
      if (gcTombstones && entry.isDeleted) {
        if (stats)
          stats->tombstonesRemoved++;
        continue;
      }

      mergedEntries.emplace_back(entry.key, entry);

      // Track min/max keys
      if (mergedEntries.size() == 1) {
        outputMeta.minKey = entry.key;
        outputMeta.minSequence = entry.seq;
        outputMeta.maxSequence = entry.seq;
      }
      outputMeta.maxKey = entry.key;
      outputMeta.minSequence = std::min(outputMeta.minSequence, entry.seq);
      outputMeta.maxSequence = std::max(outputMeta.maxSequence, entry.seq);
    }

    outputMeta.entryCount = mergedEntries.size();

    // Write to new SSTable
    sstable::SSTable outputSSTable(outputPath);
    if (outputSSTable.create(mergedEntries)) {
      // Get file size
      outputMeta.fileSize = std::filesystem::file_size(outputPath);

      if (stats) {
        stats->endTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        stats->outputBytes = outputMeta.fileSize;
        stats->outputEntries = outputMeta.entryCount;
      }

      std::cout << "Merged " << inputs.size() << " SSTables into " << outputPath
                << " (" << outputMeta.entryCount << " entries, "
                << (outputMeta.fileSize / 1024.0) << " KB)" << std::endl;
    } else {
      std::cerr << "Failed to create merged SSTable: " << outputPath
                << std::endl;
    }

    return outputMeta;
  }
};

// Background compaction scheduler
class CompactionScheduler {
private:
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;

  std::priority_queue<CompactionJob, std::vector<CompactionJob>,
                      std::greater<CompactionJob>>
      pendingJobs_;
  std::mutex jobsMutex_;

  std::thread compactionThread_;
  std::condition_variable cv_;
  std::atomic<bool> running_{false};
  std::atomic<bool> paused_{false};

  // Compaction statistics
  std::atomic<uint64_t> totalCompactions_{0};
  std::atomic<uint64_t> totalBytesCompacted_{0};

  // Calculate priority for a compaction job
  uint64_t calculatePriority(int level, size_t numTables) {
    // Lower level = higher priority (process L0 first)
    // More tables = higher priority
    return static_cast<uint64_t>(level) * 1000 +
           (100 - std::min(numTables, size_t(100)));
  }

  void compactionWorker() {
    while (running_) {
      CompactionJob job;
      {
        std::unique_lock<std::mutex> lock(jobsMutex_);
        cv_.wait(lock, [this] {
          return !running_ || (!paused_ && !pendingJobs_.empty());
        });

        if (!running_)
          break;
        if (pendingJobs_.empty())
          continue;

        job = pendingJobs_.top();
        pendingJobs_.pop();
      }

      // Execute compaction
      executeCompaction(job);

      // Check if more compactions are needed
      checkAndSchedule();
    }
  }

  void executeCompaction(const CompactionJob &job) {
    std::cout << "Starting compaction: Level " << job.sourceLevel
              << " -> Level " << job.targetLevel << " ("
              << job.inputSSTables.size() << " SSTables)" << std::endl;

    // Find overlapping SSTables in target level
    auto overlapping = lsmManager_->findOverlappingSSTables(job.targetLevel,
                                                            job.inputSSTables);

    // Combine input and overlapping tables for merge
    std::vector<lsm::SSTableMeta> allInputs = job.inputSSTables;
    allInputs.insert(allInputs.end(), overlapping.begin(), overlapping.end());

    // Generate output path
    std::string outputPath = lsmManager_->generateSSTablePath(job.targetLevel);

    // Perform merge
    MergeEngine mergeEngine;
    CompactionStats stats;
    bool gcTombstones =
        (job.targetLevel == lsmManager_->getConfig().maxLevels - 1);

    lsm::SSTableMeta outputMeta = mergeEngine.mergeAndFlush(
        allInputs, job.targetLevel, outputPath, gcTombstones, &stats);

    if (outputMeta.entryCount > 0) {
      // Remove old SSTables from LSM manager
      for (const auto &input : job.inputSSTables) {
        lsmManager_->removeSSTable(input.filePath);
        // Delete the file
        std::filesystem::remove(input.filePath);
      }
      for (const auto &overlap : overlapping) {
        lsmManager_->removeSSTable(overlap.filePath);
        std::filesystem::remove(overlap.filePath);
      }

      // Add new SSTable to LSM manager
      lsmManager_->addSSTable(outputMeta);

      totalCompactions_++;
      totalBytesCompacted_ += stats.inputBytes;

      std::cout << "Compaction completed in " << stats.getDurationMs() << "ms"
                << ", space ratio: " << stats.getSpaceAmplification()
                << std::endl;
    }
  }

public:
  explicit CompactionScheduler(std::shared_ptr<lsm::LSMTreeManager> lsmManager)
      : lsmManager_(lsmManager) {}

  ~CompactionScheduler() { stop(); }

  void start() {
    if (running_)
      return;

    running_ = true;
    compactionThread_ =
        std::thread(&CompactionScheduler::compactionWorker, this);
  }

  void stop() {
    if (!running_)
      return;

    running_ = false;
    cv_.notify_all();

    if (compactionThread_.joinable()) {
      compactionThread_.join();
    }
  }

  void pause() { paused_ = true; }

  void resume() {
    paused_ = false;
    cv_.notify_one();
  }

  // Schedule compaction for a specific level
  void scheduleCompaction(int level) {
    if (level < 0 || level >= lsmManager_->getConfig().maxLevels - 1) {
      return;
    }

    auto candidates = lsmManager_->selectCompactionCandidates(level);
    if (candidates.empty()) {
      return;
    }

    CompactionJob job;
    job.type = lsmManager_->isColumnarLevel(level + 1)
                   ? CompactionType::COLUMNAR_CONVERSION
                   : CompactionType::MINOR;
    job.sourceLevel = level;
    job.targetLevel = level + 1;
    job.inputSSTables = candidates;
    job.scheduledTime = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    job.priority = calculatePriority(level, candidates.size());

    {
      std::lock_guard<std::mutex> lock(jobsMutex_);
      pendingJobs_.push(job);
    }

    cv_.notify_one();
  }

  // Check all levels and schedule compaction if needed
  void checkAndSchedule() {
    for (int level = 0; level < lsmManager_->getConfig().maxLevels - 1;
         ++level) {
      if (lsmManager_->shouldTriggerCompaction(level)) {
        scheduleCompaction(level);
      }
    }
  }

  // Force immediate compaction check
  void triggerCheck() { checkAndSchedule(); }

  // Get statistics
  uint64_t getTotalCompactions() const { return totalCompactions_; }
  uint64_t getTotalBytesCompacted() const { return totalBytesCompacted_; }

  size_t getPendingJobCount() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(jobsMutex_));
    return pendingJobs_.size();
  }
};

} // namespace compaction

#endif // COMPACTION_HPP
