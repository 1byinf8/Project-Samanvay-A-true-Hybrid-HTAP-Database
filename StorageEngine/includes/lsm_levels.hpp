// lsm_levels.hpp - LSM-Tree Level Manager for SSTable lifecycle management
#pragma once
#ifndef LSM_LEVELS_HPP
#define LSM_LEVELS_HPP

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>
#include <cmath>

namespace lsm
{

  // Metadata for a single SSTable
  struct SSTableMeta
  {
    std::string filePath;
    std::string minKey;
    std::string maxKey;
    uint64_t minSequence;
    uint64_t maxSequence;
    size_t fileSize;
    size_t entryCount;
    int level; // 0 = freshest from memtable flush
    uint64_t creationTime;
    bool isColumnar; // True if this is a columnar format file

    SSTableMeta()
        : minSequence(0), maxSequence(0), fileSize(0), entryCount(0), level(0),
          creationTime(0), isColumnar(false) {}

    SSTableMeta(const std::string &path, const std::string &minK,
                const std::string &maxK, uint64_t minSeq, uint64_t maxSeq,
                size_t fSize, size_t eCount, int lvl, uint64_t cTime,
                bool columnar = false)
        : filePath(path), minKey(minK), maxKey(maxK), minSequence(minSeq),
          maxSequence(maxSeq), fileSize(fSize), entryCount(eCount), level(lvl),
          creationTime(cTime), isColumnar(columnar) {}

    // Check if this SSTable might contain the given key
    bool mayContainKey(const std::string &key) const
    {
      return key >= minKey && key <= maxKey;
    }

    // Check if this SSTable overlaps with a key range
    bool overlapsRange(const std::string &startKey,
                       const std::string &endKey) const
    {
      return !(endKey < minKey || startKey > maxKey);
    }

    // Check if this SSTable overlaps with another SSTable
    bool overlapsWith(const SSTableMeta &other) const
    {
      return overlapsRange(other.minKey, other.maxKey);
    }
  };

  // Statistics for a single level
  struct LevelStats
  {
    int level;
    size_t totalSize;
    size_t numSSTables;
    size_t totalEntries;

    LevelStats() : level(0), totalSize(0), numSSTables(0), totalEntries(0) {}

    LevelStats(int lvl)
        : level(lvl), totalSize(0), numSSTables(0), totalEntries(0) {}
  };

  // Configuration for LSM-tree
  struct LSMConfig
  {
    size_t level0CompactionThreshold; // Trigger compaction when L0 has this many
                                      // SSTables
    size_t level0SizeLimit;           // Size limit for Level 0 (bytes)
    double levelSizeMultiplier;       // Each level is this times larger than previous
    int maxLevels;                    // Maximum number of levels (0 to maxLevels-1)
    int columnarLevelThreshold;       // Levels >= this use columnar format
    std::string dataDirectory;        // Directory for SSTable files

    LSMConfig()
        : level0CompactionThreshold(4), level0SizeLimit(64 * 1024 * 1024), // 64MB
          levelSizeMultiplier(10.0), maxLevels(7), columnarLevelThreshold(4),
          dataDirectory("./data")
    {
    }
  };

  class LSMTreeManager
  {
  private:
    LSMConfig config_;

    // SSTables organized by level: levels_[i] contains all SSTables at level i
    std::vector<std::vector<SSTableMeta>> levels_;

    // Read-write lock for concurrent access
    mutable std::shared_mutex levelsMutex_;

    // Atomic counter for generating unique SSTable IDs
    std::atomic<uint64_t> nextSSTableId_{0};

    // Calculate size limit for a given level
    size_t getLevelSizeLimit(int level) const
    {
      if (level == 0)
      {
        return config_.level0SizeLimit;
      }
      return static_cast<size_t>(config_.level0SizeLimit *
                                 std::pow(config_.levelSizeMultiplier, level));
    }

  public:
    explicit LSMTreeManager(const LSMConfig &config = LSMConfig())
        : config_(config), levels_(config.maxLevels)
    {

      // Create data directory if it doesn't exist
      std::filesystem::create_directories(config_.dataDirectory);
    }

    // Get configuration
    const LSMConfig &getConfig() const { return config_; }

    // Generate a unique filename for a new SSTable
    std::string generateSSTablePath(int level)
    {
      uint64_t id = nextSSTableId_.fetch_add(1);
      std::string extension =
          (level >= config_.columnarLevelThreshold) ? ".col" : ".sst";
      return config_.dataDirectory + "/L" + std::to_string(level) + "_" +
             std::to_string(id) + extension;
    }

    // Add a new SSTable to the specified level
    void addSSTable(const SSTableMeta &meta)
    {
      std::unique_lock lock(levelsMutex_);

      if (meta.level < 0 || meta.level >= config_.maxLevels)
      {
        std::cerr << "Invalid level " << meta.level << " for SSTable"
                  << std::endl;
        return;
      }

      levels_[meta.level].push_back(meta);

      std::cout << "Added SSTable to Level " << meta.level << ": "
                << meta.filePath << " (keys: " << meta.minKey << " - "
                << meta.maxKey << ", entries: " << meta.entryCount << ")"
                << std::endl;
    }

    // Remove an SSTable (typically after compaction)
    bool removeSSTable(const std::string &filePath)
    {
      std::unique_lock lock(levelsMutex_);

      for (auto &level : levels_)
      {
        auto it = std::find_if(level.begin(), level.end(),
                               [&filePath](const SSTableMeta &meta)
                               {
                                 return meta.filePath == filePath;
                               });

        if (it != level.end())
        {
          level.erase(it);
          return true;
        }
      }
      return false;
    }

    // Get all SSTables that might contain the given key
    // Returns in order from newest to oldest (Level 0 first, then by creation
    // time)
    std::vector<SSTableMeta> getSSTablesForKey(const std::string &key) const
    {
      std::shared_lock lock(levelsMutex_);

      std::vector<SSTableMeta> result;

      // Search Level 0 (may have overlapping key ranges)
      for (const auto &meta : levels_[0])
      {
        if (meta.mayContainKey(key))
        {
          result.push_back(meta);
        }
      }

      // Sort Level 0 results by creation time (newest first)
      std::sort(result.begin(), result.end(),
                [](const SSTableMeta &a, const SSTableMeta &b)
                {
                  return a.creationTime > b.creationTime;
                });

      // Search other levels (non-overlapping, so at most one match per level)
      for (int level = 1; level < config_.maxLevels; ++level)
      {
        for (const auto &meta : levels_[level])
        {
          if (meta.mayContainKey(key))
          {
            result.push_back(meta);
            break; // Only one match per level since they don't overlap
          }
        }
      }

      return result;
    }

    // Get all SSTables that overlap with the given key range
    std::vector<SSTableMeta>
    getSSTablesForRange(const std::string &startKey,
                        const std::string &endKey) const
    {
      std::shared_lock lock(levelsMutex_);

      std::vector<SSTableMeta> result;

      for (int level = 0; level < config_.maxLevels; ++level)
      {
        for (const auto &meta : levels_[level])
        {
          if (meta.overlapsRange(startKey, endKey))
          {
            result.push_back(meta);
          }
        }
      }

      // Sort by level first, then by creation time within each level
      std::sort(result.begin(), result.end(),
                [](const SSTableMeta &a, const SSTableMeta &b)
                {
                  if (a.level != b.level)
                    return a.level < b.level;
                  return a.creationTime > b.creationTime;
                });

      return result;
    }

    // Get all SSTables at a specific level
    std::vector<SSTableMeta> getSSTablesAtLevel(int level) const
    {
      std::shared_lock lock(levelsMutex_);

      if (level < 0 || level >= config_.maxLevels)
      {
        return {};
      }

      return levels_[level];
    }

    // Check if compaction should be triggered for a level
    bool shouldTriggerCompaction(int level) const
    {
      std::shared_lock lock(levelsMutex_);

      if (level < 0 || level >= config_.maxLevels - 1)
      {
        return false; // Can't compact the last level further
      }

      if (level == 0)
      {
        // Level 0: trigger based on number of SSTables
        return levels_[0].size() >= config_.level0CompactionThreshold;
      }

      // Other levels: trigger based on total size
      size_t totalSize = 0;
      for (const auto &meta : levels_[level])
      {
        totalSize += meta.fileSize;
      }

      return totalSize > getLevelSizeLimit(level);
    }

    // Select SSTables for compaction from a level
    // For Level 0: select all (they may overlap)
    // For other levels: select oldest or those overlapping with chosen L0 files
    std::vector<SSTableMeta> selectCompactionCandidates(int level) const
    {
      std::shared_lock lock(levelsMutex_);

      if (level < 0 || level >= config_.maxLevels)
      {
        return {};
      }

      if (level == 0)
      {
        // For Level 0, select all SSTables for compaction
        return levels_[0];
      }

      // For other levels, select the oldest SSTables until we hit a threshold
      std::vector<SSTableMeta> candidates = levels_[level];

      // Sort by creation time (oldest first)
      std::sort(candidates.begin(), candidates.end(),
                [](const SSTableMeta &a, const SSTableMeta &b)
                {
                  return a.creationTime < b.creationTime;
                });

      // Take enough to reduce to below limit
      size_t targetSize = getLevelSizeLimit(level) / 2;
      size_t currentSize = 0;

      std::vector<SSTableMeta> selected;
      for (const auto &meta : candidates)
      {
        selected.push_back(meta);
        currentSize += meta.fileSize;
        if (currentSize >= targetSize)
        {
          break;
        }
      }

      return selected;
    }

    // Find overlapping SSTables in the target level for the given source tables
    std::vector<SSTableMeta>
    findOverlappingSSTables(int targetLevel,
                            const std::vector<SSTableMeta> &sourceTables) const
    {

      std::shared_lock lock(levelsMutex_);

      if (targetLevel < 0 || targetLevel >= config_.maxLevels)
      {
        return {};
      }

      // Compute the overall key range of source tables
      std::string minKey, maxKey;
      bool first = true;
      for (const auto &meta : sourceTables)
      {
        if (first || meta.minKey < minKey)
          minKey = meta.minKey;
        if (first || meta.maxKey > maxKey)
          maxKey = meta.maxKey;
        first = false;
      }

      // Find all SSTables in target level that overlap this range
      std::vector<SSTableMeta> overlapping;
      for (const auto &meta : levels_[targetLevel])
      {
        if (meta.overlapsRange(minKey, maxKey))
        {
          overlapping.push_back(meta);
        }
      }

      return overlapping;
    }

    // Get statistics for a level
    LevelStats getLevelStats(int level) const
    {
      std::shared_lock lock(levelsMutex_);

      LevelStats stats(level);

      if (level >= 0 && level < config_.maxLevels)
      {
        stats.numSSTables = levels_[level].size();
        for (const auto &meta : levels_[level])
        {
          stats.totalSize += meta.fileSize;
          stats.totalEntries += meta.entryCount;
        }
      }

      return stats;
    }

    // Get statistics for all levels
    std::vector<LevelStats> getAllLevelStats() const
    {
      std::vector<LevelStats> allStats;
      for (int i = 0; i < config_.maxLevels; ++i)
      {
        allStats.push_back(getLevelStats(i));
      }
      return allStats;
    }

    // Check if a level uses columnar format
    bool isColumnarLevel(int level) const
    {
      return level >= config_.columnarLevelThreshold;
    }

    // Get the total number of SSTables across all levels
    size_t getTotalSSTableCount() const
    {
      std::shared_lock lock(levelsMutex_);

      size_t count = 0;
      for (const auto &level : levels_)
      {
        count += level.size();
      }
      return count;
    }

    // Get the total size of all SSTables
    size_t getTotalSize() const
    {
      std::shared_lock lock(levelsMutex_);

      size_t totalSize = 0;
      for (const auto &level : levels_)
      {
        for (const auto &meta : level)
        {
          totalSize += meta.fileSize;
        }
      }
      return totalSize;
    }

    // Persist the LSM-tree metadata to disk (for crash recovery)
    bool persistMetadata(const std::string &metaFilePath) const
    {
      std::shared_lock lock(levelsMutex_);

      std::ofstream file(metaFilePath, std::ios::binary);
      if (!file.is_open())
      {
        return false;
      }

      // Write number of levels
      int numLevels = config_.maxLevels;
      file.write(reinterpret_cast<const char *>(&numLevels), sizeof(numLevels));

      // Write each level
      for (int level = 0; level < numLevels; ++level)
      {
        size_t numTables = levels_[level].size();
        file.write(reinterpret_cast<const char *>(&numTables), sizeof(numTables));

        for (const auto &meta : levels_[level])
        {
          // Write file path
          size_t pathLen = meta.filePath.size();
          file.write(reinterpret_cast<const char *>(&pathLen), sizeof(pathLen));
          file.write(meta.filePath.c_str(), pathLen);

          // Write min/max keys
          size_t minKeyLen = meta.minKey.size();
          file.write(reinterpret_cast<const char *>(&minKeyLen),
                     sizeof(minKeyLen));
          file.write(meta.minKey.c_str(), minKeyLen);

          size_t maxKeyLen = meta.maxKey.size();
          file.write(reinterpret_cast<const char *>(&maxKeyLen),
                     sizeof(maxKeyLen));
          file.write(meta.maxKey.c_str(), maxKeyLen);

          // Write numeric fields
          file.write(reinterpret_cast<const char *>(&meta.minSequence),
                     sizeof(meta.minSequence));
          file.write(reinterpret_cast<const char *>(&meta.maxSequence),
                     sizeof(meta.maxSequence));
          file.write(reinterpret_cast<const char *>(&meta.fileSize),
                     sizeof(meta.fileSize));
          file.write(reinterpret_cast<const char *>(&meta.entryCount),
                     sizeof(meta.entryCount));
          file.write(reinterpret_cast<const char *>(&meta.level),
                     sizeof(meta.level));
          file.write(reinterpret_cast<const char *>(&meta.creationTime),
                     sizeof(meta.creationTime));
          file.write(reinterpret_cast<const char *>(&meta.isColumnar),
                     sizeof(meta.isColumnar));
        }
      }

      file.close();
      return true;
    }

    // Load LSM-tree metadata from disk (for crash recovery)
    bool loadMetadata(const std::string &metaFilePath)
    {
      std::unique_lock lock(levelsMutex_);

      std::ifstream file(metaFilePath, std::ios::binary);
      if (!file.is_open())
      {
        return false;
      }

      // Read number of levels
      int numLevels;
      file.read(reinterpret_cast<char *>(&numLevels), sizeof(numLevels));

      if (numLevels != config_.maxLevels)
      {
        std::cerr << "Metadata level count mismatch" << std::endl;
        return false;
      }

      // Clear existing levels
      for (auto &level : levels_)
      {
        level.clear();
      }

      // Read each level
      for (int level = 0; level < numLevels; ++level)
      {
        size_t numTables;
        file.read(reinterpret_cast<char *>(&numTables), sizeof(numTables));

        for (size_t i = 0; i < numTables; ++i)
        {
          SSTableMeta meta;

          // Read file path
          size_t pathLen;
          file.read(reinterpret_cast<char *>(&pathLen), sizeof(pathLen));
          meta.filePath.resize(pathLen);
          file.read(&meta.filePath[0], pathLen);

          // Read min/max keys
          size_t minKeyLen;
          file.read(reinterpret_cast<char *>(&minKeyLen), sizeof(minKeyLen));
          meta.minKey.resize(minKeyLen);
          file.read(&meta.minKey[0], minKeyLen);

          size_t maxKeyLen;
          file.read(reinterpret_cast<char *>(&maxKeyLen), sizeof(maxKeyLen));
          meta.maxKey.resize(maxKeyLen);
          file.read(&meta.maxKey[0], maxKeyLen);

          // Read numeric fields
          file.read(reinterpret_cast<char *>(&meta.minSequence),
                    sizeof(meta.minSequence));
          file.read(reinterpret_cast<char *>(&meta.maxSequence),
                    sizeof(meta.maxSequence));
          file.read(reinterpret_cast<char *>(&meta.fileSize),
                    sizeof(meta.fileSize));
          file.read(reinterpret_cast<char *>(&meta.entryCount),
                    sizeof(meta.entryCount));
          file.read(reinterpret_cast<char *>(&meta.level), sizeof(meta.level));
          file.read(reinterpret_cast<char *>(&meta.creationTime),
                    sizeof(meta.creationTime));
          file.read(reinterpret_cast<char *>(&meta.isColumnar),
                    sizeof(meta.isColumnar));

          levels_[level].push_back(meta);
        }
      }

      file.close();
      return true;
    }

    // Print debug information about all levels
    void printDebugInfo() const
    {
      std::shared_lock lock(levelsMutex_);

      std::cout << "=== LSM-Tree Level Status ===" << std::endl;
      for (int level = 0; level < config_.maxLevels; ++level)
      {
        size_t totalSize = 0;
        for (const auto &meta : levels_[level])
        {
          totalSize += meta.fileSize;
        }

        std::cout << "Level " << level << " ["
                  << (isColumnarLevel(level) ? "COLUMNAR" : "ROW")
                  << "]: " << levels_[level].size() << " SSTables, "
                  << (totalSize / (1024.0 * 1024.0)) << " MB"
                  << " (limit: " << (getLevelSizeLimit(level) / (1024.0 * 1024.0))
                  << " MB)" << std::endl;
      }
      std::cout << "=============================" << std::endl;
    }
  };

} // namespace lsm

#endif // LSM_LEVELS_HPP
