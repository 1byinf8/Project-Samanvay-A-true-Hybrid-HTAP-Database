// wal.hpp - Fixed version
#pragma once
#ifndef WAL_HPP
#define WAL_HPP

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace wal {

// CRC32 polynomial (IEEE 802.3)
constexpr uint32_t CRC32_POLYNOMIAL = 0xEDB88320;

// Calculate CRC32 checksum for corruption detection
inline uint32_t calculateCRC32(const uint8_t *data, size_t length) {
  uint32_t crc = 0xFFFFFFFF;
  for (size_t i = 0; i < length; ++i) {
    crc ^= data[i];
    for (int j = 0; j < 8; ++j) {
      crc = (crc >> 1) ^ ((crc & 1) ? CRC32_POLYNOMIAL : 0);
    }
  }
  return ~crc;
}

enum class Operation : uint8_t { INSERT = 1, DELETE = 2 };

struct WALEntry {
  uint64_t sequenceNumber;
  uint64_t timestamp;
  Operation operation;
  std::string key;
  std::string value;

  WALEntry(uint64_t seq, uint64_t ts, Operation op, const std::string &k,
           const std::string &v)
      : sequenceNumber(seq), timestamp(ts), operation(op), key(k), value(v) {}
};

class WAL {
private:
  std::ofstream logFile;
  std::mutex writeMutex;
  std::string filePath;
  uint64_t currentOffset;

  // Serialize entry to binary format
  std::vector<uint8_t> serialize(const WALEntry &entry) {
    std::vector<uint8_t> buffer;

    // Write sequence number (8 bytes)
    uint64_t seq = entry.sequenceNumber;
    for (int i = 0; i < 8; ++i) {
      buffer.push_back(seq & 0xFF);
      seq >>= 8;
    }

    // Write timestamp (8 bytes)
    uint64_t ts = entry.timestamp;
    for (int i = 0; i < 8; ++i) {
      buffer.push_back(ts & 0xFF);
      ts >>= 8;
    }

    // Write operation (1 byte)
    buffer.push_back(static_cast<uint8_t>(entry.operation));

    // Write key length (4 bytes) and key
    uint32_t keyLen = entry.key.size();
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(keyLen & 0xFF);
      keyLen >>= 8;
    }
    for (char c : entry.key) {
      buffer.push_back(static_cast<uint8_t>(c));
    }

    // Write value length (4 bytes) and value
    uint32_t valueLen = entry.value.size();
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(valueLen & 0xFF);
      valueLen >>= 8;
    }
    for (char c : entry.value) {
      buffer.push_back(static_cast<uint8_t>(c));
    }

    // Calculate and append CRC32 checksum (4 bytes)
    uint32_t crc = calculateCRC32(buffer.data(), buffer.size());
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(crc & 0xFF);
      crc >>= 8;
    }

    return buffer;
  }

  uint64_t getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

public:
  WAL(const std::string &path) : filePath(path), currentOffset(0) {
    logFile.open(filePath, std::ios::binary | std::ios::app);
    if (!logFile.is_open()) {
      throw std::runtime_error("Failed to open WAL file: " + filePath);
    }
  }

  ~WAL() {
    if (logFile.is_open()) {
      logFile.close();
    }
  }

  bool append(uint64_t sequenceNumber, Operation op, const std::string &key,
              const std::string &value) {
    std::lock_guard<std::mutex> lock(writeMutex);

    WALEntry entry(sequenceNumber, getCurrentTimestamp(), op, key, value);
    auto buffer = serialize(entry);

    // Write entry size first (for recovery)
    uint32_t entrySize = buffer.size();
    logFile.write(reinterpret_cast<const char *>(&entrySize),
                  sizeof(entrySize));

    // Write the entry
    logFile.write(reinterpret_cast<const char *>(buffer.data()), buffer.size());

    // Force write to disk for durability
    logFile.flush();

    currentOffset += sizeof(entrySize) + buffer.size();
    return logFile.good();
  }

  void sync() {
    std::lock_guard<std::mutex> lock(writeMutex);
    // Use flush() instead of sync() which doesn't exist on all platforms
    logFile.flush();
  }

  void truncate(uint64_t upToSequence) {
    std::lock_guard<std::mutex> lock(writeMutex);

    // Close current log file
    logFile.close();

    // Read all entries
    std::vector<WALEntry> entriesToKeep;
    {
      std::ifstream readFile(filePath, std::ios::binary);
      if (readFile.is_open()) {
        while (readFile.good() && readFile.peek() != EOF) {
          // Read entry size
          uint32_t entrySize;
          if (!readFile.read(reinterpret_cast<char *>(&entrySize),
                             sizeof(entrySize))) {
            break;
          }

          // Read entry data
          std::vector<uint8_t> buffer(entrySize);
          if (!readFile.read(reinterpret_cast<char *>(buffer.data()),
                             entrySize)) {
            break;
          }

          // Deserialize entry
          size_t offset = 0;

          // Read sequence number
          uint64_t seq = 0;
          for (int i = 0; i < 8; ++i) {
            seq |= static_cast<uint64_t>(buffer[offset++]) << (i * 8);
          }

          // Only keep entries with sequence > upToSequence
          if (seq > upToSequence) {
            // Read timestamp
            uint64_t ts = 0;
            for (int i = 0; i < 8; ++i) {
              ts |= static_cast<uint64_t>(buffer[offset++]) << (i * 8);
            }

            // Read operation
            Operation op = static_cast<Operation>(buffer[offset++]);

            // Read key
            uint32_t keyLen = 0;
            for (int i = 0; i < 4; ++i) {
              keyLen |= static_cast<uint32_t>(buffer[offset++]) << (i * 8);
            }
            std::string key(reinterpret_cast<char *>(&buffer[offset]), keyLen);
            offset += keyLen;

            // Read value
            uint32_t valueLen = 0;
            for (int i = 0; i < 4; ++i) {
              valueLen |= static_cast<uint32_t>(buffer[offset++]) << (i * 8);
            }
            std::string value(reinterpret_cast<char *>(&buffer[offset]),
                              valueLen);

            entriesToKeep.emplace_back(seq, ts, op, key, value);
          }
        }
        readFile.close();
      }
    }

    // Write a new WAL file with only the entries to keep
    std::string tempPath = filePath + ".tmp";
    {
      std::ofstream tempFile(tempPath, std::ios::binary | std::ios::trunc);
      if (tempFile.is_open()) {
        for (const auto &entry : entriesToKeep) {
          auto buffer = serialize(entry);
          uint32_t entrySize = buffer.size();
          tempFile.write(reinterpret_cast<const char *>(&entrySize),
                         sizeof(entrySize));
          tempFile.write(reinterpret_cast<const char *>(buffer.data()),
                         buffer.size());
        }
        tempFile.close();
      }
    }

    // Replace old file with new file
    std::remove(filePath.c_str());
    std::rename(tempPath.c_str(), filePath.c_str());

    // Reopen log file for appending
    logFile.open(filePath, std::ios::binary | std::ios::app);
    currentOffset = 0;
    for (const auto &entry : entriesToKeep) {
      currentOffset += sizeof(uint32_t) + serialize(entry).size();
    }
  }

  std::vector<WALEntry> recover() {
    std::vector<WALEntry> entries;
    std::ifstream recoveryFile(filePath, std::ios::binary);
    size_t corruptedEntries = 0;

    if (!recoveryFile.is_open()) {
      return entries; // No existing WAL file
    }

    while (recoveryFile.good() && recoveryFile.peek() != EOF) {
      // Read entry size
      uint32_t entrySize;
      if (!recoveryFile.read(reinterpret_cast<char *>(&entrySize),
                             sizeof(entrySize))) {
        break;
      }

      // Check for reasonable entry size (avoid reading garbage)
      // Must be at least 4 bytes for CRC + minimal data
      if (entrySize < 4 || entrySize > 10 * 1024 * 1024) {
        break;
      }

      // Read entry data
      std::vector<uint8_t> buffer(entrySize);
      if (!recoveryFile.read(reinterpret_cast<char *>(buffer.data()),
                             entrySize)) {
        break;
      }

      // Validate CRC32 checksum (last 4 bytes of buffer)
      if (entrySize < 4) {
        corruptedEntries++;
        continue;
      }

      size_t dataLen = entrySize - 4; // Exclude CRC from data
      uint32_t storedCRC = 0;
      for (int i = 0; i < 4; ++i) {
        storedCRC |= static_cast<uint32_t>(buffer[dataLen + i]) << (i * 8);
      }

      uint32_t calculatedCRC = calculateCRC32(buffer.data(), dataLen);
      if (storedCRC != calculatedCRC) {
        corruptedEntries++;
        std::cerr << "WAL Recovery: Skipping corrupted entry (CRC mismatch)"
                  << std::endl;
        continue; // Skip this corrupted entry
      }

      // Deserialize entry (excluding CRC bytes)
      size_t offset = 0;

      // Read sequence number (8 bytes, little endian)
      uint64_t seq = 0;
      for (int i = 0; i < 8; ++i) {
        seq |= static_cast<uint64_t>(buffer[offset++]) << (i * 8);
      }

      // Read timestamp (8 bytes, little endian)
      uint64_t ts = 0;
      for (int i = 0; i < 8; ++i) {
        ts |= static_cast<uint64_t>(buffer[offset++]) << (i * 8);
      }

      // Read operation (1 byte)
      Operation op = static_cast<Operation>(buffer[offset++]);

      // Read key length (4 bytes, little endian)
      uint32_t keyLen = 0;
      for (int i = 0; i < 4; ++i) {
        keyLen |= static_cast<uint32_t>(buffer[offset++]) << (i * 8);
      }

      // Validate key length
      if (keyLen > dataLen - offset) {
        corruptedEntries++;
        continue; // Corrupted entry
      }

      std::string key(reinterpret_cast<char *>(&buffer[offset]), keyLen);
      offset += keyLen;

      // Read value length (4 bytes, little endian)
      uint32_t valueLen = 0;
      for (int i = 0; i < 4; ++i) {
        valueLen |= static_cast<uint32_t>(buffer[offset++]) << (i * 8);
      }

      // Validate value length
      if (valueLen > dataLen - offset) {
        corruptedEntries++;
        continue; // Corrupted entry
      }

      std::string value(reinterpret_cast<char *>(&buffer[offset]), valueLen);

      entries.emplace_back(seq, ts, op, key, value);
    }

    recoveryFile.close();

    if (corruptedEntries > 0) {
      std::cerr << "WAL Recovery: Skipped " << corruptedEntries
                << " corrupted entries" << std::endl;
    }

    // Sort by sequence number to ensure correct replay order
    std::sort(entries.begin(), entries.end(),
              [](const WALEntry &a, const WALEntry &b) {
                return a.sequenceNumber < b.sequenceNumber;
              });

    return entries;
  }

  // Get the highest sequence number in the WAL
  uint64_t getMaxSequence() {
    auto entries = recover();
    uint64_t maxSeq = 0;
    for (const auto &entry : entries) {
      maxSeq = std::max(maxSeq, entry.sequenceNumber);
    }
    return maxSeq;
  }

  // Get file size
  uint64_t getFileSize() const { return currentOffset; }

  void createCheckpoint(const std::string &checkpointPath) {
    std::lock_guard<std::mutex> lock(writeMutex);

    // Flush current WAL
    logFile.flush();

    // Copy WAL to checkpoint location
    std::ifstream src(filePath, std::ios::binary);
    std::ofstream dst(checkpointPath, std::ios::binary | std::ios::trunc);

    if (src.is_open() && dst.is_open()) {
      dst << src.rdbuf();
    }
  }
};

// ==================== Group Commit WAL ====================
// High-performance WAL with batched writes for improved throughput

enum class FlushMode {
  IMMEDIATE, // Flush after every write (safest, slowest)
  BATCHED,   // Flush when batch is full or on explicit sync
  TIMED      // Flush at regular intervals (background thread)
};

struct GroupCommitConfig {
  FlushMode mode = FlushMode::TIMED;
  size_t batchSize = 100;                      // Max entries before flush
  size_t batchBytes = 64 * 1024;               // Max bytes before flush (64KB)
  std::chrono::milliseconds flushInterval{10}; // Flush interval for TIMED mode
};

class GroupCommitWAL {
private:
  std::ofstream logFile_;
  std::string filePath_;
  GroupCommitConfig config_;

  // Write buffer for batching
  std::vector<std::vector<uint8_t>> pendingWrites_;
  size_t pendingBytes_ = 0;
  mutable std::mutex bufferMutex_;

  // Background flush thread (for TIMED mode)
  std::thread flushThread_;
  std::atomic<bool> running_{false};
  std::condition_variable flushCond_;
  std::mutex flushCondMutex_;

  // Statistics
  std::atomic<uint64_t> totalWrites_{0};
  std::atomic<uint64_t> totalFlushes_{0};
  std::atomic<uint64_t> totalBytesWritten_{0};

  uint64_t getCurrentTimestamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }

  std::vector<uint8_t> serialize(const WALEntry &entry) {
    std::vector<uint8_t> buffer;

    // Same serialization as WAL class
    uint64_t seq = entry.sequenceNumber;
    for (int i = 0; i < 8; ++i) {
      buffer.push_back(seq & 0xFF);
      seq >>= 8;
    }

    uint64_t ts = entry.timestamp;
    for (int i = 0; i < 8; ++i) {
      buffer.push_back(ts & 0xFF);
      ts >>= 8;
    }

    buffer.push_back(static_cast<uint8_t>(entry.operation));

    uint32_t keyLen = entry.key.size();
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(keyLen & 0xFF);
      keyLen >>= 8;
    }
    for (char c : entry.key) {
      buffer.push_back(static_cast<uint8_t>(c));
    }

    uint32_t valueLen = entry.value.size();
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(valueLen & 0xFF);
      valueLen >>= 8;
    }
    for (char c : entry.value) {
      buffer.push_back(static_cast<uint8_t>(c));
    }

    // CRC32 checksum
    uint32_t crc = calculateCRC32(buffer.data(), buffer.size());
    for (int i = 0; i < 4; ++i) {
      buffer.push_back(crc & 0xFF);
      crc >>= 8;
    }

    return buffer;
  }

  void flushBuffer() {
    std::vector<std::vector<uint8_t>> toWrite;

    {
      std::lock_guard<std::mutex> lock(bufferMutex_);
      if (pendingWrites_.empty())
        return;
      toWrite.swap(pendingWrites_);
      pendingBytes_ = 0;
    }

    // Write all buffered entries with a single disk I/O
    for (const auto &buffer : toWrite) {
      uint32_t entrySize = buffer.size();
      logFile_.write(reinterpret_cast<const char *>(&entrySize),
                     sizeof(entrySize));
      logFile_.write(reinterpret_cast<const char *>(buffer.data()),
                     buffer.size());
      totalBytesWritten_ += sizeof(entrySize) + buffer.size();
    }

    logFile_.flush();
    totalFlushes_++;
  }

  void backgroundFlushWorker() {
    while (running_) {
      std::unique_lock<std::mutex> lock(flushCondMutex_);
      flushCond_.wait_for(lock, config_.flushInterval,
                          [this] { return !running_; });

      if (!running_)
        break;
      flushBuffer();
    }

    // Final flush on shutdown
    flushBuffer();
  }

public:
  explicit GroupCommitWAL(const std::string &path,
                          const GroupCommitConfig &config = GroupCommitConfig())
      : filePath_(path), config_(config) {

    logFile_.open(filePath_, std::ios::binary | std::ios::app);
    if (!logFile_.is_open()) {
      throw std::runtime_error("Failed to open WAL file: " + filePath_);
    }

    // Start background flush thread for TIMED mode
    if (config_.mode == FlushMode::TIMED) {
      running_ = true;
      flushThread_ = std::thread(&GroupCommitWAL::backgroundFlushWorker, this);
    }
  }

  ~GroupCommitWAL() {
    // Stop background thread
    running_ = false;
    flushCond_.notify_all();
    if (flushThread_.joinable()) {
      flushThread_.join();
    }

    // Final flush
    flushBuffer();
    logFile_.close();
  }

  // Append entry to buffer (may not hit disk immediately)
  bool append(uint64_t sequenceNumber, Operation op, const std::string &key,
              const std::string &value) {

    WALEntry entry(sequenceNumber, getCurrentTimestamp(), op, key, value);
    auto buffer = serialize(entry);

    bool shouldFlush = false;

    {
      std::lock_guard<std::mutex> lock(bufferMutex_);
      pendingWrites_.push_back(std::move(buffer));
      pendingBytes_ += pendingWrites_.back().size();
      totalWrites_++;

      // Check flush conditions
      if (config_.mode == FlushMode::IMMEDIATE) {
        shouldFlush = true;
      } else if (config_.mode == FlushMode::BATCHED) {
        shouldFlush = (pendingWrites_.size() >= config_.batchSize ||
                       pendingBytes_ >= config_.batchBytes);
      }
      // TIMED mode flushes in background thread
    }

    if (shouldFlush) {
      flushBuffer();
    }

    return true;
  }

  // Force immediate flush (for commit/sync)
  void sync() { flushBuffer(); }

  // Get statistics
  uint64_t getTotalWrites() const { return totalWrites_; }
  uint64_t getTotalFlushes() const { return totalFlushes_; }
  uint64_t getTotalBytesWritten() const { return totalBytesWritten_; }

  double getAverageWritesPerFlush() const {
    uint64_t flushes = totalFlushes_.load();
    return flushes > 0 ? static_cast<double>(totalWrites_) / flushes : 0.0;
  }

  size_t getPendingCount() const {
    std::lock_guard<std::mutex> lock(bufferMutex_);
    return pendingWrites_.size();
  }
};

} // namespace wal

#endif // WAL_HPP