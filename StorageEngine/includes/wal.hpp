// wal.hpp - Fixed version
#pragma once
#ifndef WAL_HPP
#define WAL_HPP

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace wal {

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
      if (entrySize > 10 * 1024 * 1024) { // Max 10MB per entry
        break;
      }

      // Read entry data
      std::vector<uint8_t> buffer(entrySize);
      if (!recoveryFile.read(reinterpret_cast<char *>(buffer.data()),
                             entrySize)) {
        break;
      }

      // Deserialize entry
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
      if (keyLen > entrySize - offset) {
        break; // Corrupted entry
      }

      std::string key(reinterpret_cast<char *>(&buffer[offset]), keyLen);
      offset += keyLen;

      // Read value length (4 bytes, little endian)
      uint32_t valueLen = 0;
      for (int i = 0; i < 4; ++i) {
        valueLen |= static_cast<uint32_t>(buffer[offset++]) << (i * 8);
      }

      // Validate value length
      if (valueLen > entrySize - offset) {
        break; // Corrupted entry
      }

      std::string value(reinterpret_cast<char *>(&buffer[offset]), valueLen);

      entries.emplace_back(seq, ts, op, key, value);
    }

    recoveryFile.close();

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

} // namespace wal

#endif // WAL_HPP