// wal.hpp - Fixed version
#pragma once
#ifndef WAL_HPP
#define WAL_HPP

#include <fstream>
#include <string>
#include <vector>
#include <mutex>
#include <cstdint>
#include <memory>
#include <chrono>

namespace wal {

enum class Operation : uint8_t {
    INSERT = 1,
    DELETE = 2
};

struct WALEntry {
    uint64_t sequenceNumber;
    uint64_t timestamp;
    Operation operation;
    std::string key;
    std::string value;
    
    WALEntry(uint64_t seq, uint64_t ts, Operation op, const std::string& k, const std::string& v)
        : sequenceNumber(seq), timestamp(ts), operation(op), key(k), value(v) {}
};

class WAL {
private:
    std::ofstream logFile;
    std::mutex writeMutex;
    std::string filePath;
    uint64_t currentOffset;
    
    // Serialize entry to binary format
    std::vector<uint8_t> serialize(const WALEntry& entry) {
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
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    
public:
    WAL(const std::string& path) : filePath(path), currentOffset(0) {
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
    
    bool append(uint64_t sequenceNumber, Operation op, const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(writeMutex);
        
        WALEntry entry(sequenceNumber, getCurrentTimestamp(), op, key, value);
        auto buffer = serialize(entry);
        
        // Write entry size first (for recovery)
        uint32_t entrySize = buffer.size();
        logFile.write(reinterpret_cast<const char*>(&entrySize), sizeof(entrySize));
        
        // Write the entry
        logFile.write(reinterpret_cast<const char*>(buffer.data()), buffer.size());
        
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
        // TODO: Implement WAL truncation after successful SSTable flush
        // This removes old log entries that are now safely persisted in SSTables
    }
    
    std::vector<WALEntry> recover() {
        std::vector<WALEntry> entries;
        std::ifstream recoveryFile(filePath, std::ios::binary);
        
        if (!recoveryFile.is_open()) {
            return entries; // No existing WAL file
        }
        
        // TODO: Implement WAL recovery logic
        // Read entries and rebuild memtable state
        
        recoveryFile.close();
        return entries;
    }
    
    void createCheckpoint(const std::string& checkpointPath) {
        std::lock_guard<std::mutex> lock(writeMutex);
        // TODO: Create WAL checkpoint for backup/recovery
    }
};

} // namespace wal

#endif // WAL_HPP