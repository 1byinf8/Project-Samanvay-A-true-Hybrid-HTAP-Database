// sstable.hpp - Fixed version
#pragma once
#ifndef SSTABLE_HPP
#define SSTABLE_HPP

#include <string>
#include <vector>
#include <fstream>
#include <memory>
#include <optional>
#include <unordered_map>
#include <algorithm>
#include <cmath>
#include <iostream>
#include "../includes/skiplist.hpp"

namespace sstable {

struct IndexEntry {
    std::string key;
    uint64_t offset;
    uint32_t size;
    
    IndexEntry(const std::string& k, uint64_t off, uint32_t sz) 
        : key(k), offset(off), size(sz) {}
};

struct BloomFilter {
private:
    std::vector<bool> bits;
    size_t numHashFunctions;
    size_t size;
    
    uint64_t hash1(const std::string& key) const {
        uint64_t hash = 0;
        for (char c : key) {
            hash = hash * 31 + c;
        }
        return hash;
    }
    
    uint64_t hash2(const std::string& key) const {
        uint64_t hash = 0;
        for (char c : key) {
            hash = hash * 37 + c;
        }
        return hash;
    }
    
public:
    BloomFilter(size_t expectedElements, double falsePositiveRate = 0.01) {
        size = static_cast<size_t>(-expectedElements * log(falsePositiveRate) / (log(2) * log(2)));
        numHashFunctions = static_cast<size_t>(size * log(2) / expectedElements);
        bits.resize(size, false);
    }
    
    void add(const std::string& key) {
        uint64_t h1 = hash1(key) % size;
        uint64_t h2 = hash2(key) % size;
        
        for (size_t i = 0; i < numHashFunctions; ++i) {
            size_t pos = (h1 + i * h2) % size;
            bits[pos] = true;
        }
    }
    
    bool mightContain(const std::string& key) const {
        uint64_t h1 = hash1(key) % size;
        uint64_t h2 = hash2(key) % size;
        
        for (size_t i = 0; i < numHashFunctions; ++i) {
            size_t pos = (h1 + i * h2) % size;
            if (!bits[pos]) {
                return false;
            }
        }
        return true;
    }
    
    void serialize(std::ofstream& file) const {
        // Write bloom filter metadata and data
        file.write(reinterpret_cast<const char*>(&size), sizeof(size));
        file.write(reinterpret_cast<const char*>(&numHashFunctions), sizeof(numHashFunctions));
        
        // Write bits (convert to bytes)
        std::vector<uint8_t> bytes((size + 7) / 8, 0);
        for (size_t i = 0; i < size; ++i) {
            if (bits[i]) {
                bytes[i / 8] |= (1 << (i % 8));
            }
        }
        file.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
};

class SSTable {
private:
    std::string filePath;
    std::vector<IndexEntry> sparseIndex;
    std::unique_ptr<BloomFilter> bloomFilter;
    uint64_t minSequence;
    uint64_t maxSequence;
    std::string minKey;
    std::string maxKey;
    size_t entryCount;
    
    struct SSTableHeader {
        uint32_t version = 1;
        uint64_t minSequence;
        uint64_t maxSequence;
        uint32_t entryCount;
        uint32_t indexCount;
        uint64_t indexOffset;
        uint64_t bloomFilterOffset;
        uint32_t minKeyLen;
        uint32_t maxKeyLen;
        // Followed by minKey and maxKey strings
    };
    
public:
    SSTable(const std::string& path) : filePath(path), entryCount(0) {}
    
    // Create SSTable from memtable
    bool create(const std::vector<std::pair<std::string, skiplist::Entry>>& sortedEntries) {
        if (sortedEntries.empty()) {
            return false;
        }
        
        std::ofstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            return false;
        }
        
        // Initialize metadata
        minKey = sortedEntries.front().first;
        maxKey = sortedEntries.back().first;
        minSequence = UINT64_MAX;
        maxSequence = 0;
        entryCount = sortedEntries.size();
        
        // Create bloom filter
        bloomFilter = std::make_unique<BloomFilter>(entryCount);
        
        // Write placeholder header (we'll update it later)
        SSTableHeader header;
        std::streampos headerPos = file.tellp();
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        
        // Write minKey and maxKey lengths and strings (placeholder)
        file.write(reinterpret_cast<const char*>(&header.minKeyLen), sizeof(header.minKeyLen));
        file.write(reinterpret_cast<const char*>(&header.maxKeyLen), sizeof(header.maxKeyLen));
        
        std::streampos dataStartOffset = file.tellp();
        dataStartOffset += static_cast<std::streampos>(minKey.size() + maxKey.size());
        file.seekp(dataStartOffset);
        
        // Write data blocks and build sparse index
        const size_t SPARSE_INDEX_INTERVAL = 1000; // Every 1000 entries
        for (size_t i = 0; i < sortedEntries.size(); ++i) {
            const auto& [key, entry] = sortedEntries[i];
            
            std::streampos entryOffset = file.tellp();
            
            // Build sparse index (every Nth entry)
            if (i % SPARSE_INDEX_INTERVAL == 0) {
                sparseIndex.emplace_back(key, static_cast<uint64_t>(entryOffset), 0); // Size will be calculated later
            }
            
            // Update sequence range
            minSequence = std::min(minSequence, entry.seq);
            maxSequence = std::max(maxSequence, entry.seq);
            
            // Add to bloom filter
            bloomFilter->add(key);
            
            // Write entry: [keyLen][key][valueLen][value][sequence][isDeleted]
            uint32_t keyLen = key.size();
            uint32_t valueLen = entry.value.size();
            
            file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
            file.write(key.c_str(), keyLen);
            file.write(reinterpret_cast<const char*>(&valueLen), sizeof(valueLen));
            file.write(entry.value.c_str(), valueLen);
            file.write(reinterpret_cast<const char*>(&entry.seq), sizeof(entry.seq));
            file.write(reinterpret_cast<const char*>(&entry.isDeleted), sizeof(entry.isDeleted));
            
            // Update sparse index entry size
            if (!sparseIndex.empty() && i % SPARSE_INDEX_INTERVAL == 0) {
                std::streampos currentPos = file.tellp();
                sparseIndex.back().size = static_cast<uint32_t>(currentPos - entryOffset);
            }
        }
        
        // Write sparse index
        std::streampos indexOffset = file.tellp();
        for (const auto& indexEntry : sparseIndex) {
            uint32_t keyLen = indexEntry.key.size();
            file.write(reinterpret_cast<const char*>(&keyLen), sizeof(keyLen));
            file.write(indexEntry.key.c_str(), keyLen);
            file.write(reinterpret_cast<const char*>(&indexEntry.offset), sizeof(indexEntry.offset));
            file.write(reinterpret_cast<const char*>(&indexEntry.size), sizeof(indexEntry.size));
        }
        
        // Write bloom filter
        std::streampos bloomFilterOffset = file.tellp();
        bloomFilter->serialize(file);
        
        // Update and write final header
        file.seekp(headerPos);
        header.minSequence = minSequence;
        header.maxSequence = maxSequence;
        header.entryCount = entryCount;
        header.indexCount = sparseIndex.size();
        header.indexOffset = static_cast<uint64_t>(indexOffset);
        header.bloomFilterOffset = static_cast<uint64_t>(bloomFilterOffset);
        header.minKeyLen = minKey.size();
        header.maxKeyLen = maxKey.size();
        
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        file.write(reinterpret_cast<const char*>(&header.minKeyLen), sizeof(header.minKeyLen));
        file.write(minKey.c_str(), minKey.size());
        file.write(reinterpret_cast<const char*>(&header.maxKeyLen), sizeof(header.maxKeyLen));
        file.write(maxKey.c_str(), maxKey.size());
        
        file.close();
        return true;
    }
    
    std::optional<skiplist::Entry> get(const std::string& key) {
        // Quick bloom filter check
        if (bloomFilter && !bloomFilter->mightContain(key)) {
            return std::nullopt;
        }
        
        // Check key range
        if (key < minKey || key > maxKey) {
            return std::nullopt;
        }
        
        std::ifstream file(filePath, std::ios::binary);
        if (!file.is_open()) {
            return std::nullopt;
        }
        
        // Find appropriate sparse index entry
        auto it = std::upper_bound(sparseIndex.begin(), sparseIndex.end(), key,
            [](const std::string& key, const IndexEntry& entry) {
                return key < entry.key;
            });
        
        if (it != sparseIndex.begin()) {
            --it; // Go to the entry before or equal to our key
        }
        
        // Seek to the sparse index position and scan
        file.seekg(it->offset);
        
        // Linear scan from sparse index position
        while (file.good()) {
            uint32_t keyLen;
            if (!file.read(reinterpret_cast<char*>(&keyLen), sizeof(keyLen))) {
                break;
            }
            
            std::string entryKey(keyLen, '\0');
            file.read(&entryKey[0], keyLen);
            
            if (entryKey == key) {
                // Found the key, read the entry
                uint32_t valueLen;
                file.read(reinterpret_cast<char*>(&valueLen), sizeof(valueLen));
                
                std::string value(valueLen, '\0');
                file.read(&value[0], valueLen);
                
                uint64_t seq;
                bool isDeleted;
                file.read(reinterpret_cast<char*>(&seq), sizeof(seq));
                file.read(reinterpret_cast<char*>(&isDeleted), sizeof(isDeleted));
                
                return skiplist::Entry(entryKey, value, seq, isDeleted);
            } else if (entryKey > key) {
                // We've passed the key, it doesn't exist
                break;
            }
            
            // Skip this entry
            uint32_t valueLen;
            file.read(reinterpret_cast<char*>(&valueLen), sizeof(valueLen));
            file.seekg(valueLen + sizeof(uint64_t) + sizeof(bool), std::ios::cur);
        }
        
        return std::nullopt;
    }
    
    // Getters for metadata
    const std::string& getMinKey() const { return minKey; }
    const std::string& getMaxKey() const { return maxKey; }
    uint64_t getMinSequence() const { return minSequence; }
    uint64_t getMaxSequence() const { return maxSequence; }
    size_t getEntryCount() const { return entryCount; }
    const std::string& getFilePath() const { return filePath; }
    
    // Range query support
    std::vector<std::pair<std::string, skiplist::Entry>> rangeQuery(
        const std::string& startKey, const std::string& endKey) {
        // TODO: Implement range query using sparse index
        std::vector<std::pair<std::string, skiplist::Entry>> results;
        return results;
    }
};

} // namespace sstable

#endif // SSTABLE_HPP