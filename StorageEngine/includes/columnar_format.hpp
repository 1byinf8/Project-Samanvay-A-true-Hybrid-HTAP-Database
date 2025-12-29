// columnar_format.hpp - Columnar storage format for OLAP workloads
#pragma once
#ifndef COLUMNAR_FORMAT_HPP
#define COLUMNAR_FORMAT_HPP

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

namespace columnar {

// Column data types
enum class ColumnType : uint8_t {
  INT64 = 1,
  DOUBLE = 2,
  STRING = 3,
  BOOL = 4,
  BYTES = 5,
  INT32 = 6,
  FLOAT = 7,
  TIMESTAMP = 8 // Stored as int64 (milliseconds since epoch)
};

// Compression strategies for columns
enum class Compression : uint8_t {
  NONE = 0,
  RLE = 1,        // Run-length encoding (good for repeated values)
  DICTIONARY = 2, // Dictionary encoding (good for low-cardinality strings)
  DELTA = 3,      // Delta encoding (good for sorted numerics)
  BITPACKING = 4  // Bit-packing for small integers
};

// Schema definition for a single column
struct ColumnSchema {
  std::string name;
  ColumnType type;
  bool nullable;
  Compression preferredCompression;

  ColumnSchema()
      : type(ColumnType::STRING), nullable(true),
        preferredCompression(Compression::NONE) {}

  ColumnSchema(const std::string &n, ColumnType t, bool null = true,
               Compression comp = Compression::NONE)
      : name(n), type(t), nullable(null), preferredCompression(comp) {}

  // Get the size of a single value (for fixed-size types)
  size_t getValueSize() const {
    switch (type) {
    case ColumnType::INT64:
    case ColumnType::DOUBLE:
    case ColumnType::TIMESTAMP:
      return 8;
    case ColumnType::INT32:
    case ColumnType::FLOAT:
      return 4;
    case ColumnType::BOOL:
      return 1;
    case ColumnType::STRING:
    case ColumnType::BYTES:
      return 0; // Variable size
    default:
      return 0;
    }
  }

  bool isFixedSize() const {
    return type != ColumnType::STRING && type != ColumnType::BYTES;
  }
};

// Table schema containing multiple columns
struct TableSchema {
  std::string tableName;
  std::vector<ColumnSchema> columns;
  std::string primaryKeyColumn; // Usually the row key

  TableSchema() = default;

  TableSchema(const std::string &name) : tableName(name) {}

  void addColumn(const ColumnSchema &col) { columns.push_back(col); }

  int getColumnIndex(const std::string &name) const {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].name == name) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

  size_t columnCount() const { return columns.size(); }

  // Serialize schema to binary
  std::vector<uint8_t> serialize() const {
    std::vector<uint8_t> buffer;

    // Table name
    uint32_t nameLen = tableName.size();
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&nameLen),
                  reinterpret_cast<uint8_t *>(&nameLen) + sizeof(nameLen));
    buffer.insert(buffer.end(), tableName.begin(), tableName.end());

    // Primary key column
    uint32_t pkLen = primaryKeyColumn.size();
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&pkLen),
                  reinterpret_cast<uint8_t *>(&pkLen) + sizeof(pkLen));
    buffer.insert(buffer.end(), primaryKeyColumn.begin(),
                  primaryKeyColumn.end());

    // Number of columns
    uint32_t numCols = columns.size();
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&numCols),
                  reinterpret_cast<uint8_t *>(&numCols) + sizeof(numCols));

    // Each column
    for (const auto &col : columns) {
      // Column name
      uint32_t colNameLen = col.name.size();
      buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&colNameLen),
                    reinterpret_cast<uint8_t *>(&colNameLen) +
                        sizeof(colNameLen));
      buffer.insert(buffer.end(), col.name.begin(), col.name.end());

      // Type, nullable, compression
      buffer.push_back(static_cast<uint8_t>(col.type));
      buffer.push_back(col.nullable ? 1 : 0);
      buffer.push_back(static_cast<uint8_t>(col.preferredCompression));
    }

    return buffer;
  }
};

// Statistics for a column (used for query optimization)
struct ColumnStats {
  // For numeric types
  int64_t minInt = INT64_MAX;
  int64_t maxInt = INT64_MIN;
  double minDouble = std::numeric_limits<double>::max();
  double maxDouble = std::numeric_limits<double>::lowest();

  // For string types
  std::string minString;
  std::string maxString;

  // General stats
  uint64_t nullCount = 0;
  uint64_t distinctCount = 0; // Approximate (HyperLogLog in production)
  uint64_t totalCount = 0;

  // Sum for aggregations (INT64 and DOUBLE)
  int64_t sumInt = 0;
  double sumDouble = 0.0;

  ColumnStats() = default;

  void updateInt(int64_t value) {
    minInt = std::min(minInt, value);
    maxInt = std::max(maxInt, value);
    sumInt += value;
    totalCount++;
  }

  void updateDouble(double value) {
    minDouble = std::min(minDouble, value);
    maxDouble = std::max(maxDouble, value);
    sumDouble += value;
    totalCount++;
  }

  void updateString(const std::string &value) {
    if (minString.empty() || value < minString)
      minString = value;
    if (maxString.empty() || value > maxString)
      maxString = value;
    totalCount++;
  }

  void updateNull() {
    nullCount++;
    totalCount++;
  }
};

// In-memory column block (for processing)
class ColumnBlock {
private:
  ColumnType type_;
  Compression compression_;
  size_t numRows_;
  std::vector<uint8_t> data_;    // Raw or compressed data
  std::vector<bool> nullBitmap_; // Null tracking
  ColumnStats stats_;

  // For string columns: offsets into data_
  std::vector<uint32_t> stringOffsets_;

public:
  ColumnBlock(ColumnType type, size_t numRows = 0)
      : type_(type), compression_(Compression::NONE), numRows_(numRows) {
    nullBitmap_.resize(numRows, false);
  }

  ColumnType getType() const { return type_; }
  size_t getNumRows() const { return numRows_; }
  const ColumnStats &getStats() const { return stats_; }
  Compression getCompression() const { return compression_; }

  // Check if a specific row is null
  bool isNull(size_t row) const {
    if (row < nullBitmap_.size()) {
      return nullBitmap_[row];
    }
    return true;
  }

  // Set null for a specific row
  void setNull(size_t row) {
    if (row >= nullBitmap_.size()) {
      nullBitmap_.resize(row + 1, false);
    }
    nullBitmap_[row] = true;
    stats_.updateNull();
  }

  // Append an INT64 value
  void appendInt64(int64_t value) {
    size_t offset = data_.size();
    data_.resize(offset + sizeof(int64_t));
    std::memcpy(&data_[offset], &value, sizeof(value));
    nullBitmap_.push_back(false);
    numRows_++;
    stats_.updateInt(value);
  }

  // Append a DOUBLE value
  void appendDouble(double value) {
    size_t offset = data_.size();
    data_.resize(offset + sizeof(double));
    std::memcpy(&data_[offset], &value, sizeof(value));
    nullBitmap_.push_back(false);
    numRows_++;
    stats_.updateDouble(value);
  }

  // Append a STRING value
  void appendString(const std::string &value) {
    stringOffsets_.push_back(static_cast<uint32_t>(data_.size()));
    data_.insert(data_.end(), value.begin(), value.end());
    data_.push_back('\0'); // Null terminator for safety
    nullBitmap_.push_back(false);
    numRows_++;
    stats_.updateString(value);
  }

  // Append a BOOL value
  void appendBool(bool value) {
    data_.push_back(value ? 1 : 0);
    nullBitmap_.push_back(false);
    numRows_++;
    stats_.updateInt(value ? 1 : 0);
  }

  // Get INT64 values
  std::vector<int64_t> getAsInt64() const {
    std::vector<int64_t> result;
    result.reserve(numRows_);

    for (size_t i = 0; i < numRows_; ++i) {
      if (isNull(i)) {
        result.push_back(0); // Default for null
      } else {
        int64_t value;
        std::memcpy(&value, &data_[i * sizeof(int64_t)], sizeof(value));
        result.push_back(value);
      }
    }
    return result;
  }

  // Get DOUBLE values
  std::vector<double> getAsDouble() const {
    std::vector<double> result;
    result.reserve(numRows_);

    for (size_t i = 0; i < numRows_; ++i) {
      if (isNull(i)) {
        result.push_back(0.0);
      } else {
        double value;
        std::memcpy(&value, &data_[i * sizeof(double)], sizeof(value));
        result.push_back(value);
      }
    }
    return result;
  }

  // Get STRING values
  std::vector<std::string> getAsString() const {
    std::vector<std::string> result;
    result.reserve(numRows_);

    for (size_t i = 0; i < numRows_; ++i) {
      if (isNull(i)) {
        result.push_back("");
      } else if (i < stringOffsets_.size()) {
        const char *start =
            reinterpret_cast<const char *>(&data_[stringOffsets_[i]]);
        result.push_back(std::string(start));
      }
    }
    return result;
  }

  // Aggregation helpers (operate directly on data for efficiency)
  int64_t sumInt64() const { return stats_.sumInt; }

  double sumDouble() const { return stats_.sumDouble; }

  int64_t minInt64() const { return stats_.minInt; }

  int64_t maxInt64() const { return stats_.maxInt; }

  double minDouble() const { return stats_.minDouble; }

  double maxDouble() const { return stats_.maxDouble; }

  size_t count() const { return numRows_ - stats_.nullCount; }

  size_t countNulls() const { return stats_.nullCount; }

  // Serialize column data to binary
  std::vector<uint8_t> serialize() const {
    std::vector<uint8_t> buffer;

    // Header: type, compression, numRows
    buffer.push_back(static_cast<uint8_t>(type_));
    buffer.push_back(static_cast<uint8_t>(compression_));

    uint64_t rows = numRows_;
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&rows),
                  reinterpret_cast<uint8_t *>(&rows) + sizeof(rows));

    // Null bitmap (packed into bytes)
    uint64_t nullBitmapSize = (numRows_ + 7) / 8;
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&nullBitmapSize),
                  reinterpret_cast<uint8_t *>(&nullBitmapSize) +
                      sizeof(nullBitmapSize));

    std::vector<uint8_t> packedNulls((numRows_ + 7) / 8, 0);
    for (size_t i = 0; i < numRows_; ++i) {
      if (nullBitmap_[i]) {
        packedNulls[i / 8] |= (1 << (i % 8));
      }
    }
    buffer.insert(buffer.end(), packedNulls.begin(), packedNulls.end());

    // Data size and data
    uint64_t dataSize = data_.size();
    buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&dataSize),
                  reinterpret_cast<uint8_t *>(&dataSize) + sizeof(dataSize));
    buffer.insert(buffer.end(), data_.begin(), data_.end());

    // String offsets if applicable
    if (type_ == ColumnType::STRING || type_ == ColumnType::BYTES) {
      uint32_t numOffsets = stringOffsets_.size();
      buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&numOffsets),
                    reinterpret_cast<uint8_t *>(&numOffsets) +
                        sizeof(numOffsets));
      for (uint32_t offset : stringOffsets_) {
        buffer.insert(buffer.end(), reinterpret_cast<uint8_t *>(&offset),
                      reinterpret_cast<uint8_t *>(&offset) + sizeof(offset));
      }
    }

    return buffer;
  }

  // Get raw data (for writing to file)
  const std::vector<uint8_t> &getRawData() const { return data_; }
  const std::vector<bool> &getNullBitmap() const { return nullBitmap_; }
};

// Run-length encoding helper
class RLEEncoder {
public:
  // Encode a vector of int64 values
  static std::vector<uint8_t> encodeInt64(const std::vector<int64_t> &values) {
    std::vector<uint8_t> encoded;

    if (values.empty())
      return encoded;

    int64_t currentValue = values[0];
    uint32_t runLength = 1;

    for (size_t i = 1; i < values.size(); ++i) {
      if (values[i] == currentValue && runLength < UINT32_MAX) {
        runLength++;
      } else {
        // Write run
        encoded.insert(encoded.end(), reinterpret_cast<uint8_t *>(&runLength),
                       reinterpret_cast<uint8_t *>(&runLength) +
                           sizeof(runLength));
        encoded.insert(
            encoded.end(), reinterpret_cast<uint8_t *>(&currentValue),
            reinterpret_cast<uint8_t *>(&currentValue) + sizeof(currentValue));

        currentValue = values[i];
        runLength = 1;
      }
    }

    // Write final run
    encoded.insert(encoded.end(), reinterpret_cast<uint8_t *>(&runLength),
                   reinterpret_cast<uint8_t *>(&runLength) + sizeof(runLength));
    encoded.insert(encoded.end(), reinterpret_cast<uint8_t *>(&currentValue),
                   reinterpret_cast<uint8_t *>(&currentValue) +
                       sizeof(currentValue));

    return encoded;
  }

  // Decode RLE-encoded int64 values
  static std::vector<int64_t> decodeInt64(const std::vector<uint8_t> &encoded) {
    std::vector<int64_t> values;

    size_t offset = 0;
    while (offset + sizeof(uint32_t) + sizeof(int64_t) <= encoded.size()) {
      uint32_t runLength;
      int64_t value;

      std::memcpy(&runLength, &encoded[offset], sizeof(runLength));
      offset += sizeof(runLength);

      std::memcpy(&value, &encoded[offset], sizeof(value));
      offset += sizeof(value);

      for (uint32_t i = 0; i < runLength; ++i) {
        values.push_back(value);
      }
    }

    return values;
  }
};

// Dictionary encoding helper
class DictionaryEncoder {
private:
  std::vector<std::string> dictionary_;

public:
  // Build dictionary and return encoded indices
  std::vector<uint32_t> encode(const std::vector<std::string> &values) {
    dictionary_.clear();
    std::vector<uint32_t> indices;
    indices.reserve(values.size());

    for (const auto &value : values) {
      auto it = std::find(dictionary_.begin(), dictionary_.end(), value);
      if (it != dictionary_.end()) {
        indices.push_back(static_cast<uint32_t>(it - dictionary_.begin()));
      } else {
        dictionary_.push_back(value);
        indices.push_back(static_cast<uint32_t>(dictionary_.size() - 1));
      }
    }

    return indices;
  }

  // Decode indices back to strings
  std::vector<std::string> decode(const std::vector<uint32_t> &indices) const {
    std::vector<std::string> values;
    values.reserve(indices.size());

    for (uint32_t idx : indices) {
      if (idx < dictionary_.size()) {
        values.push_back(dictionary_[idx]);
      } else {
        values.push_back(""); // Invalid index
      }
    }

    return values;
  }

  const std::vector<std::string> &getDictionary() const { return dictionary_; }

  size_t getDictionarySize() const { return dictionary_.size(); }
};

} // namespace columnar

#endif // COLUMNAR_FORMAT_HPP
