// columnar_file.hpp - Columnar file format (similar to Parquet/ORC)
#pragma once
#ifndef COLUMNAR_FILE_HPP
#define COLUMNAR_FILE_HPP

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "columnar_format.hpp"

namespace columnar {

/*
 * Columnar File Layout:
 *
 * +------------------+
 * | File Header      |  <- Magic bytes, version, num row groups
 * +------------------+
 * | Schema           |  <- Serialized TableSchema
 * +------------------+
 * | Row Group 1      |  <- Contains chunks of all columns
 * |  - Column 0 data |
 * |  - Column 1 data |
 * |  - ...           |
 * +------------------+
 * | Row Group 2      |
 * +------------------+
 * | ...              |
 * +------------------+
 * | Footer           |  <- Row group offsets, statistics
 * +------------------+
 */

// Magic bytes for columnar file
constexpr uint32_t COLUMNAR_MAGIC = 0x434F4C46; // "COLF" in little endian
constexpr uint16_t COLUMNAR_VERSION = 1;

// Metadata for a single row group
struct RowGroupMeta {
  uint64_t offset;  // File offset where this row group starts
  uint64_t size;    // Total bytes of this row group
  uint64_t numRows; // Number of rows in this row group
  std::vector<ColumnStats> columnStats; // Per-column statistics
  std::vector<uint64_t> columnOffsets; // Offset of each column within row group

  RowGroupMeta() : offset(0), size(0), numRows(0) {}
};

// File header structure
struct ColumnarFileHeader {
  uint32_t magic;
  uint16_t version;
  uint64_t numRows;
  uint32_t numRowGroups;
  uint64_t schemaOffset;
  uint64_t footerOffset;

  ColumnarFileHeader()
      : magic(COLUMNAR_MAGIC), version(COLUMNAR_VERSION), numRows(0),
        numRowGroups(0), schemaOffset(0), footerOffset(0) {}
};

// Columnar file reader/writer
class ColumnarFile {
private:
  std::string filePath_;
  TableSchema schema_;
  std::vector<RowGroupMeta> rowGroups_;
  ColumnarFileHeader header_;
  bool isWriteMode_;
  std::ofstream writeFile_;
  uint64_t currentOffset_;

  // Current row group being built
  std::vector<ColumnBlock> currentRowGroup_;
  size_t rowsInCurrentGroup_;
  size_t targetRowGroupSize_; // Target number of rows per group

public:
  ColumnarFile(const std::string &path, const TableSchema &schema,
               size_t rowGroupSize = 1000000)
      : filePath_(path), schema_(schema), isWriteMode_(true), currentOffset_(0),
        rowsInCurrentGroup_(0), targetRowGroupSize_(rowGroupSize) {

    // Initialize column blocks for current row group
    for (const auto &col : schema_.columns) {
      currentRowGroup_.emplace_back(col.type);
    }
  }

  // Open existing file for reading
  static std::unique_ptr<ColumnarFile> open(const std::string &path) {
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
      return nullptr;
    }

    // Read header
    ColumnarFileHeader header;
    file.read(reinterpret_cast<char *>(&header), sizeof(header));

    if (header.magic != COLUMNAR_MAGIC) {
      return nullptr; // Not a valid columnar file
    }

    // Read schema (simplified - would need full deserialization)
    TableSchema schema;

    auto result = std::make_unique<ColumnarFile>(path, schema);
    result->header_ = header;
    result->isWriteMode_ = false;

    // Read footer to get row group metadata
    // (Full implementation would deserialize footer here)

    file.close();
    return result;
  }

  // Start writing a new file
  bool create() {
    writeFile_.open(filePath_, std::ios::binary | std::ios::trunc);
    if (!writeFile_.is_open()) {
      return false;
    }

    // Write placeholder header
    header_.magic = COLUMNAR_MAGIC;
    header_.version = COLUMNAR_VERSION;
    writeFile_.write(reinterpret_cast<const char *>(&header_), sizeof(header_));
    currentOffset_ = sizeof(header_);

    // Write schema
    header_.schemaOffset = currentOffset_;
    auto schemaData = schema_.serialize();
    uint32_t schemaSize = schemaData.size();
    writeFile_.write(reinterpret_cast<const char *>(&schemaSize),
                     sizeof(schemaSize));
    writeFile_.write(reinterpret_cast<const char *>(schemaData.data()),
                     schemaSize);
    currentOffset_ += sizeof(schemaSize) + schemaSize;

    return true;
  }

  // Add a row to the current row group
  void addRow(const std::vector<std::string> &values) {
    if (values.size() != schema_.columns.size()) {
      return; // Invalid row
    }

    for (size_t i = 0; i < values.size(); ++i) {
      const auto &col = schema_.columns[i];
      const auto &value = values[i];

      if (value.empty() && col.nullable) {
        currentRowGroup_[i].setNull(rowsInCurrentGroup_);
      } else {
        switch (col.type) {
        case ColumnType::INT64:
        case ColumnType::TIMESTAMP:
          currentRowGroup_[i].appendInt64(std::stoll(value));
          break;
        case ColumnType::INT32:
          currentRowGroup_[i].appendInt64(std::stoi(value));
          break;
        case ColumnType::DOUBLE:
        case ColumnType::FLOAT:
          currentRowGroup_[i].appendDouble(std::stod(value));
          break;
        case ColumnType::BOOL:
          currentRowGroup_[i].appendBool(value == "true" || value == "1");
          break;
        case ColumnType::STRING:
        case ColumnType::BYTES:
          currentRowGroup_[i].appendString(value);
          break;
        }
      }
    }

    rowsInCurrentGroup_++;

    // Flush row group if it's full
    if (rowsInCurrentGroup_ >= targetRowGroupSize_) {
      flushRowGroup();
    }
  }

  // Flush the current row group to disk
  void flushRowGroup() {
    if (rowsInCurrentGroup_ == 0) {
      return;
    }

    RowGroupMeta meta;
    meta.offset = currentOffset_;
    meta.numRows = rowsInCurrentGroup_;

    // Write each column
    for (size_t i = 0; i < currentRowGroup_.size(); ++i) {
      meta.columnOffsets.push_back(currentOffset_ - meta.offset);
      meta.columnStats.push_back(currentRowGroup_[i].getStats());

      auto columnData = currentRowGroup_[i].serialize();
      uint32_t columnSize = columnData.size();

      writeFile_.write(reinterpret_cast<const char *>(&columnSize),
                       sizeof(columnSize));
      writeFile_.write(reinterpret_cast<const char *>(columnData.data()),
                       columnSize);

      currentOffset_ += sizeof(columnSize) + columnSize;
    }

    meta.size = currentOffset_ - meta.offset;
    rowGroups_.push_back(meta);

    header_.numRows += rowsInCurrentGroup_;
    header_.numRowGroups++;

    // Reset current row group
    currentRowGroup_.clear();
    for (const auto &col : schema_.columns) {
      currentRowGroup_.emplace_back(col.type);
    }
    rowsInCurrentGroup_ = 0;
  }

  // Write row group directly from column blocks
  void writeRowGroup(const std::vector<ColumnBlock> &columns) {
    if (columns.size() != schema_.columns.size()) {
      return;
    }

    RowGroupMeta meta;
    meta.offset = currentOffset_;
    meta.numRows = columns.empty() ? 0 : columns[0].getNumRows();

    for (size_t i = 0; i < columns.size(); ++i) {
      meta.columnOffsets.push_back(currentOffset_ - meta.offset);
      meta.columnStats.push_back(columns[i].getStats());

      auto columnData = columns[i].serialize();
      uint32_t columnSize = columnData.size();

      writeFile_.write(reinterpret_cast<const char *>(&columnSize),
                       sizeof(columnSize));
      writeFile_.write(reinterpret_cast<const char *>(columnData.data()),
                       columnSize);

      currentOffset_ += sizeof(columnSize) + columnSize;
    }

    meta.size = currentOffset_ - meta.offset;
    rowGroups_.push_back(meta);

    header_.numRows += meta.numRows;
    header_.numRowGroups++;
  }

  // Finalize the file (write footer and update header)
  void finalize() {
    // Flush any remaining rows
    flushRowGroup();

    // Write footer
    header_.footerOffset = currentOffset_;

    // Write row group metadata
    uint32_t numRowGroups = rowGroups_.size();
    writeFile_.write(reinterpret_cast<const char *>(&numRowGroups),
                     sizeof(numRowGroups));

    for (const auto &rg : rowGroups_) {
      writeFile_.write(reinterpret_cast<const char *>(&rg.offset),
                       sizeof(rg.offset));
      writeFile_.write(reinterpret_cast<const char *>(&rg.size),
                       sizeof(rg.size));
      writeFile_.write(reinterpret_cast<const char *>(&rg.numRows),
                       sizeof(rg.numRows));

      // Write column offsets
      uint32_t numCols = rg.columnOffsets.size();
      writeFile_.write(reinterpret_cast<const char *>(&numCols),
                       sizeof(numCols));
      for (uint64_t offset : rg.columnOffsets) {
        writeFile_.write(reinterpret_cast<const char *>(&offset),
                         sizeof(offset));
      }

      // Write column stats (simplified - just min/max for INT64)
      for (const auto &stats : rg.columnStats) {
        writeFile_.write(reinterpret_cast<const char *>(&stats.minInt),
                         sizeof(stats.minInt));
        writeFile_.write(reinterpret_cast<const char *>(&stats.maxInt),
                         sizeof(stats.maxInt));
        writeFile_.write(reinterpret_cast<const char *>(&stats.totalCount),
                         sizeof(stats.totalCount));
      }
    }

    // Update header
    writeFile_.seekp(0);
    writeFile_.write(reinterpret_cast<const char *>(&header_), sizeof(header_));

    writeFile_.close();
  }

  // Read a specific column from a row group
  ColumnBlock readColumn(int rowGroupIndex, const std::string &columnName) {
    int colIdx = schema_.getColumnIndex(columnName);
    if (colIdx < 0 || rowGroupIndex < 0 ||
        rowGroupIndex >= static_cast<int>(rowGroups_.size())) {
      return ColumnBlock(ColumnType::STRING); // Return empty block
    }

    std::ifstream file(filePath_, std::ios::binary);
    if (!file.is_open()) {
      return ColumnBlock(ColumnType::STRING);
    }

    const auto &rg = rowGroups_[rowGroupIndex];
    uint64_t columnOffset = rg.offset + rg.columnOffsets[colIdx];

    file.seekg(columnOffset);

    uint32_t columnSize;
    file.read(reinterpret_cast<char *>(&columnSize), sizeof(columnSize));

    std::vector<uint8_t> columnData(columnSize);
    file.read(reinterpret_cast<char *>(columnData.data()), columnSize);

    // Deserialize column (simplified)
    ColumnBlock block(schema_.columns[colIdx].type);
    // Full deserialization would go here

    file.close();
    return block;
  }

  // Query optimization: check if a row group might contain values in range
  bool rowGroupMayContain(int rowGroupIndex, const std::string &column,
                          int64_t minVal, int64_t maxVal) {
    int colIdx = schema_.getColumnIndex(column);
    if (colIdx < 0 || rowGroupIndex < 0 ||
        rowGroupIndex >= static_cast<int>(rowGroups_.size())) {
      return true; // Conservative: assume it might
    }

    const auto &stats = rowGroups_[rowGroupIndex].columnStats[colIdx];

    // Range doesn't overlap with column's min/max
    if (maxVal < stats.minInt || minVal > stats.maxInt) {
      return false;
    }

    return true;
  }

  // Getters
  const TableSchema &getSchema() const { return schema_; }
  size_t getNumRowGroups() const { return rowGroups_.size(); }
  uint64_t getTotalRows() const { return header_.numRows; }
  const std::vector<RowGroupMeta> &getRowGroups() const { return rowGroups_; }
};

} // namespace columnar

#endif // COLUMNAR_FILE_HPP
