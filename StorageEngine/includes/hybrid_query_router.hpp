// hybrid_query_router.hpp - Query router for OLTP vs OLAP workloads
#pragma once
#ifndef HYBRID_QUERY_ROUTER_HPP
#define HYBRID_QUERY_ROUTER_HPP

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "columnar_file.hpp"
#include "lsm_levels.hpp"

namespace query {

// Query types that influence routing
enum class QueryType {
  POINT_LOOKUP, // Single key get (GET key)
  RANGE_SCAN,   // Range query (SCAN start end)
  FULL_SCAN,    // Full table scan
  AGGREGATION,  // Aggregate query (SUM, COUNT, AVG, MIN, MAX)
  PREFIX_SCAN   // Prefix-based scan
};

// Aggregation types
enum class AggregationType { NONE, COUNT, SUM, AVG, MIN, MAX };

// Query request structure
struct QueryRequest {
  QueryType type;
  std::string key;                        // For point lookup
  std::string startKey;                   // For range/prefix scan
  std::string endKey;                     // For range scan
  std::string columnName;                 // For aggregations
  AggregationType aggType;                // For aggregations
  std::vector<std::string> selectColumns; // Column projection

  QueryRequest()
      : type(QueryType::POINT_LOOKUP), aggType(AggregationType::NONE) {}

  // Builder pattern for query construction
  static QueryRequest pointLookup(const std::string &k) {
    QueryRequest req;
    req.type = QueryType::POINT_LOOKUP;
    req.key = k;
    return req;
  }

  static QueryRequest rangeScan(const std::string &start,
                                const std::string &end) {
    QueryRequest req;
    req.type = QueryType::RANGE_SCAN;
    req.startKey = start;
    req.endKey = end;
    return req;
  }

  static QueryRequest fullScan() {
    QueryRequest req;
    req.type = QueryType::FULL_SCAN;
    return req;
  }

  static QueryRequest aggregation(AggregationType agg,
                                  const std::string &column) {
    QueryRequest req;
    req.type = QueryType::AGGREGATION;
    req.aggType = agg;
    req.columnName = column;
    return req;
  }
};

// Query execution plan
struct QueryPlan {
  QueryType type;
  bool useColumnarPath;                   // If true, use columnar storage
  bool useRowPath;                        // If true, use row-oriented storage
  std::vector<std::string> columnsNeeded; // For projection pushdown
  std::string filterColumn;
  int64_t filterMinValue;
  int64_t filterMaxValue;
  bool hasFilter;

  // Source selection
  bool scanMemtable;
  bool scanRowSSTables;
  bool scanColumnarFiles;
  std::vector<int> rowLevelsToScan;      // Which LSM levels (row)
  std::vector<int> columnarLevelsToScan; // Which LSM levels (columnar)

  QueryPlan()
      : type(QueryType::POINT_LOOKUP), useColumnarPath(false), useRowPath(true),
        filterMinValue(0), filterMaxValue(0), hasFilter(false),
        scanMemtable(true), scanRowSSTables(true), scanColumnarFiles(false) {}
};

// Query result structure
struct QueryResult {
  bool success;
  std::string errorMessage;

  // For point lookup
  std::string value;
  bool found;

  // For range/full scan
  std::vector<std::pair<std::string, std::string>> rows;

  // For aggregations
  int64_t countResult;
  int64_t sumResult;
  double avgResult;
  int64_t minResult;
  int64_t maxResult;

  // Performance metrics
  uint64_t executionTimeMs;
  size_t rowsScanned;
  size_t bytesRead;

  QueryResult()
      : success(false), found(false), countResult(0), sumResult(0),
        avgResult(0.0), minResult(0), maxResult(0), executionTimeMs(0),
        rowsScanned(0), bytesRead(0) {}

  static QueryResult ok() {
    QueryResult r;
    r.success = true;
    return r;
  }

  static QueryResult error(const std::string &msg) {
    QueryResult r;
    r.success = false;
    r.errorMessage = msg;
    return r;
  }
};

// Hybrid query router - decides between row and columnar paths
class HybridQueryRouter {
private:
  std::shared_ptr<lsm::LSMTreeManager> lsmManager_;
  size_t columnarThreshold_; // Rows estimate above which to prefer columnar

  // Estimate row count for a range query
  size_t estimateRowCount(const QueryRequest &request) const {
    if (request.type == QueryType::POINT_LOOKUP) {
      return 1;
    }

    if (request.type == QueryType::FULL_SCAN) {
      // Sum up all entries across levels
      size_t total = 0;
      auto stats = lsmManager_->getAllLevelStats();
      for (const auto &s : stats) {
        total += s.totalEntries;
      }
      return total;
    }

    if (request.type == QueryType::RANGE_SCAN) {
      // Get SSTables that overlap the range
      auto sstables =
          lsmManager_->getSSTablesForRange(request.startKey, request.endKey);

      size_t estimate = 0;
      for (const auto &meta : sstables) {
        // Simple estimate: assume uniform distribution
        estimate += meta.entryCount;
      }
      return estimate;
    }

    return 0;
  }

public:
  explicit HybridQueryRouter(std::shared_ptr<lsm::LSMTreeManager> lsmManager,
                             size_t columnarThreshold = 10000)
      : lsmManager_(lsmManager), columnarThreshold_(columnarThreshold) {}

  // Plan a query based on its characteristics
  QueryPlan planQuery(const QueryRequest &request) {
    QueryPlan plan;
    plan.type = request.type;
    plan.columnsNeeded = request.selectColumns;

    const auto &config = lsmManager_->getConfig();

    switch (request.type) {
    case QueryType::POINT_LOOKUP:
      // Always use row store path for point lookups
      plan.useColumnarPath = false;
      plan.useRowPath = true;
      plan.scanMemtable = true;
      plan.scanRowSSTables = true;
      plan.scanColumnarFiles = false;

      // Scan all row-oriented levels
      for (int i = 0; i < config.columnarLevelThreshold; ++i) {
        plan.rowLevelsToScan.push_back(i);
      }
      break;

    case QueryType::AGGREGATION:
      // Prefer columnar for aggregations - can operate on compressed data
      plan.useColumnarPath = true;
      plan.useRowPath = false;  // Skip row store for pure aggregations
      plan.scanMemtable = true; // Still need fresh data
      plan.scanRowSSTables = false;
      plan.scanColumnarFiles = true;

      // Only scan columnar levels
      for (int i = config.columnarLevelThreshold; i < config.maxLevels; ++i) {
        plan.columnarLevelsToScan.push_back(i);
      }
      break;

    case QueryType::FULL_SCAN:
      // Full scan benefits from columnar I/O efficiency
      plan.useColumnarPath = true;
      plan.useRowPath = true; // Need both for complete data
      plan.scanMemtable = true;
      plan.scanRowSSTables = true;
      plan.scanColumnarFiles = true;

      // All levels
      for (int i = 0; i < config.columnarLevelThreshold; ++i) {
        plan.rowLevelsToScan.push_back(i);
      }
      for (int i = config.columnarLevelThreshold; i < config.maxLevels; ++i) {
        plan.columnarLevelsToScan.push_back(i);
      }
      break;

    case QueryType::RANGE_SCAN:
    case QueryType::PREFIX_SCAN: {
      // Hybrid approach based on estimated size
      size_t estimatedRows = estimateRowCount(request);

      if (estimatedRows > columnarThreshold_) {
        // Large range: prefer columnar for deep levels
        plan.useColumnarPath = true;
        plan.useRowPath = true;
        plan.scanColumnarFiles = true;

        for (int i = config.columnarLevelThreshold; i < config.maxLevels; ++i) {
          plan.columnarLevelsToScan.push_back(i);
        }
      } else {
        // Small range: row store is fine
        plan.useColumnarPath = false;
        plan.useRowPath = true;
        plan.scanColumnarFiles = false;
      }

      plan.scanMemtable = true;
      plan.scanRowSSTables = true;

      for (int i = 0; i < config.columnarLevelThreshold; ++i) {
        plan.rowLevelsToScan.push_back(i);
      }
      break;
    }
    }

    return plan;
  }

  // Get a description of the query plan (for debugging/EXPLAIN)
  std::string explainPlan(const QueryPlan &plan) const {
    std::string explanation = "Query Plan:\n";

    switch (plan.type) {
    case QueryType::POINT_LOOKUP:
      explanation += "  Type: POINT_LOOKUP\n";
      break;
    case QueryType::RANGE_SCAN:
      explanation += "  Type: RANGE_SCAN\n";
      break;
    case QueryType::FULL_SCAN:
      explanation += "  Type: FULL_SCAN\n";
      break;
    case QueryType::AGGREGATION:
      explanation += "  Type: AGGREGATION\n";
      break;
    case QueryType::PREFIX_SCAN:
      explanation += "  Type: PREFIX_SCAN\n";
      break;
    }

    explanation += "  Storage Path:\n";
    if (plan.scanMemtable) {
      explanation += "    - Memtable: YES\n";
    }
    if (plan.useRowPath && !plan.rowLevelsToScan.empty()) {
      explanation += "    - Row SSTables (Levels): ";
      for (int l : plan.rowLevelsToScan) {
        explanation += std::to_string(l) + " ";
      }
      explanation += "\n";
    }
    if (plan.useColumnarPath && !plan.columnarLevelsToScan.empty()) {
      explanation += "    - Columnar Files (Levels): ";
      for (int l : plan.columnarLevelsToScan) {
        explanation += std::to_string(l) + " ";
      }
      explanation += "\n";
    }

    if (!plan.columnsNeeded.empty()) {
      explanation += "  Projection: ";
      for (const auto &col : plan.columnsNeeded) {
        explanation += col + " ";
      }
      explanation += "\n";
    }

    return explanation;
  }

  // Set the threshold for preferring columnar storage
  void setColumnarThreshold(size_t threshold) {
    columnarThreshold_ = threshold;
  }

  size_t getColumnarThreshold() const { return columnarThreshold_; }
};

// Simple aggregation executor for columnar data
class ColumnarAggregator {
public:
  // Execute COUNT on a column block
  static int64_t count(const columnar::ColumnBlock &block) {
    return static_cast<int64_t>(block.count());
  }

  // Execute SUM on an INT64 column block
  static int64_t sumInt64(const columnar::ColumnBlock &block) {
    return block.sumInt64();
  }

  // Execute SUM on a DOUBLE column block
  static double sumDouble(const columnar::ColumnBlock &block) {
    return block.sumDouble();
  }

  // Execute MIN on an INT64 column block
  static int64_t minInt64(const columnar::ColumnBlock &block) {
    return block.minInt64();
  }

  // Execute MAX on an INT64 column block
  static int64_t maxInt64(const columnar::ColumnBlock &block) {
    return block.maxInt64();
  }

  // Execute AVG on an INT64 column block
  static double avgInt64(const columnar::ColumnBlock &block) {
    size_t cnt = block.count();
    if (cnt == 0)
      return 0.0;
    return static_cast<double>(block.sumInt64()) / cnt;
  }

  // Execute AVG on a DOUBLE column block
  static double avgDouble(const columnar::ColumnBlock &block) {
    size_t cnt = block.count();
    if (cnt == 0)
      return 0.0;
    return block.sumDouble() / cnt;
  }
};

} // namespace query

#endif // HYBRID_QUERY_ROUTER_HPP
