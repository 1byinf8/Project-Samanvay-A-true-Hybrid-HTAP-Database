// hybrid_query_router.hpp - Query router for OLTP vs OLAP workloads
#pragma once
#ifndef HYBRID_QUERY_ROUTER_HPP
#define HYBRID_QUERY_ROUTER_HPP

#include <functional>
#include <limits>
#include <map>
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

  // Filter pushdown hints (set by SQL layer from WHERE clause)
  std::string filterColumn;               // Column being filtered on
  bool hasWhereFilter = false;            // True if a WHERE clause exists
  double selectivityHint = 1.0;           // Estimated fraction of rows matching (0.0-1.0)

  QueryRequest()
      : type(QueryType::POINT_LOOKUP), aggType(AggregationType::NONE),
        hasWhereFilter(false), selectivityHint(1.0) {}

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

  // Cost model output
  size_t estimatedRows = 0;      // Planner's row count estimate
  double estimatedCostIO = 0.0;  // Estimated I/O units (1 unit = 1 SSTable read)

  QueryPlan()
      : type(QueryType::POINT_LOOKUP), useColumnarPath(false), useRowPath(true),
        filterMinValue(0), filterMaxValue(0), hasFilter(false),
        scanMemtable(true), scanRowSSTables(true), scanColumnarFiles(false),
        estimatedRows(0), estimatedCostIO(0.0) {}
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
  double sumResult;
  double avgResult;
  double minResult;
  double maxResult;

  // Performance metrics
  uint64_t executionTimeMs;
  size_t rowsScanned;
  size_t bytesRead;

  QueryResult()
      : success(false), found(false), countResult(0), sumResult(0.0),
        avgResult(0.0), minResult(std::numeric_limits<double>::max()), 
        maxResult(std::numeric_limits<double>::lowest()), 
        executionTimeMs(0), rowsScanned(0), bytesRead(0) {}

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

  // Estimate row count for a query, accounting for key overlap across levels.
  // Uses max-per-level instead of sum to avoid massive over-counting
  // (since the same key can appear in multiple levels before compaction).
  size_t estimateRowCount(const QueryRequest &request) const {
    if (request.type == QueryType::POINT_LOOKUP) {
      return 1;
    }

    if (request.type == QueryType::FULL_SCAN ||
        request.type == QueryType::AGGREGATION) {
      // Take the max entry count across levels as a baseline
      // (keys in higher levels are duplicates of lower-level keys)
      size_t maxLevelEntries = 0;
      size_t totalEntries = 0;
      auto stats = lsmManager_->getAllLevelStats();
      for (const auto &s : stats) {
        if (s.totalEntries > maxLevelEntries)
          maxLevelEntries = s.totalEntries;
        totalEntries += s.totalEntries;
      }
      // Heuristic: use max(maxLevel, total/2) to balance between
      // over-counting and under-counting
      size_t estimate = std::max(maxLevelEntries, totalEntries / 2);

      // Apply selectivity hint if a WHERE filter is present
      if (request.hasWhereFilter && request.selectivityHint < 1.0) {
        estimate = static_cast<size_t>(estimate * request.selectivityHint);
      }
      return estimate;
    }

    if (request.type == QueryType::RANGE_SCAN) {
      auto sstables =
          lsmManager_->getSSTablesForRange(request.startKey, request.endKey);

      // Group by level and take max to avoid over-counting overlapping keys
      std::map<int, size_t> perLevelEntries;
      for (const auto &meta : sstables) {
        perLevelEntries[meta.level] += meta.entryCount;
      }
      size_t maxLevel = 0;
      size_t total = 0;
      for (const auto &[level, count] : perLevelEntries) {
        if (count > maxLevel)
          maxLevel = count;
        total += count;
      }
      size_t estimate = std::max(maxLevel, total / 2);

      // Apply selectivity for WHERE on non-PK columns
      if (request.hasWhereFilter && request.selectivityHint < 1.0) {
        estimate = static_cast<size_t>(estimate * request.selectivityHint);
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
    size_t estRows = estimateRowCount(request);
    plan.estimatedRows = estRows;

    // Propagate filter info from request to plan
    if (request.hasWhereFilter) {
      plan.hasFilter = true;
      plan.filterColumn = request.filterColumn;
    }

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
      // Cost: 1 memtable lookup + up to N SSTable lookups (bloom filter amortized)
      plan.estimatedCostIO = 1.0 + plan.rowLevelsToScan.size() * 0.5;
      break;

    case QueryType::AGGREGATION:
      // Aggregations need ALL data — both row levels and columnar levels
      plan.useColumnarPath = true;
      plan.useRowPath = true;
      plan.scanMemtable = true;
      plan.scanRowSSTables = true;
      plan.scanColumnarFiles = true;

      // Include row levels (L0-L3) for fresh un-compacted data
      for (int i = 0; i < config.columnarLevelThreshold; ++i) {
        plan.rowLevelsToScan.push_back(i);
      }
      // Include columnar levels (L4+) for compacted data
      for (int i = config.columnarLevelThreshold; i < config.maxLevels; ++i) {
        plan.columnarLevelsToScan.push_back(i);
      }
      plan.estimatedCostIO = plan.rowLevelsToScan.size() +
                             plan.columnarLevelsToScan.size() * 0.3;
      break;

    case QueryType::FULL_SCAN:
      // Full scan needs both paths for complete data
      plan.useColumnarPath = true;
      plan.useRowPath = true;
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
      plan.estimatedCostIO = plan.rowLevelsToScan.size() +
                             plan.columnarLevelsToScan.size();
      break;

    case QueryType::RANGE_SCAN:
    case QueryType::PREFIX_SCAN: {
      if (estRows > columnarThreshold_) {
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
      plan.estimatedCostIO = plan.rowLevelsToScan.size() +
                             plan.columnarLevelsToScan.size();
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

    // Estimated rows & cost
    explanation += "  Estimated Rows: " +
                   std::to_string(plan.estimatedRows) + "\n";
    explanation += "  Estimated Cost (I/O units): " +
                   std::to_string(plan.estimatedCostIO) + "\n";

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

    if (plan.hasFilter) {
      explanation += "  Filter Pushdown: YES";
      if (!plan.filterColumn.empty())
        explanation += " (column: " + plan.filterColumn + ")";
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
