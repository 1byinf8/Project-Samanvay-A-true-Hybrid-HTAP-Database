// json_helpers.hpp — JSON serialization for SQL layer types
// Converts ResultSet, TableSchema, StorageEngineStats → nlohmann::json
#pragma once
#ifndef JSON_HELPERS_HPP
#define JSON_HELPERS_HPP

#include <string>
#include <vector>

#include "nlohmann/json.hpp"

// SQL layer types
#include "../SQLLayer/includes/result_formatter.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"
#include "../includes/storage_engine.hpp"

namespace api {

using json = nlohmann::json;

// ─── Consistent JSON Envelope ────────────────────────────────────────
inline json successResponse(const json &data) {
  return {{"success", true}, {"data", data}, {"error", nullptr}};
}

inline json errorResponse(const std::string &message,
                          const std::string &type = "ERROR") {
  return {{"success", false},
          {"data", nullptr},
          {"error", {{"message", message}, {"type", type}}}};
}

// ─── ResultSet → JSON ────────────────────────────────────────────────
inline json resultSetToJson(const sql::ResultSet &rs) {
  if (!rs.success) {
    return errorResponse(rs.errorMessage, "QUERY_ERROR");
  }

  json data;
  data["headers"] = rs.headers;
  data["rows"] = rs.rows;
  data["rowsAffected"] = rs.rowsAffected;
  data["executionTimeMs"] = rs.executionTimeMs;

  // Include status message for DML operations (CREATE, DROP, etc.)
  if (!rs.errorMessage.empty() && rs.success) {
    data["message"] = rs.errorMessage;
  }

  return successResponse(data);
}

// ─── TableSchema → JSON ─────────────────────────────────────────────
inline json schemaToJson(const columnar::TableSchema &schema) {
  json cols = json::array();
  for (const auto &col : schema.columns) {
    cols.push_back({
        {"name", col.name},
        {"type", sql::SchemaRegistry::columnTypeToString(col.type)},
        {"nullable", col.nullable},
    });
  }

  return {
      {"tableName", schema.tableName},
      {"primaryKey", schema.primaryKeyColumn},
      {"columns", cols},
      {"columnCount", schema.columns.size()},
  };
}

// ─── StorageEngineStats → JSON ───────────────────────────────────────
inline json statsToJson(const storage::StorageEngineStats &stats) {
  json levels = json::array();
  for (const auto &ls : stats.levelStats) {
    levels.push_back({
        {"level", ls.level},
        {"numSSTables", ls.numSSTables},
        {"totalSize", ls.totalSize},
        {"totalEntries", ls.totalEntries},
    });
  }

  return {
      {"memory",
       {{"activeMemtables", stats.activeMemtables},
        {"frozenMemtables", stats.frozenMemtables},
        {"memtableMemoryUsage", stats.memtableMemoryUsage}}},
      {"sstables",
       {{"totalCount", stats.totalSSTables},
        {"totalBytes", stats.totalSSTableBytes},
        {"levels", levels}}},
      {"columnar",
       {{"files", stats.columnarFiles}, {"rows", stats.columnarRows}}},
      {"operations",
       {{"totalWrites", stats.totalWrites},
        {"totalReads", stats.totalReads},
        {"totalRangeQueries", stats.totalRangeQueries},
        {"totalCompactions", stats.totalCompactions}}},
      {"performance",
       {{"avgReadLatencyUs", stats.avgReadLatencyUs},
        {"avgWriteLatencyUs", stats.avgWriteLatencyUs}}},
  };
}

} // namespace api

#endif // JSON_HELPERS_HPP
