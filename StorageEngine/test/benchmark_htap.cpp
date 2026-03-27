#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "../includes/storage_engine.hpp"
#include "../SQLLayer/includes/query_executor.hpp"
#include "../SQLLayer/includes/row_codec.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"

using namespace std::chrono;

// Configuration
const std::string DB_DIR = "benchmark_htap_data";

void printHeader(const std::string &title) {
  std::cout << "\n======================================================\n"
            << title << "\n"
            << "======================================================\n";
}

void timeQuery(sql::QueryExecutor &executor, const std::string &queryDesc,
               const std::string &sql) {
  std::cout << "Executing: " << queryDesc << "\nSQL: " << sql << "\n";

  auto start = high_resolution_clock::now();
  auto result = executor.execute(sql);
  auto end = high_resolution_clock::now();

  auto duration = duration_cast<milliseconds>(end - start).count();

  if (!result.success) {
    std::cerr << "  [FAIL] Query failed: " << result.errorMessage << "\n\n";
    return;
  }

  std::cout << "  Time Taken : " << duration << " ms\n";
  std::cout << "  Rows Found : " << result.rows.size() << "\n";
  if (!result.rows.empty() && !result.rows[0].empty()) {
    std::cout << "  Sample Res : " << result.rows[0][0] << "\n";
  }
  std::cout << "\n";
}

int main(int argc, char *argv[]) {
  size_t NUM_ROWS = 1000000; // 1 Million default
  if (argc > 1) {
    NUM_ROWS = std::stoull(argv[1]);
  }

  printHeader("Samanvay HTAP Query Benchmark");
  std::cout << "Target Rows : " << NUM_ROWS << "\n";
  std::cout << "Data Dir    : " << DB_DIR << "\n\n";

  // Clean old DB
  std::filesystem::remove_all(DB_DIR);
  std::filesystem::create_directories(DB_DIR + "/columnar");

  // 1. Initialize Engine & Executor
  storage::StorageEngineConfig cfg;
  cfg.dataDirectory = DB_DIR;
  cfg.columnarDirectory = DB_DIR + "/columnar";
  cfg.walPath = DB_DIR + "/wal.log";

  auto engine = std::make_shared<storage::StorageEngine>(cfg);
  sql::SchemaRegistry registry(DB_DIR + "/schema.sdb");
  sql::QueryExecutor executor(*engine, registry);

  std::cout << "Initializing Schema...\n";
  auto res = executor.execute(
      "CREATE TABLE htap_bench_table (id INT PRIMARY KEY, category_id INT, "
      "status VARCHAR(20), amount DOUBLE)");
  if (!res.success) {
    std::cerr << "Failed to create table: " << res.errorMessage << "\n";
    return 1;
  }
  
  auto schemaOpt = registry.getSchema("htap_bench_table");
  if (!schemaOpt) {
      std::cerr << "Failed to load schema for insertion.\n";
      return 1;
  }
  auto schema = schemaOpt.value();

  // 2. Generate Data via raw StorageEngine to bypass SQL parsing overhead
  std::cout << "Generating and inserting " << NUM_ROWS << " rows directly...\n";
  auto startInsert = high_resolution_clock::now();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> catDist(1, 100);
  std::uniform_real_distribution<> amtDist(10.0, 5000.0);
  const std::vector<std::string> statuses = {"active", "inactive", "pending",
                                             "archived"};

  size_t batchSize = 100000;
  std::vector<std::pair<std::string, std::string>> batch;
  batch.reserve(batchSize);

  for (size_t i = 1; i <= NUM_ROWS; ++i) {
    std::string key = "htap_bench_table#" + std::to_string(i);

    sql::Row rowData = {
        {"id", std::to_string(i)},
        {"category_id", std::to_string(catDist(gen))},
        {"status", statuses[i % statuses.size()]},
        {"amount", std::to_string(amtDist(gen))}
    };

    std::string val = sql::RowCodec::encode(schema, rowData);
    batch.push_back({key, val});

    if (batch.size() == batchSize) {
      engine->batchPut(batch);
      batch.clear();
      std::cout << "  Inserted " << i << " rows...\r" << std::flush;
    }
  }
  if (!batch.empty()) {
    engine->batchPut(batch);
  }
  std::cout << "  Inserted " << NUM_ROWS << " rows.      \n";

  // Flush and Compact to push data to Columnar levels
  std::cout << "Flushing Memtables & Compacting to Columnar Format...\n";
  engine->forceFlush();
  engine->triggerCompaction();
  auto endInsert = high_resolution_clock::now();
  auto insertMs = duration_cast<milliseconds>(endInsert - startInsert).count();
  std::cout << "Data Load & Prep Time: " << insertMs << " ms\n\n";

  printHeader("Running Analytical Queries");

  // Query 1: Full Range Aggregation Count
  timeQuery(executor, "1. Full Table COUNT (Columnar Scan)",
            "SELECT COUNT(*) FROM htap_bench_table");

  // Query 2: Multiple Aggregations
  timeQuery(executor, "2. Full Table SUM & AVG (Mathematical Scan)",
            "SELECT SUM(amount), AVG(amount) FROM htap_bench_table");

  // Query 3: Range Filtered Aggregation
  timeQuery(executor, "3. Filtered SUM via Range Scan (Status = active)",
            "SELECT SUM(amount) FROM htap_bench_table WHERE status = 'active'");

  // Query 4: Point Lookup via Index (PK)
  timeQuery(executor, "4. Single Point Lookup via LSM Index",
            "SELECT * FROM htap_bench_table WHERE id = 50000");

  std::cout << "Cleaning up benchmark data...\n";
  std::filesystem::remove_all(DB_DIR);
  std::cout << "Done.\n";
  return 0;
}
