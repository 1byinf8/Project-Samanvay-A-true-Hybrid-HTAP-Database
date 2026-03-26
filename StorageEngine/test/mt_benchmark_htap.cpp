#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "../includes/storage_engine.hpp"
#include "../SQLLayer/includes/query_executor.hpp"
#include "../SQLLayer/includes/row_codec.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"

using namespace std::chrono;

const std::string DB_DIR = "mt_benchmark_htap_data";

void printHeader(const std::string &title) {
  std::cout << "\n======================================================\n"
            << title << "\n"
            << "======================================================\n";
}

int main(int argc, char *argv[]) {
  size_t NUM_ROWS = 100000;      // 100k rows
  int TOTAL_DURATION_SEC = 2;    // Run for 2 seconds per phase to prevent OOM
  
  if (argc > 1) {
    NUM_ROWS = std::stoull(argv[1]);
  }

  // Detect Hardware Concurrency
  unsigned int numThreads = std::thread::hardware_concurrency();
  if (numThreads == 0) numThreads = 4; // Fallback

  printHeader("Samanvay Auto-Scaling Multi-Threaded HTAP Benchmark");
  std::cout << "Target Rows  : " << NUM_ROWS << "\n";
  std::cout << "Data Dir     : " << DB_DIR << "\n";
  std::cout << "CPU Cores    : " << numThreads << "\n";
  std::cout << "Phase Timing : " << TOTAL_DURATION_SEC << " seconds per test\n\n";

  std::string rmCmd = "rm -rf " + DB_DIR;
  system(rmCmd.c_str());
  std::string mkdirCmd = "mkdir -p " + DB_DIR + "/columnar";
  system(mkdirCmd.c_str());

  // 1. Initialize Engine & Executor
  storage::StorageEngineConfig cfg;
  cfg.dataDirectory = DB_DIR;
  cfg.columnarDirectory = DB_DIR + "/columnar";
  cfg.walPath = DB_DIR + "/wal.log";

  auto engine = std::make_shared<storage::StorageEngine>(cfg);
  sql::SchemaRegistry registry(DB_DIR + "/schema.sdb");
  
  // NOTE: Hyrise SQLParser might have locking overhead per parse, but we test the entire executor path.
  sql::QueryExecutor executor(*engine, registry);

  std::cout << "Initializing Schema...\n";
  auto res = executor.execute(
      "CREATE TABLE mt_bench_table (id INT PRIMARY KEY, category_id INT, "
      "status VARCHAR(20), amount DOUBLE)");
  if (!res.success) {
    std::cerr << "Failed to create table: " << res.errorMessage << "\n";
    return 1;
  }
  
  auto schemaOpt = registry.getSchema("mt_bench_table");
  if (!schemaOpt) {
      std::cerr << "Failed to load schema for insertion.\n";
      return 1;
  }
  auto schema = schemaOpt.value();

  // 2. Generate Data via raw StorageEngine
  std::cout << "Loading " << NUM_ROWS << " rows directly...\n";
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> catDist(1, 100);
  std::uniform_real_distribution<> amtDist(10.0, 5000.0);
  const std::vector<std::string> statuses = {"active", "inactive", "pending", "archived"};

  size_t batchSize = 100000;
  std::vector<std::pair<std::string, std::string>> batch;
  batch.reserve(batchSize);

  for (size_t i = 1; i <= NUM_ROWS; ++i) {
    std::string key = "mt_bench_table#" + std::to_string(i);

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

  std::cout << "Flushing Memtables & Compacting...\n\n";
  engine->forceFlush();
  engine->triggerCompaction();
  
  // 3. Multi-Threaded Execution Phases
  
  auto runConcurrentTest = [&](const std::string& name, const std::string& sqlQuery, unsigned int testThreads) {
      std::atomic<bool> keep_running{true};
      std::atomic<uint64_t> total_queries{0};
      
      std::cout << "--- " << name << " ---\n";
      std::cout << "Running '" << sqlQuery << "' on " << testThreads << " threads for " << TOTAL_DURATION_SEC << "s...\n";
      
      std::vector<std::thread> workers;
      for (unsigned int t = 0; t < testThreads; ++t) {
          workers.emplace_back([&]() {
              sql::QueryExecutor localExecutor(*engine, registry);
              uint64_t my_queries = 0;
              while (keep_running.load(std::memory_order_relaxed)) {
                  auto result = localExecutor.execute(sqlQuery);
                  if (result.success) {
                      my_queries++;
                  }
              }
              total_queries.fetch_add(my_queries, std::memory_order_relaxed);
          });
      }
      
      std::this_thread::sleep_for(std::chrono::seconds(TOTAL_DURATION_SEC));
      keep_running.store(false, std::memory_order_relaxed);
      
      for (auto& w : workers) {
          if (w.joinable()) w.join();
      }
      
      double opsSec = static_cast<double>(total_queries.load()) / TOTAL_DURATION_SEC;
      std::cout << "  Total Queries Executed: " << total_queries.load() << "\n";
      std::cout << "  Concurrent Ops/Sec    : " << opsSec << "\n\n";
      
      std::this_thread::sleep_for(std::chrono::seconds(1));
  };
  
  // Run tests
  printHeader("Running Multi-Threaded Scans & Lookups");
  
  std::string sqlPoint = "SELECT * FROM mt_bench_table WHERE id = " + std::to_string(NUM_ROWS / 2);
  runConcurrentTest("OLTP: High-Concurrency Point Lookups", sqlPoint, numThreads);
  
  unsigned int olapThreads = std::max(1u, numThreads / 4);
  
  std::string sqlAgg = "SELECT SUM(amount) FROM mt_bench_table";
  runConcurrentTest("OLAP: Concurrent Full Table Scans (Resource Pooled)", sqlAgg, olapThreads);

  std::string sqlFilterAgg = "SELECT SUM(amount) FROM mt_bench_table WHERE status = 'active'";
  runConcurrentTest("HTAP: Concurrent Filtered Range Scans (Resource Pooled)", sqlFilterAgg, olapThreads);

  std::cout << "Cleaning up...\n";
  system(rmCmd.c_str());
  std::cout << "Done.\n";
  
  return 0;
}
