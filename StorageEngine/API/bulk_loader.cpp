// bulk_loader.cpp — Insert 1M rows directly via StorageEngine (no HTTP)
// Build: cmake --build build --target bulk_loader
// Run:   ./build/bulk_loader [data_dir]

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "StorageEngine/SQLLayer/includes/query_executor.hpp"
#include "StorageEngine/SQLLayer/includes/schema_registry.hpp"
#include "StorageEngine/includes/storage_engine.hpp"

static const char *REGIONS[] = {"North", "South", "East", "West", "Central"};
static const char *CATEGORIES[] = {"Electronics", "Software", "Hardware",
                                   "Services",    "Support",  "Logistics",
                                   "Marketing",   "Finance"};
static const char *STATUSES[] = {"completed", "pending", "refunded",
                                 "processing", "shipped"};

int main(int argc, char *argv[]) {
  std::string dataDir = "./data";
  int totalRows = 1000000;

  if (argc >= 2)
    dataDir = argv[1];
  if (argc >= 3)
    totalRows = std::atoi(argv[2]);

  std::cout << "\n";
  std::cout << "========================================\n";
  std::cout << "  Samanvay Bulk Loader\n";
  std::cout << "========================================\n";
  std::cout << "  Data directory : " << dataDir << "\n";
  std::cout << "  Target rows    : " << totalRows << "\n";
  std::cout << "========================================\n\n";

  // ── Initialize engine ──────────────────────────────────────────
  storage::StorageEngineConfig config;
  config.dataDirectory = dataDir;
  config.columnarDirectory = dataDir + "/columnar";
  config.walPath = dataDir + "/wal.log";

  storage::StorageEngine engine(config);
  engine.recoverFromWAL();

  sql::SchemaRegistry registry(dataDir + "/schema_registry.sdb");
  sql::QueryExecutor executor(engine, registry);

  // ── Create the transactions table ──────────────────────────────
  std::cout << "[1/3] Creating transactions table...\n";
  auto createResult = executor.execute(
      "CREATE TABLE transactions (id INT PRIMARY KEY, region VARCHAR(50), "
      "category VARCHAR(50), amount DOUBLE, units INT, status VARCHAR(20))");

  if (createResult.success) {
    std::cout << "  OK: Table created\n";
  } else {
    std::cout << "  Note: " << createResult.message << "\n";
  }

  // ── Bulk insert ────────────────────────────────────────────────
  std::cout << "[2/3] Inserting " << totalRows << " rows...\n";

  std::mt19937 rng(42); // Fixed seed for reproducibility
  std::uniform_int_distribution<int> regionDist(0, 4);
  std::uniform_int_distribution<int> categoryDist(0, 7);
  std::uniform_int_distribution<int> statusDist(0, 4);
  std::uniform_int_distribution<int> unitsDist(1, 500);
  std::uniform_real_distribution<double> amountDist(5.0, 9999.99);

  auto startTime = std::chrono::steady_clock::now();
  int successCount = 0;
  int failCount = 0;
  int progressStep = totalRows / 20; // 5% increments

  for (int i = 1; i <= totalRows; i++) {
    const char *region = REGIONS[regionDist(rng)];
    const char *category = CATEGORIES[categoryDist(rng)];
    const char *status = STATUSES[statusDist(rng)];
    int units = unitsDist(rng);
    double amount = amountDist(rng);

    std::ostringstream sql;
    sql << "INSERT INTO transactions VALUES (" << i << ", '" << region << "', '"
        << category << "', " << std::fixed << std::setprecision(2) << amount
        << ", " << units << ", '" << status << "')";

    auto result = executor.execute(sql.str());
    if (result.success) {
      successCount++;
    } else {
      failCount++;
    }

    if (i % progressStep == 0 || i == totalRows) {
      auto now = std::chrono::steady_clock::now();
      double elapsed = std::chrono::duration<double>(now - startTime).count();
      double rate = i / elapsed;
      double eta = (totalRows - i) / rate;

      std::cout << "  [" << std::setw(3) << (i * 100 / totalRows) << "%] " << i
                << "/" << totalRows << " rows  |  " << std::fixed
                << std::setprecision(0) << rate << " rows/sec  |  ETA "
                << std::setprecision(1) << eta << "s\n";
    }
  }

  auto endTime = std::chrono::steady_clock::now();
  double totalElapsed =
      std::chrono::duration<double>(endTime - startTime).count();

  // ── Summary ────────────────────────────────────────────────────
  std::cout << "\n[3/3] Complete!\n";
  std::cout << "========================================\n";
  std::cout << "  Inserted   : " << successCount << " rows\n";
  std::cout << "  Failed     : " << failCount << " rows\n";
  std::cout << "  Time       : " << std::fixed << std::setprecision(2)
            << totalElapsed << " seconds\n";
  std::cout << "  Throughput : " << std::setprecision(0)
            << (successCount / totalElapsed) << " rows/sec\n";
  std::cout << "========================================\n\n";

  std::cout << "Data persisted to: " << dataDir << "\n";
  std::cout << "Restart the API server to pick up the new data.\n\n";

  return 0;
}
