// bulk_loader.cpp — Insert rows directly via StorageEngine (no HTTP)
// Build: cmake --build build --target bulk_loader
// Run:   ./build/bulk_loader [data_dir] [row_count]

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

// FIX 1: Corrected include paths (file lives at StorageEngine/API/, so
//         SQL layer and includes are one level up via "../")
#include "../SQLLayer/includes/query_executor.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"
#include "../includes/storage_engine.hpp"

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

  // Guard against nonsense row counts
  if (totalRows <= 0) {
    std::cerr << "ERROR: row_count must be > 0\n";
    return 1;
  }

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

  // LSM config must mirror the data directory so generated SSTable
  // paths land in the right place
  config.lsmConfig.dataDirectory = dataDir;

  storage::StorageEngine engine(config);

  // FIX 2: Recover persisted LSM metadata FIRST so previously flushed
  //        SSTables are visible, then replay WAL on top.
  //        Original code only called recoverFromWAL() which meant any
  //        SSTables from a prior run were completely invisible.
  engine.recover();        // load lsm_meta.bin  (SSTable registry)
  engine.recoverFromWAL(); // replay wal.log      (unflushed writes)

  sql::SchemaRegistry registry(dataDir + "/schema_registry.sdb");
  sql::QueryExecutor executor(engine, registry);

  // ── Create the transactions table ──────────────────────────────
  std::cout << "[1/3] Creating transactions table...\n";
  auto createResult = executor.execute("CREATE TABLE transactions ("
                                       "id INT PRIMARY KEY, "
                                       "region VARCHAR(50), "
                                       "category VARCHAR(50), "
                                       "amount DOUBLE, "
                                       "units INT, "
                                       "status VARCHAR(20))");

  if (createResult.success) {
    std::cout << "  OK: Table created\n\n";
  } else {
    // Table probably already exists from a previous run — that is fine
    std::cout << "  Note: " << createResult.errorMessage << "\n\n";
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

  // FIX 3: progressStep must be at least 1 to avoid modulo-by-zero when
  //        totalRows < 20.  Original code: int progressStep = totalRows / 20
  //        which is 0 for small row counts → undefined behaviour on line
  //        "if (i % progressStep == 0)".
  int progressStep = std::max(1, totalRows / 20); // 5% increments, min 1

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

      // FIX 4: Guard against division by zero on ETA when elapsed ~ 0.
      //        Original code computed rate = i/elapsed then eta =
      //        remaining/rate without any protection — crashes or prints
      //        inf/nan in the first few milliseconds.
      double rate = (elapsed > 0.0) ? (i / elapsed) : 0.0;
      double eta = (rate > 0.0) ? ((totalRows - i) / rate) : 0.0;

      std::cout << "  [" << std::setw(3) << (i * 100 / totalRows) << "%] " << i
                << "/" << totalRows << " rows  |  " << std::fixed
                << std::setprecision(0) << rate << " rows/sec"
                << "  |  ETA " << std::setprecision(1) << eta << "s\n";
    }
  }

  auto endTime = std::chrono::steady_clock::now();
  double totalElapsed =
      std::chrono::duration<double>(endTime - startTime).count();

  // FIX 5: Explicitly flush memtables to disk before printing the "done"
  //        message so the caller can safely restart the API server immediately.
  //        Original code relied on the StorageEngine destructor to do this
  //        implicitly, which happens *after* main() returns — meaning the
  //        "Data persisted" message printed below was a lie.
  std::cout << "\n  Flushing memtables to disk...\n";
  engine.forceFlush();
  // Give the background flush thread a moment to finish
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // ── Summary ────────────────────────────────────────────────────
  std::cout << "\n[3/3] Complete!\n";
  std::cout << "========================================\n";
  std::cout << "  Inserted   : " << successCount << " rows\n";
  std::cout << "  Failed     : " << failCount << " rows\n";
  std::cout << "  Time       : " << std::fixed << std::setprecision(2)
            << totalElapsed << " seconds\n";
  std::cout << "  Throughput : " << std::setprecision(0)
            << (totalElapsed > 0.0 ? successCount / totalElapsed : 0.0)
            << " rows/sec\n";
  std::cout << "========================================\n\n";
  std::cout << "Data persisted to : " << dataDir << "\n";
  std::cout << "You can now start the API server.\n\n";

  return 0;
}