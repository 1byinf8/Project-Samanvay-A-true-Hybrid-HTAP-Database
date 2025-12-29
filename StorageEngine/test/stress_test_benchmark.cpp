// stress_test_benchmark.cpp - Stress test and benchmarks for HTAP database
// Compile: g++ -std=c++17 -O3 -pthread -o stress_test stress_test_benchmark.cpp
// Run: ./stress_test [num_records] [num_threads]
// Example: ./stress_test 1000000 8

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Include HTAP components
#include "../includes/columnar_file.hpp"
#include "../includes/columnar_format.hpp"
#include "../includes/lsm_levels.hpp"
#include "../includes/skiplist.hpp"
#include "../includes/sstable.hpp"
#include "../includes/wal.hpp"

// Define the thread_local static for skiplist
thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());

// Benchmark result structure
struct BenchmarkResult {
  std::string name;
  size_t operations;
  double durationMs;
  double opsPerSecond;
  double throughputMBps;
  size_t dataBytes;

  void print() const {
    std::cout << std::left << std::setw(35) << name << " | " << std::right
              << std::setw(12) << operations << " ops | " << std::setw(10)
              << std::fixed << std::setprecision(2) << durationMs << " ms | "
              << std::setw(12) << std::fixed << std::setprecision(0)
              << opsPerSecond << " ops/s | " << std::setw(10) << std::fixed
              << std::setprecision(2) << throughputMBps << " MB/s" << std::endl;
  }
};

class StressTestBenchmark {
private:
  size_t numRecords_;
  size_t numThreads_;
  std::vector<BenchmarkResult> results_;
  std::atomic<uint64_t> globalSequence_{0};
  std::mutex printMutex_;

  // Generate random string of given length
  std::string randomString(size_t length, std::mt19937 &gen) {
    static const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      result += alphanum[dist(gen)];
    }
    return result;
  }

  // Generate key with fixed format for ordering
  std::string generateKey(size_t index) {
    std::ostringstream oss;
    oss << "key_" << std::setfill('0') << std::setw(12) << index;
    return oss.str();
  }

  // Generate value of specified size
  std::string generateValue(size_t index, size_t valueSize, std::mt19937 &gen) {
    std::ostringstream oss;
    oss << "value_" << index << "_" << randomString(valueSize - 20, gen);
    return oss.str();
  }

public:
  StressTestBenchmark(size_t numRecords = 1000000, size_t numThreads = 8)
      : numRecords_(numRecords), numThreads_(numThreads) {}

  // ==================== Skiplist Concurrent Write Benchmark
  // ====================
  BenchmarkResult benchmarkSkiplistConcurrentWrites() {
    skiplist::Skiplist list;
    std::atomic<size_t> completedOps{0};
    size_t opsPerThread = numRecords_ / numThreads_;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < numThreads_; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(t);
        size_t startIdx = t * opsPerThread;
        size_t endIdx =
            (t == numThreads_ - 1) ? numRecords_ : startIdx + opsPerThread;

        for (size_t i = startIdx; i < endIdx; ++i) {
          std::string key = generateKey(i);
          std::string value = generateValue(i, 100, gen);
          uint64_t seq = globalSequence_.fetch_add(1);
          skiplist::Entry entry(key, value, seq, false);
          list.add(entry);
          completedOps++;
        }
      });
    }

    for (auto &t : threads)
      t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    size_t dataBytes = numRecords_ * (17 + 100); // key + value size approx
    BenchmarkResult result{"Skiplist Concurrent Writes (" +
                               std::to_string(numThreads_) + " threads)",
                           numRecords_,
                           durationMs,
                           numRecords_ / (durationMs / 1000.0),
                           (dataBytes / (1024.0 * 1024.0)) /
                               (durationMs / 1000.0),
                           dataBytes};

    results_.push_back(result);
    return result;
  }

  // ==================== Skiplist Concurrent Read Benchmark
  // ====================
  BenchmarkResult benchmarkSkiplistConcurrentReads() {
    skiplist::Skiplist list;
    std::mt19937 gen(42);

    // Pre-populate
    for (size_t i = 0; i < numRecords_; ++i) {
      std::string key = generateKey(i);
      std::string value = generateValue(i, 100, gen);
      skiplist::Entry entry(key, value, i, false);
      list.add(entry);
    }

    std::atomic<size_t> found{0};
    size_t opsPerThread = numRecords_ / numThreads_;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < numThreads_; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 localGen(t + 1000);
        std::uniform_int_distribution<size_t> dist(0, numRecords_ - 1);

        for (size_t i = 0; i < opsPerThread; ++i) {
          std::string key = generateKey(dist(localGen));
          skiplist::Entry searchEntry(key, "", 0, false);
          skiplist::Entry result;
          if (list.search(searchEntry, &result))
            found++;
        }
      });
    }

    for (auto &t : threads)
      t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    BenchmarkResult result{"Skiplist Concurrent Reads (" +
                               std::to_string(numThreads_) + " threads)",
                           numRecords_,
                           durationMs,
                           numRecords_ / (durationMs / 1000.0),
                           0, // Not measuring throughput for reads
                           0};

    results_.push_back(result);
    return result;
  }

  // ==================== SSTable Write Benchmark ====================
  BenchmarkResult benchmarkSSTableWrite() {
    std::vector<std::pair<std::string, skiplist::Entry>> entries;
    entries.reserve(numRecords_);

    std::mt19937 gen(42);

    // Generate sorted entries
    for (size_t i = 0; i < numRecords_; ++i) {
      std::string key = generateKey(i);
      std::string value = generateValue(i, 100, gen);
      entries.emplace_back(key, skiplist::Entry(key, value, i, false));
    }

    std::string sstPath = "benchmark_sstable.sst";
    sstable::SSTable sst(sstPath);

    auto start = std::chrono::high_resolution_clock::now();
    bool success = sst.create(entries);
    auto end = std::chrono::high_resolution_clock::now();

    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();
    size_t dataBytes = numRecords_ * (17 + 100);

    BenchmarkResult result{"SSTable Sequential Write",
                           numRecords_,
                           durationMs,
                           numRecords_ / (durationMs / 1000.0),
                           (dataBytes / (1024.0 * 1024.0)) /
                               (durationMs / 1000.0),
                           dataBytes};

    results_.push_back(result);

    // Clean up
    std::remove(sstPath.c_str());

    return result;
  }

  // ==================== Columnar Write Benchmark ====================
  BenchmarkResult benchmarkColumnarWrite() {
    // Create schema
    columnar::TableSchema schema("benchmark_table");
    schema.addColumn(
        columnar::ColumnSchema("id", columnar::ColumnType::INT64, false));
    schema.addColumn(
        columnar::ColumnSchema("name", columnar::ColumnType::STRING, true));
    schema.addColumn(
        columnar::ColumnSchema("amount", columnar::ColumnType::DOUBLE, true));
    schema.addColumn(columnar::ColumnSchema(
        "timestamp", columnar::ColumnType::TIMESTAMP, true));

    std::string colPath = "benchmark_columnar.col";
    std::mt19937 gen(42);

    auto start = std::chrono::high_resolution_clock::now();

    columnar::ColumnarFile writer(colPath, schema,
                                  100000); // 100K rows per group
    writer.create();

    for (size_t i = 0; i < numRecords_; ++i) {
      writer.addRow({std::to_string(i), "user_" + std::to_string(i),
                     std::to_string(static_cast<double>(i) * 1.5),
                     std::to_string(1700000000000 + i * 1000)});
    }

    writer.finalize();

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    // Approximate data size
    size_t dataBytes =
        numRecords_ * (8 + 20 + 8 + 8); // id + name + amount + timestamp

    BenchmarkResult result{"Columnar Write (4 columns)",
                           numRecords_,
                           durationMs,
                           numRecords_ / (durationMs / 1000.0),
                           (dataBytes / (1024.0 * 1024.0)) /
                               (durationMs / 1000.0),
                           dataBytes};

    results_.push_back(result);

    // Clean up
    std::remove(colPath.c_str());

    return result;
  }

  // ==================== Column Block Aggregation Benchmark
  // ====================
  BenchmarkResult benchmarkColumnarAggregation() {
    columnar::ColumnBlock block(columnar::ColumnType::INT64);

    // Fill with data
    for (size_t i = 0; i < numRecords_; ++i) {
      block.appendInt64(static_cast<int64_t>(i));
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Run multiple aggregations
    int64_t sum = 0, min = 0, max = 0, count = 0;
    for (int iter = 0; iter < 100; ++iter) {
      sum += block.sumInt64();
      min += block.minInt64();
      max += block.maxInt64();
      count += block.count();
    }

    // Prevent optimization
    volatile int64_t result = sum + min + max + count;
    (void)result;

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    size_t totalOps = numRecords_ * 100 * 4; // 4 aggregations, 100 iterations

    BenchmarkResult benchResult{
        "Columnar Aggregation (SUM/MIN/MAX/COUNT x100)",
        totalOps,
        durationMs,
        totalOps / (durationMs / 1000.0),
        (numRecords_ * sizeof(int64_t) * 100 / (1024.0 * 1024.0)) /
            (durationMs / 1000.0),
        numRecords_ * sizeof(int64_t) * 100};

    results_.push_back(benchResult);
    return benchResult;
  }

  // ==================== RLE Compression Benchmark ====================
  BenchmarkResult benchmarkRLECompression() {
    // Generate data with repeating patterns (good for RLE)
    std::vector<int64_t> data;
    data.reserve(numRecords_);

    for (size_t i = 0; i < numRecords_; ++i) {
      data.push_back(i / 1000); // Creates runs of 1000 identical values
    }

    auto start = std::chrono::high_resolution_clock::now();

    auto encoded = columnar::RLEEncoder::encodeInt64(data);
    auto decoded = columnar::RLEEncoder::decodeInt64(encoded);

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    double compressionRatio =
        static_cast<double>(numRecords_ * sizeof(int64_t)) / encoded.size();

    BenchmarkResult result{
        "RLE Encode+Decode (compression: " +
            std::to_string(static_cast<int>(compressionRatio)) + "x)",
        numRecords_,
        durationMs,
        numRecords_ / (durationMs / 1000.0),
        (numRecords_ * sizeof(int64_t) / (1024.0 * 1024.0)) /
            (durationMs / 1000.0),
        numRecords_ * sizeof(int64_t)};

    results_.push_back(result);
    return result;
  }

  // ==================== WAL Write Benchmark ====================
  BenchmarkResult benchmarkWALWrite() {
    std::string walPath = "benchmark_wal.log";
    std::remove(walPath.c_str()); // Clean start

    wal::WAL walLog(walPath);
    std::mt19937 gen(42);

    auto start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < numRecords_; ++i) {
      std::string key = generateKey(i);
      std::string value = generateValue(i, 100, gen);
      walLog.append(i, wal::Operation::INSERT, key, value);
    }

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    size_t dataBytes = numRecords_ * (17 + 100 + 17); // key + value + overhead

    BenchmarkResult result{"WAL Sequential Write (fsync per op)",
                           numRecords_,
                           durationMs,
                           numRecords_ / (durationMs / 1000.0),
                           (dataBytes / (1024.0 * 1024.0)) /
                               (durationMs / 1000.0),
                           dataBytes};

    results_.push_back(result);

    // Clean up
    std::remove(walPath.c_str());

    return result;
  }

  // ==================== LSM Manager Benchmark ====================
  BenchmarkResult benchmarkLSMManager() {
    lsm::LSMConfig config;
    config.maxLevels = 7;
    config.dataDirectory = "./benchmark_data";

    lsm::LSMTreeManager manager(config);

    auto start = std::chrono::high_resolution_clock::now();

    // Simulate adding many SSTables
    for (size_t i = 0; i < numRecords_ / 1000; ++i) {
      std::string minKey = generateKey(i * 1000);
      std::string maxKey = generateKey((i + 1) * 1000 - 1);

      lsm::SSTableMeta meta("sst_" + std::to_string(i) + ".sst", minKey, maxKey,
                            i * 1000, (i + 1) * 1000 - 1,
                            1024 * 1024, // 1MB
                            1000,
                            i % 4, // Distribute across levels 0-3
                            i);

      manager.addSSTable(meta);
    }

    // Query for random keys
    std::mt19937 gen(42);
    std::uniform_int_distribution<size_t> dist(0, numRecords_ - 1);

    size_t queryCount = std::min(numRecords_, size_t(100000));
    for (size_t i = 0; i < queryCount; ++i) {
      std::string key = generateKey(dist(gen));
      auto tables = manager.getSSTablesForKey(key);
      (void)tables;
    }

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    size_t totalOps = numRecords_ / 1000 + queryCount;

    BenchmarkResult result{
        "LSM Manager (add + query SSTables)", totalOps, durationMs,
        totalOps / (durationMs / 1000.0),     0,        0};

    results_.push_back(result);
    return result;
  }

  // ==================== Mixed OLTP/OLAP Workload ====================
  BenchmarkResult benchmarkMixedWorkload() {
    skiplist::Skiplist memtable;
    std::atomic<size_t> writeOps{0};
    std::atomic<size_t> readOps{0};
    std::atomic<size_t> scanOps{0};

    size_t oltp_threads = numThreads_ / 2;            // Half for OLTP
    size_t olap_threads = numThreads_ - oltp_threads; // Half for OLAP

    // Pre-populate some data
    std::mt19937 gen(42);
    for (size_t i = 0; i < numRecords_ / 10; ++i) {
      std::string key = generateKey(i);
      std::string value = generateValue(i, 100, gen);
      skiplist::Entry entry(key, value, i, false);
      memtable.add(entry);
    }

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;

    // OLTP threads (point lookups and writes)
    for (size_t t = 0; t < oltp_threads; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 localGen(t);
        std::uniform_int_distribution<size_t> keyDist(0, numRecords_ - 1);
        std::uniform_int_distribution<int> opDist(0, 1);

        for (size_t i = 0; i < numRecords_ / numThreads_; ++i) {
          if (opDist(localGen) == 0) {
            // Write
            std::string key = generateKey(keyDist(localGen));
            std::string value = generateValue(i, 100, localGen);
            skiplist::Entry entry(key, value, globalSequence_.fetch_add(1),
                                  false);
            memtable.add(entry);
            writeOps++;
          } else {
            // Read
            std::string key = generateKey(keyDist(localGen));
            skiplist::Entry searchEntry(key, "", 0, false);
            skiplist::Entry result;
            memtable.search(searchEntry, &result);
            readOps++;
          }
        }
      });
    }

    // OLAP threads (range scans)
    for (size_t t = 0; t < olap_threads; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 localGen(t + 1000);
        std::uniform_int_distribution<size_t> startDist(0, numRecords_ - 1001);

        for (size_t i = 0; i < numRecords_ / numThreads_ / 100; ++i) {
          size_t startIdx = startDist(localGen);
          std::string startKey = generateKey(startIdx);
          std::string endKey = generateKey(startIdx + 1000);

          auto results = memtable.rangeQuery(startKey, endKey);
          (void)results;
          scanOps++;
        }
      });
    }

    for (auto &t : threads)
      t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double durationMs =
        std::chrono::duration<double, std::milli>(end - start).count();

    size_t totalOps = writeOps + readOps + scanOps;

    std::cout << "  Mixed workload breakdown:" << std::endl;
    std::cout << "    Writes: " << writeOps << std::endl;
    std::cout << "    Reads:  " << readOps << std::endl;
    std::cout << "    Scans:  " << scanOps << std::endl;

    BenchmarkResult result{"Mixed OLTP+OLAP Workload (" +
                               std::to_string(numThreads_) + " threads)",
                           totalOps,
                           durationMs,
                           totalOps / (durationMs / 1000.0),
                           0,
                           0};

    results_.push_back(result);
    return result;
  }

  // Run all benchmarks
  void runAllBenchmarks() {
    std::cout << "\n╔══════════════════════════════════════════════════════════"
                 "═════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║                     HTAP Database Stress Test & Benchmark "
                 "Suite                                        ║"
              << std::endl;
    std::cout << "╠════════════════════════════════════════════════════════════"
                 "═══════════════════════════════════════════╣"
              << std::endl;
    std::cout << "║  Records: " << std::setw(12) << numRecords_
              << "   |   Threads: " << std::setw(3) << numThreads_
              << "                                                     ║"
              << std::endl;
    std::cout << "╚════════════════════════════════════════════════════════════"
                 "═══════════════════════════════════════════╝"
              << std::endl;

    std::cout << "\n[1/8] Skiplist Concurrent Writes..." << std::endl;
    benchmarkSkiplistConcurrentWrites().print();

    std::cout << "\n[2/8] Skiplist Concurrent Reads..." << std::endl;
    benchmarkSkiplistConcurrentReads().print();

    std::cout << "\n[3/8] SSTable Sequential Write..." << std::endl;
    benchmarkSSTableWrite().print();

    std::cout << "\n[4/8] Columnar Write..." << std::endl;
    benchmarkColumnarWrite().print();

    std::cout << "\n[5/8] Columnar Aggregation..." << std::endl;
    benchmarkColumnarAggregation().print();

    std::cout << "\n[6/8] RLE Compression..." << std::endl;
    benchmarkRLECompression().print();

    std::cout << "\n[7/8] LSM Manager Operations..." << std::endl;
    benchmarkLSMManager().print();

    // WAL is slow with fsync, use fewer records
    size_t originalRecords = numRecords_;
    numRecords_ = std::min(numRecords_, size_t(100000));
    std::cout << "\n[8/8] WAL Write (limited to " << numRecords_
              << " for fsync)..." << std::endl;
    benchmarkWALWrite().print();
    numRecords_ = originalRecords;

    std::cout << "\n[BONUS] Mixed OLTP+OLAP Workload..." << std::endl;
    benchmarkMixedWorkload().print();

    // Print summary
    printSummary();
  }

  void printSummary() {
    std::cout << "\n╔══════════════════════════════════════════════════════════"
                 "═════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║                                    BENCHMARK SUMMARY       "
                 "                                            ║"
              << std::endl;
    std::cout << "╠════════════════════════════════════════════════════════════"
                 "═══════════════════════════════════════════╣"
              << std::endl;
    std::cout << "║ " << std::left << std::setw(35) << "Benchmark" << " | "
              << std::right << std::setw(12) << "Operations" << " | "
              << std::setw(10) << "Time (ms)" << " | " << std::setw(12)
              << "Ops/sec" << " | " << std::setw(10) << "MB/s" << " ║"
              << std::endl;
    std::cout << "╠════════════════════════════════════════════════════════════"
                 "═══════════════════════════════════════════╣"
              << std::endl;

    for (const auto &result : results_) {
      std::cout << "║ " << std::left << std::setw(35) << result.name << " | "
                << std::right << std::setw(12) << result.operations << " | "
                << std::setw(10) << std::fixed << std::setprecision(1)
                << result.durationMs << " | " << std::setw(12) << std::fixed
                << std::setprecision(0) << result.opsPerSecond << " | "
                << std::setw(10) << std::fixed << std::setprecision(2)
                << result.throughputMBps << " ║" << std::endl;
    }

    std::cout << "╚════════════════════════════════════════════════════════════"
                 "═══════════════════════════════════════════╝"
              << std::endl;

    // Find best performers
    if (!results_.empty()) {
      auto maxOps = std::max_element(
          results_.begin(), results_.end(),
          [](const BenchmarkResult &a, const BenchmarkResult &b) {
            return a.opsPerSecond < b.opsPerSecond;
          });

      std::cout << "\n🏆 Highest Throughput: " << maxOps->name << " ("
                << std::fixed << std::setprecision(0) << maxOps->opsPerSecond
                << " ops/sec)" << std::endl;
    }
  }
};

int main(int argc, char *argv[]) {
  size_t numRecords = 1000000; // Default: 1 million
  size_t numThreads = 8;       // Default: 8 threads

  if (argc >= 2) {
    numRecords = std::stoull(argv[1]);
  }
  if (argc >= 3) {
    numThreads = std::stoull(argv[2]);
  }

  std::cout << "═══════════════════════════════════════════════════════════════"
            << std::endl;
  std::cout << "  Project Samanvay - HTAP Database Stress Test" << std::endl;
  std::cout << "═══════════════════════════════════════════════════════════════"
            << std::endl;
  std::cout << "  Records:  " << numRecords << std::endl;
  std::cout << "  Threads:  " << numThreads << std::endl;
  std::cout << "  Hardware: " << std::thread::hardware_concurrency()
            << " cores available" << std::endl;
  std::cout << "═══════════════════════════════════════════════════════════════"
            << std::endl;

  try {
    StressTestBenchmark benchmark(numRecords, numThreads);
    benchmark.runAllBenchmarks();

    std::cout << "\n✅ Stress test completed successfully!\n" << std::endl;
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "❌ Error: " << e.what() << std::endl;
    return 1;
  }
}
