#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

// include your memtable
#include "../includes/memtable.hpp"

class ReadHeavyBenchmark {
private:
  static constexpr int NUM_THREADS = 8;
  static constexpr int PRELOAD_KEYS = 10'000'000; // 10M
  static constexpr int TOTAL_READS = 50'000'000;  // 50M

  memTable::MemtableManager manager;
  std::atomic<long long> total_reads{0};
  std::atomic<long long> found_reads{0};

  std::string generateKey(int id) { return "key_" + std::to_string(id); }

  std::string generateValue(int id) { return "val_" + std::to_string(id); }

  void printProgress(long long current, long long total,
                     const std::string &label) {
    if (current % (total / 100) == 0 || current == total) {
      double pct = (double)current / total * 100.0;
      std::cout << "\r[" << label << "] " << std::fixed << std::setprecision(1)
                << pct << "% (" << current << "/" << total << ")" << std::flush;
    }
  }

public:
  ReadHeavyBenchmark(size_t memtableSizeMB = 1024)
      : manager(memtableSizeMB * 1024 * 1024) {}

  void preload() {
    std::cout << "[Preloading " << PRELOAD_KEYS << " keys...]" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < PRELOAD_KEYS; i++) {
      manager.insert(generateKey(i), generateValue(i));
      if (i % (PRELOAD_KEYS / 100) == 0) {
        printProgress(i, PRELOAD_KEYS, "Preload");
      }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();
    std::cout << "\n[Preload done in " << ms << " ms]" << std::endl;
  }

  void runBenchmark() {
    std::cout << "[Running " << TOTAL_READS << " random lookups with "
              << NUM_THREADS << " threads...]" << std::endl;

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> threads;

    // spawn readers
    for (int t = 0; t < NUM_THREADS; t++) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(1000 + t);
        std::uniform_int_distribution<int> dis(0, PRELOAD_KEYS - 1);

        long long ops = TOTAL_READS / NUM_THREADS;
        for (long long i = 0; i < ops; i++) {
          int key_id = dis(gen);
          auto res = manager.search(generateKey(key_id));

          if (res.has_value())
            found_reads++;
          total_reads++;

          if (t == 0 && i % (ops / 100) == 0) {
            printProgress(total_reads.load(), TOTAL_READS, "Read-Heavy");
          }
        }
      });
    }

    for (auto &th : threads)
      th.join();

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                  .count();

    std::cout << "\n=== Scaled Read-Heavy Benchmark Results ===" << std::endl;
    std::cout << "Total reads performed : " << total_reads.load() << std::endl;
    std::cout << "Found (non-empty)     : " << found_reads.load() << std::endl;
    std::cout << "Duration              : " << ms << " ms" << std::endl;
    std::cout << "Ops/sec               : "
              << (double)TOTAL_READS / (ms / 1000.0) << std::endl;
    std::cout << "Memory usage          : " << std::fixed
              << std::setprecision(2)
              << (double)manager.getCurrentMemoryUsage() / (1024 * 1024)
              << " MB" << std::endl;
    std::cout << "==========================================" << std::endl;
  }
};

int main() {
  ReadHeavyBenchmark bench(2048); // 2GB memtable for 10M preload
  bench.preload();
  bench.runBenchmark();
  return 0;
}
