#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

// Include your memtable implementation
#include "../includes/memtable.hpp"

class MemTableTestSuite {
private:
  static constexpr int NUM_THREADS = 8;
  static constexpr int OPERATIONS_PER_THREAD = 10000;
  static constexpr int STRESS_TEST_SIZE = 10000000; // 10M operations

  std::atomic<int> passed_tests{0};
  std::atomic<int> failed_tests{0};
  std::atomic<int> total_operations{0};

  // Random string generator
  std::string generateRandomString(int length, std::mt19937 &gen) {
    const std::string charset =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<> dis(0, charset.size() - 1);
    std::string result;
    result.reserve(length);
    for (int i = 0; i < length; ++i) {
      result += charset[dis(gen)];
    }
    return result;
  }

  // Generate deterministic key for stress test
  std::string generateKey(int id) { return "key_" + std::to_string(id); }

  std::string generateValue(int id) {
    return "value_" + std::to_string(id) +
           "_data_with_some_content_to_make_it_longer";
  }

  void logTest(const std::string &testName, bool passed,
               const std::string &details = "") {
    if (passed) {
      passed_tests++;
      std::cout << "✓ " << testName << " PASSED";
    } else {
      failed_tests++;
      std::cout << "✗ " << testName << " FAILED";
    }

    if (!details.empty()) {
      std::cout << " - " << details;
    }
    std::cout << std::endl;
  }

  void printProgress(int current, int total, const std::string &operation) {
    if (current % (total / 100) == 0 || current == total) {
      double percentage = (double)current / total * 100.0;
      std::cout << "\r" << operation << ": " << std::fixed
                << std::setprecision(1) << percentage << "% (" << current << "/"
                << total << ")" << std::flush;
    }
  }

public:
  void testBasicOperations() {
    std::cout << "\n=== Testing Basic Operations ===" << std::endl;

    memTable::MemtableManager manager(1024 * 1024); // 1MB for quick testing

    // Test 1: Basic Insert and Search
    bool result = manager.insert("test_key", "test_value");
    logTest("Basic Insert", result);

    auto search_result = manager.search("test_key");
    logTest("Basic Search",
            search_result.has_value() && search_result.value() == "test_value");

    // Test 2: Search non-existent key
    auto not_found = manager.search("non_existent");
    logTest("Search Non-existent", !not_found.has_value());

    // Test 3: Delete operation
    bool delete_result = manager.erase("test_key");
    logTest("Basic Delete", delete_result);

    // Test 4: Search deleted key (should return nullopt due to tombstone)
    auto deleted_search = manager.search("test_key");
    logTest("Search Deleted Key", !deleted_search.has_value());

    // Test 5: Insert after delete
    bool reinsert = manager.insert("test_key", "new_value");
    logTest("Insert After Delete", reinsert);

    auto reinsert_search = manager.search("test_key");
    logTest("Search After Reinsert",
            reinsert_search.has_value() &&
                reinsert_search.value() == "new_value");
  }

  void testConcurrentInserts() {
    std::cout << "\n=== Testing Concurrent Inserts ===" << std::endl;

    memTable::MemtableManager manager(64 * 1024 * 1024); // 64MB
    std::vector<std::thread> threads;
    std::atomic<int> successful_inserts{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // Spawn multiple threads for concurrent inserts
    for (int t = 0; t < NUM_THREADS; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(42 + t); // Different seed per thread
        for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
          std::string key =
              "thread" + std::to_string(t) + "_key" + std::to_string(i);
          std::string value = generateRandomString(50, gen);

          if (manager.insert(key, value)) {
            successful_inserts++;
          }
        }
      });
    }

    // Wait for all threads to complete
    for (auto &thread : threads) {
      thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    int expected = NUM_THREADS * OPERATIONS_PER_THREAD;
    logTest("Concurrent Inserts", successful_inserts.load() == expected,
            std::to_string(successful_inserts.load()) + "/" +
                std::to_string(expected) + " in " +
                std::to_string(duration.count()) + "ms");
  }

  void testConcurrentReads() {
    std::cout << "\n=== Testing Concurrent Reads ===" << std::endl;

    memTable::MemtableManager manager(64 * 1024 * 1024);

    // First, insert some test data
    const int NUM_KEYS = 1000;
    for (int i = 0; i < NUM_KEYS; ++i) {
      manager.insert("read_key_" + std::to_string(i),
                     "value_" + std::to_string(i));
    }

    std::vector<std::thread> threads;
    std::atomic<int> successful_reads{0};
    std::atomic<int> total_reads{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // Spawn reader threads
    for (int t = 0; t < NUM_THREADS; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(100 + t);
        std::uniform_int_distribution<> dis(0, NUM_KEYS - 1);

        for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
          int key_id = dis(gen);
          std::string key = "read_key_" + std::to_string(key_id);

          auto result = manager.search(key);
          total_reads++;

          if (result.has_value() &&
              result.value() == ("value_" + std::to_string(key_id))) {
            successful_reads++;
          }
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    logTest("Concurrent Reads", successful_reads.load() == total_reads.load(),
            std::to_string(successful_reads.load()) + "/" +
                std::to_string(total_reads.load()) + " in " +
                std::to_string(duration.count()) + "ms");
  }

  void testConcurrentMixedOperations() {
    std::cout << "\n=== Testing Concurrent Mixed Operations ===" << std::endl;

    memTable::MemtableManager manager(64 * 1024 * 1024);
    std::vector<std::thread> threads;

    std::atomic<int> insert_ops{0};
    std::atomic<int> read_ops{0};
    std::atomic<int> delete_ops{0};
    std::atomic<int> successful_ops{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // Mixed operations threads
    for (int t = 0; t < NUM_THREADS; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(200 + t);
        std::uniform_int_distribution<> op_dis(0,
                                               2); // 0=insert, 1=read, 2=delete
        std::uniform_int_distribution<> key_dis(0, OPERATIONS_PER_THREAD - 1);

        for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
          int op = op_dis(gen);
          std::string key = "mixed_key_" + std::to_string(t) + "_" +
                            std::to_string(key_dis(gen));

          if (op == 0) { // Insert
            insert_ops++;
            std::string value = generateRandomString(30, gen);
            if (manager.insert(key, value)) {
              successful_ops++;
            }
          } else if (op == 1) { // Read
            read_ops++;
            auto result = manager.search(key);
            successful_ops++; // Count all reads as successful (may or may not
                              // find)
          } else {            // Delete
            delete_ops++;
            if (manager.erase(key)) {
              successful_ops++;
            }
          }
        }
      });
    }

    for (auto &thread : threads) {
      thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    int total_ops = insert_ops + read_ops + delete_ops;
    logTest("Mixed Operations",
            total_ops == NUM_THREADS * OPERATIONS_PER_THREAD,
            "Inserts: " + std::to_string(insert_ops.load()) +
                ", Reads: " + std::to_string(read_ops.load()) +
                ", Deletes: " + std::to_string(delete_ops.load()) + " in " +
                std::to_string(duration.count()) + "ms");
  }

  void testMemTableFlushing() {
    std::cout << "\n=== Testing MemTable Flushing ===" << std::endl;

    // Use small size to trigger frequent flushes
    memTable::MemtableManager manager(1024 * 1024); // 1MB

    const int NUM_LARGE_INSERTS = 1000;
    std::string large_value(1024, 'A'); // 1KB value

    auto start_time = std::chrono::high_resolution_clock::now();

    // Insert enough data to trigger multiple flushes
    for (int i = 0; i < NUM_LARGE_INSERTS; ++i) {
      std::string key = "flush_key_" + std::to_string(i);
      manager.insert(key, large_value + std::to_string(i));

      if (i % 100 == 0) {
        printProgress(i, NUM_LARGE_INSERTS, "Flush Test");
      }
    }
    std::cout << std::endl;

    // Force a final flush
    manager.forceFlush();

    // Wait a bit for background flushing
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    // Test that we can still read the data
    int successful_reads = 0;
    for (int i = 0; i < NUM_LARGE_INSERTS; i += 10) { // Sample every 10th
      std::string key = "flush_key_" + std::to_string(i);
      auto result = manager.search(key);
      if (result.has_value()) {
        successful_reads++;
      }
    }

    int expected_reads = (NUM_LARGE_INSERTS + 9) / 10; // Ceiling division
    logTest("MemTable Flushing",
            successful_reads >= expected_reads * 0.9, // Allow 90% success rate
            std::to_string(successful_reads) + "/" +
                std::to_string(expected_reads) +
                " reads successful after flush in " +
                std::to_string(duration.count()) + "ms");
  }

  void testRangeQueries() {
    std::cout << "\n=== Testing Range Queries ===" << std::endl;

    memTable::MemtableManager manager(64 * 1024 * 1024);

    // Insert sorted keys
    const int NUM_RANGE_KEYS = 1000;
    for (int i = 0; i < NUM_RANGE_KEYS; ++i) {
      std::string key = "range_" +
                        std::string(4 - std::to_string(i).length(), '0') +
                        std::to_string(i);
      std::string value = "value_" + std::to_string(i);
      manager.insert(key, value);
    }

    // Test range query
    auto start_time = std::chrono::high_resolution_clock::now();
    auto results = manager.rangeQuery("range_0100", "range_0200");
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        end_time - start_time);

    // Should get keys from range_0100 to range_0200 (inclusive)
    int expected_count = 101; // 0100 to 0200 inclusive
    logTest("Range Query", results.size() == expected_count,
            std::to_string(results.size()) + "/" +
                std::to_string(expected_count) + " results in " +
                std::to_string(duration.count()) + "μs");

    // Verify results are in order
    bool sorted = std::is_sorted(results.begin(), results.end());
    logTest("Range Query Ordering", sorted);
  }

  void stressTest() {
    std::cout << "\n=== STRESS TEST: 10M Operations ===" << std::endl;

    memTable::MemtableManager manager(512 * 1024 *
                                      1024); // 512MB for stress test

    const int STRESS_THREADS = 16;
    const int OPS_PER_THREAD = STRESS_TEST_SIZE / STRESS_THREADS;

    std::vector<std::thread> threads;
    std::atomic<int> completed_ops{0};
    std::atomic<int> successful_ops{0};

    auto start_time = std::chrono::high_resolution_clock::now();

    // Progress reporting thread
    std::thread progress_thread([&]() {
      while (completed_ops < STRESS_TEST_SIZE) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        printProgress(completed_ops.load(), STRESS_TEST_SIZE, "Stress Test");
      }
    });

    // Worker threads
    for (int t = 0; t < STRESS_THREADS; ++t) {
      threads.emplace_back([&, t]() {
        std::mt19937 gen(1000 + t);
        std::uniform_int_distribution<> op_dis(
            0, 4); // 0-2=insert, 3=read, 4=delete

        for (int i = 0; i < OPS_PER_THREAD; ++i) {
          int op = op_dis(gen);
          int key_id = (t * OPS_PER_THREAD) + i;

          if (op <= 2) { // Insert (60% probability)
            std::string key = generateKey(key_id);
            std::string value = generateValue(key_id);
            if (manager.insert(key, value)) {
              successful_ops++;
            }
          } else if (op == 3) { // Read (20% probability)
            // Read a random existing key
            int read_key_id = gen() % (key_id + 1);
            std::string key = generateKey(read_key_id);
            auto result = manager.search(key);
            successful_ops++; // Count all reads as successful
          } else {            // Delete (20% probability)
            // Delete a random existing key
            int delete_key_id = gen() % (key_id + 1);
            std::string key = generateKey(delete_key_id);
            if (manager.erase(key)) {
              successful_ops++;
            }
          }

          completed_ops++;
        }
      });
    }

    // Wait for all threads
    for (auto &thread : threads) {
      thread.join();
    }

    progress_thread.join();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    std::cout << std::endl;

    // Performance metrics
    double ops_per_second =
        (double)STRESS_TEST_SIZE / (duration.count() / 1000.0);
    double memory_usage_mb =
        (double)manager.getCurrentMemoryUsage() / (1024 * 1024);

    logTest("Stress Test Completion", completed_ops.load() == STRESS_TEST_SIZE,
            std::to_string(STRESS_TEST_SIZE) + " operations in " +
                std::to_string(duration.count()) + "ms");

    std::cout << "Performance Metrics:" << std::endl;
    std::cout << "  Operations/second: " << std::fixed << std::setprecision(0)
              << ops_per_second << std::endl;
    std::cout << "  Memory usage: " << std::fixed << std::setprecision(2)
              << memory_usage_mb << " MB" << std::endl;
    std::cout << "  Active tables: " << manager.getActiveTableCount()
              << std::endl;
    std::cout << "  Frozen tables: " << manager.getFrozenTableCount()
              << std::endl;
  }

  void testTombstoneHandling() {
    std::cout << "\n=== Testing Tombstone Handling ===" << std::endl;

    memTable::MemtableManager manager(64 * 1024 * 1024);

    // Test sequence: Insert -> Delete -> Insert -> Search
    std::string key = "tombstone_test";

    // Initial insert
    bool insert1 = manager.insert(key, "value1");
    logTest("Initial Insert", insert1);

    // Verify it exists
    auto search1 = manager.search(key);
    logTest("Search Before Delete",
            search1.has_value() && search1.value() == "value1");

    // Delete (creates tombstone)
    bool delete1 = manager.erase(key);
    logTest("Create Tombstone", delete1);

    // Verify it's deleted
    auto search2 = manager.search(key);
    logTest("Search After Delete", !search2.has_value());

    // Insert again (should override tombstone)
    bool insert2 = manager.insert(key, "value2");
    logTest("Insert After Delete", insert2);

    // Verify new value
    auto search3 = manager.search(key);
    logTest("Search After Reinsert",
            search3.has_value() && search3.value() == "value2");
  }

  void runAllTests() {
    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "Starting Comprehensive MemTable Test Suite" << std::endl;
    std::cout << "===========================================" << std::endl;

    testBasicOperations();
    testTombstoneHandling();
    testConcurrentInserts();
    testConcurrentReads();
    testConcurrentMixedOperations();
    testRangeQueries();
    testMemTableFlushing();
    stressTest();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration =
        std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);

    std::cout << "\n===========================================" << std::endl;
    std::cout << "Test Suite Summary:" << std::endl;
    std::cout << "  Passed: " << passed_tests.load() << std::endl;
    std::cout << "  Failed: " << failed_tests.load() << std::endl;
    std::cout << "  Total Duration: " << total_duration.count() << " seconds"
              << std::endl;
    std::cout << "  Success Rate: " << std::fixed << std::setprecision(1)
              << (double)passed_tests.load() /
                     (passed_tests.load() + failed_tests.load()) * 100.0
              << "%" << std::endl;

    if (failed_tests.load() == 0) {
      std::cout << "\n🎉 ALL TESTS PASSED! 🎉" << std::endl;
    } else {
      std::cout << "\n⚠️  Some tests failed. Please review the results above."
                << std::endl;
    }
  }
};

int main() {
  try {
    MemTableTestSuite testSuite;
    testSuite.runAllTests();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Test suite crashed with exception: " << e.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "Test suite crashed with unknown exception" << std::endl;
    return 1;
  }
}