#include "../includes/skiplist.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

// Define thread_local static for skiplist random generator
thread_local std::mt19937 skiplist::Skiplist::gen(std::random_device{}());

using namespace skiplist;

// ---------- Helper to Generate Test Values for Entry ----------
struct TestValues {
  static std::vector<Entry> values() {
    return {Entry("key1", "value1", 1),    Entry("key5", "value5", 2),
            Entry("key10", "value10", 3),  Entry("key15", "value15", 4),
            Entry("key20", "value20", 5),  Entry("key25", "value25", 6),
            Entry("key30", "value30", 7),  Entry("key42", "value42", 8),
            Entry("key100", "value100", 9)};
  }

  static std::vector<Entry> largeValues(size_t count) {
    std::vector<Entry> vals;
    for (size_t i = 0; i < count; ++i) {
      vals.emplace_back("key_" + std::to_string(i * 7 + 3),
                        "value_" + std::to_string(i * 7 + 3), i + 1);
    }
    return vals;
  }

  static std::vector<Entry> tombstoneValues(size_t count) {
    std::vector<Entry> vals;
    for (size_t i = 0; i < count; ++i) {
      vals.emplace_back("key_" + std::to_string(i * 7 + 3), "", i + 1,
                        true); // Tombstones
    }
    return vals;
  }
};

// ---------- Thread-Safe Test Result Collector ----------
struct TestResults {
  std::atomic<int> successful_adds{0};
  std::atomic<int> failed_adds{0};
  std::atomic<int> successful_searches{0};
  std::atomic<int> failed_searches{0};
  std::atomic<int> successful_deletes{0};
  std::atomic<int> failed_deletes{0};
  std::atomic<bool> data_race_detected{false};
  std::atomic<int> incorrect_seq{0};

  void reset() {
    successful_adds = 0;
    failed_adds = 0;
    successful_searches = 0;
    failed_searches = 0;
    successful_deletes = 0;
    failed_deletes = 0;
    data_race_detected = false;
    incorrect_seq = 0;
  }

  void print() const {
    std::cout << "  Adds: " << successful_adds << " success, " << failed_adds
              << " failed\n";
    std::cout << "  Searches: " << successful_searches << " success, "
              << failed_searches << " failed\n";
    std::cout << "  Deletes: " << successful_deletes << " success, "
              << failed_deletes << " failed\n";
    std::cout << "  Incorrect Sequence Numbers: " << incorrect_seq << "\n";
    if (data_race_detected.load()) {
      std::cout << "  ⚠️  Data race detected!\n";
    }
  }
};

// ---------- Single-Threaded Tests ----------
void testInsertAndSearch() {
  auto vals = TestValues::values();
  Skiplist sl;
  sl.add(vals[2]); // key10
  sl.add(vals[4]); // key20
  sl.add(vals[6]); // key30

  assert(sl.search(vals[2]) == true);
  assert(sl.search(vals[4]) == true);
  assert(sl.search(vals[6]) == true);
  assert(sl.search(vals[7]) == false);
  std::cout << "testInsertAndSearch passed\n";
}

void testDelete() {
  auto vals = TestValues::values();
  Skiplist sl;
  sl.add(vals[1]); // key5
  sl.add(vals[3]); // key15
  sl.add(vals[5]); // key25

  assert(sl.erase(vals[3]) == true);
  assert(sl.search(vals[3]) == false);
  assert(sl.erase(vals[8]) == false); // non-existent
  std::cout << "testDelete passed\n";
}

void testDuplicates() {
  auto vals = TestValues::values();
  Skiplist sl;
  sl.add(vals[7]); // key42
  sl.add(vals[7]); // duplicate ignored

  assert(sl.search(vals[7]) == true);
  assert(sl.erase(vals[7]) == true);
  assert(sl.search(vals[7]) == false);
  std::cout << "testDuplicates passed\n";
}

void testSequentialInsert() {
  Skiplist sl;
  auto vals = TestValues::values();

  for (const auto &v : vals)
    sl.add(v);

  for (const auto &v : vals)
    assert(sl.search(v) == true);
  assert(sl.search(Entry("notfound", "", 0)) == false);
  std::cout << "testSequentialInsert passed\n";
}

void testSequentialDelete() {
  Skiplist sl;
  auto vals = TestValues::values();

  for (const auto &v : vals)
    sl.add(v);
  for (const auto &v : vals) {
    assert(sl.erase(v) == true);
    assert(sl.search(v) == false);
  }
  std::cout << "testSequentialDelete passed\n";
}

void testRandomInsertDelete() {
  auto vals = TestValues::values();
  Skiplist sl;

  std::vector<Entry> subset = {vals[1], vals[3], vals[5]};
  for (const auto &v : subset)
    sl.add(v);

  assert(sl.search(vals[1]) == true);
  assert(sl.search(vals[3]) == true);
  assert(sl.search(vals[5]) == true);

  sl.erase(vals[3]);
  assert(sl.search(vals[3]) == false);

  sl.erase(vals[5]);
  assert(sl.search(vals[5]) == false);

  std::cout << "testRandomInsertDelete passed\n";
}

void testEmptySkiplist() {
  auto vals = TestValues::values();
  Skiplist sl;
  assert(sl.search(vals[2]) == false);
  assert(sl.erase(vals[2]) == false);
  std::cout << "testEmptySkiplist passed\n";
}

void testStress() {
  Skiplist sl;
  auto vals = TestValues::values();

  for (const auto &v : vals)
    sl.add(v);
  for (const auto &v : vals)
    assert(sl.search(v) == true);
  for (const auto &v : vals)
    assert(sl.erase(v) == true);
  for (const auto &v : vals)
    assert(sl.search(v) == false);

  std::cout << "testStress passed\n";
}

void testTombstoneHandling() {
  Skiplist sl;
  auto vals = TestValues::tombstoneValues(5);
  auto normal_vals = TestValues::values();

  for (const auto &v : vals)
    sl.add(v);

  for (const auto &v : vals) {
    assert(sl.search(v) == true); // Tombstones are still "present"
  }

  // Add normal entries with same keys
  for (size_t i = 0; i < vals.size(); ++i) {
    Entry normal_entry(vals[i].key, "new_value_" + std::to_string(i), i + 10);
    assert(sl.add(normal_entry) == false); // Should fail due to existing key
  }

  std::cout << "testTombstoneHandling passed\n";
}

void testSequenceNumber() {
  Skiplist sl;
  auto vals = TestValues::values();
  Entry newer_entry(vals[2].key, "new_value", 100); // Same key, higher seq

  sl.add(vals[2]); // key10, seq=3
  assert(sl.search(vals[2]) == true);
  assert(sl.add(newer_entry) == false);   // Should fail due to existing key
  assert(sl.search(newer_entry) == true); // Should find the original entry

  std::cout << "testSequenceNumber passed\n";
}

// ---------- Multithreaded Tests ----------

void concurrentInsertWorker(Skiplist &sl, const std::vector<Entry> &values,
                            size_t start_idx, size_t end_idx,
                            TestResults &results) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> delay_dis(0, 10);

  for (size_t i = start_idx; i < end_idx; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(delay_dis(gen)));

    if (sl.add(values[i])) {
      results.successful_adds.fetch_add(1);
    } else {
      results.failed_adds.fetch_add(1);
    }
  }
}

void concurrentSearchWorker(Skiplist &sl, const std::vector<Entry> &values,
                            TestResults &results, int iterations) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> idx_dis(0, values.size() - 1);
  std::uniform_int_distribution<> delay_dis(0, 5);

  for (int i = 0; i < iterations; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(delay_dis(gen)));

    size_t idx = idx_dis(gen);
    if (sl.search(values[idx])) {
      results.successful_searches.fetch_add(1);
    } else {
      results.failed_searches.fetch_add(1);
    }
  }
}

void concurrentDeleteWorker(Skiplist &sl, const std::vector<Entry> &values,
                            size_t start_idx, size_t end_idx,
                            TestResults &results) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> delay_dis(0, 15);

  for (size_t i = start_idx; i < end_idx; ++i) {
    std::this_thread::sleep_for(std::chrono::microseconds(delay_dis(gen)));

    if (sl.erase(values[i])) {
      results.successful_deletes.fetch_add(1);
    } else {
      results.failed_deletes.fetch_add(1);
    }
  }
}

void testConcurrentInserts() {
  std::cout << "testConcurrentInserts...\n";

  Skiplist sl;
  TestResults results;
  const size_t num_values = 1000;
  const size_t num_threads = 8;

  auto values = TestValues::largeValues(num_values);
  std::vector<std::thread> threads;

  size_t values_per_thread = num_values / num_threads;
  auto start_time = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < num_threads; ++i) {
    size_t start_idx = i * values_per_thread;
    size_t end_idx =
        (i == num_threads - 1) ? num_values : (i + 1) * values_per_thread;

    threads.emplace_back(concurrentInsertWorker, std::ref(sl),
                         std::cref(values), start_idx, end_idx,
                         std::ref(results));
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  // Verify all unique values were inserted
  std::set<std::string> unique_keys;
  for (const auto &val : values)
    unique_keys.insert(val.key);
  for (const auto &key : unique_keys) {
    if (!sl.search(Entry(key, "", 0))) {
      results.data_race_detected = true;
      break;
    }
  }

  std::cout << "  Duration: " << duration.count() << "ms\n";
  results.print();
  assert(!results.data_race_detected.load());
  std::cout << "testConcurrentInserts passed\n";
}

void testConcurrentMixed() {
  std::cout << "testConcurrentMixed...\n";

  Skiplist sl;
  TestResults results;
  const size_t num_values = 500;
  auto values = TestValues::largeValues(num_values);

  // Pre-populate with half the values
  for (size_t i = 0; i < num_values / 2; ++i) {
    sl.add(values[i]);
  }

  std::vector<std::thread> threads;
  auto start_time = std::chrono::high_resolution_clock::now();

  // Insert thread
  threads.emplace_back(concurrentInsertWorker, std::ref(sl), std::cref(values),
                       num_values / 2, num_values, std::ref(results));

  // Search threads
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(concurrentSearchWorker, std::ref(sl),
                         std::cref(values), std::ref(results), 200);
  }

  // Delete thread (delete first quarter)
  threads.emplace_back(concurrentDeleteWorker, std::ref(sl), std::cref(values),
                       0, num_values / 4, std::ref(results));

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "  Duration: " << duration.count() << "ms\n";
  results.print();
  std::cout << "testConcurrentMixed passed\n";
}

void testHighContentionScenario() {
  std::cout << "testHighContentionScenario...\n";

  Skiplist sl;
  TestResults results;
  const size_t num_operations = 1000;
  const size_t num_threads = std::thread::hardware_concurrency();

  auto values = TestValues::largeValues(10); // Small set for high contention
  std::vector<std::thread> threads;

  auto mixed_worker = [&](int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);
    std::uniform_int_distribution<> op_dis(0,
                                           2); // 0=insert, 1=search, 2=delete
    std::uniform_int_distribution<size_t> val_dis(0, values.size() - 1);

    for (size_t i = 0; i < num_operations / num_threads; ++i) {
      size_t val_idx = val_dis(gen);
      int operation = op_dis(gen);

      switch (operation) {
      case 0: // Insert
        if (sl.add(values[val_idx])) {
          results.successful_adds.fetch_add(1);
        } else {
          results.failed_adds.fetch_add(1);
        }
        break;
      case 1: // Search
        if (sl.search(values[val_idx])) {
          results.successful_searches.fetch_add(1);
        } else {
          results.failed_searches.fetch_add(1);
        }
        break;
      case 2: // Delete
        if (sl.erase(values[val_idx])) {
          results.successful_deletes.fetch_add(1);
        } else {
          results.failed_deletes.fetch_add(1);
        }
        break;
      }
    }
  };

  auto start_time = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(mixed_worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "  Threads: " << num_threads
            << ", Duration: " << duration.count() << "ms\n";
  results.print();
  std::cout << "testHighContentionScenario passed\n";
}

void testMemoryConsistency() {
  std::cout << "testMemoryConsistency...\n";

  const int iterations = 100;
  const int values_per_iteration = 50;

  for (int iter = 0; iter < iterations; ++iter) {
    Skiplist sl;
    auto values = TestValues::largeValues(values_per_iteration);

    // Insert all values concurrently
    std::vector<std::future<void>> futures;
    for (size_t i = 0; i < values.size(); ++i) {
      futures.push_back(std::async(std::launch::async,
                                   [&sl, &values, i]() { sl.add(values[i]); }));
    }

    for (auto &f : futures) {
      f.wait();
    }

    // Verify all values are present
    std::atomic<bool> consistency_error{false};
    std::vector<std::future<void>> search_futures;

    for (const auto &val : values) {
      search_futures.push_back(
          std::async(std::launch::async, [&sl, &val, &consistency_error]() {
            if (!sl.search(val)) {
              consistency_error = true;
            }
          }));
    }

    for (auto &f : search_futures) {
      f.wait();
    }

    assert(!consistency_error.load());
  }

  std::cout << "  Completed " << iterations << " consistency iterations\n";
  std::cout << "testMemoryConsistency passed\n";
}

void testRaceConditionDetection() {
  std::cout << "testRaceConditionDetection...\n";

  const int test_rounds = 50;
  std::atomic<bool> race_detected{false};

  for (int round = 0; round < test_rounds; ++round) {
    Skiplist sl;
    auto values = TestValues::largeValues(20);

    // All threads try to insert the same value simultaneously
    const Entry contested_value = values[0];
    std::atomic<int> successful_inserts{0};

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
      threads.emplace_back([&sl, &contested_value, &successful_inserts]() {
        if (sl.add(contested_value)) {
          successful_inserts.fetch_add(1);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    }

    // Only one thread should have successfully inserted
    if (successful_inserts.load() != 1) {
      race_detected = true;
      break;
    }

    // Verify the value is actually in the skiplist
    if (!sl.search(contested_value)) {
      race_detected = true;
      break;
    }
  }

  assert(!race_detected.load());
  std::cout << "  Completed " << test_rounds
            << " race condition detection rounds\n";
  std::cout << "testRaceConditionDetection passed\n";
}

void testLargeScaleOperations() {
  std::cout << "testLargeScaleOperations...\n";

  Skiplist sl;
  TestResults results;
  const size_t num_values = 10000; // Large dataset for memtable simulation

  auto values = TestValues::largeValues(num_values);
  auto start_time = std::chrono::high_resolution_clock::now();

  // Sequential insert
  for (const auto &v : values) {
    if (sl.add(v)) {
      results.successful_adds.fetch_add(1);
    } else {
      results.failed_adds.fetch_add(1);
    }
  }

  // Verify all inserted
  for (const auto &v : values) {
    if (sl.search(v)) {
      results.successful_searches.fetch_add(1);
    } else {
      results.failed_searches.fetch_add(1);
    }
  }

  // Sequential delete
  for (const auto &v : values) {
    if (sl.erase(v)) {
      results.successful_deletes.fetch_add(1);
    } else {
      results.failed_deletes.fetch_add(1);
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "  Duration: " << duration.count() << "ms\n";
  results.print();
  assert(results.successful_adds == num_values);
  assert(results.successful_searches == num_values);
  assert(results.successful_deletes == num_values);
  assert(!results.data_race_detected.load());
  std::cout << "testLargeScaleOperations passed\n";
}

void testHTAPMixedWorkload() {
  std::cout << "testHTAPMixedWorkload...\n";

  Skiplist sl;
  TestResults results;
  const size_t num_values = 1000;
  const size_t num_threads = std::thread::hardware_concurrency();
  const size_t ops_per_thread = 500;

  auto values = TestValues::largeValues(num_values);
  auto tombstones = TestValues::tombstoneValues(num_values / 4);

  // Pre-populate with some values
  for (size_t i = 0; i < num_values / 2; ++i) {
    sl.add(values[i]);
  }

  auto mixed_worker = [&](int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);
    std::uniform_int_distribution<> op_dis(
        0, 3); // 0=insert, 1=search, 2=delete, 3=tombstone
    std::uniform_int_distribution<size_t> val_dis(0, values.size() - 1);
    std::uniform_int_distribution<size_t> tomb_dis(0, tombstones.size() - 1);

    for (size_t i = 0; i < ops_per_thread; ++i) {
      int operation = op_dis(gen);
      switch (operation) {
      case 0: // Insert
        if (sl.add(values[val_dis(gen)])) {
          results.successful_adds.fetch_add(1);
        } else {
          results.failed_adds.fetch_add(1);
        }
        break;
      case 1: // Search
        if (sl.search(values[val_dis(gen)])) {
          results.successful_searches.fetch_add(1);
        } else {
          results.failed_searches.fetch_add(1);
        }
        break;
      case 2: // Delete
        if (sl.erase(values[val_dis(gen)])) {
          results.successful_deletes.fetch_add(1);
        } else {
          results.failed_deletes.fetch_add(1);
        }
        break;
      case 3: // Insert tombstone
        if (sl.add(tombstones[tomb_dis(gen)])) {
          results.successful_adds.fetch_add(1);
        } else {
          results.failed_adds.fetch_add(1);
        }
        break;
      }
    }
  };

  std::vector<std::thread> threads;
  auto start_time = std::chrono::high_resolution_clock::now();

  for (size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(mixed_worker, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "  Threads: " << num_threads
            << ", Duration: " << duration.count() << "ms\n";
  results.print();
  std::cout << "testHTAPMixedWorkload passed\n";
}

// ---------- Run All Tests ----------
void runAllTests() {
  // Single-threaded tests
  testInsertAndSearch();
  testDelete();
  testDuplicates();
  testSequentialInsert();
  testSequentialDelete();
  testRandomInsertDelete();
  testEmptySkiplist();
  testStress();
  testTombstoneHandling();
  testSequenceNumber();

  // Multithreaded tests
  testConcurrentInserts();
  testConcurrentMixed();
  testHighContentionScenario();
  testMemoryConsistency();
  testRaceConditionDetection();
  testLargeScaleOperations();
  testHTAPMixedWorkload();
}

int main() {
  std::cout
      << "Starting Skiplist Regression Tests with Thread Safety for Entry\n";
  std::cout << "Hardware concurrency: " << std::thread::hardware_concurrency()
            << " threads\n\n";

  runAllTests();

  std::cout << "\nAll tests passed successfully for Entry!\n";
  std::cout << "Thread safety verification completed ✨\n";
  return 0;
}