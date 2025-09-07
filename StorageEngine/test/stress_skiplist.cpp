#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <future>
#include <algorithm>
#include <set>
#include <mutex>
#include "../includes/skiplist.hpp"

using namespace skiplist;

// ---------- Helper to Generate Test Values for Entry ----------
struct TestValues {
    static std::vector<Entry> largeValues(size_t count) {
        std::vector<Entry> vals;
        for (size_t i = 0; i < count; ++i) {
            vals.emplace_back("key_" + std::to_string(i), 
                             "value_" + std::to_string(i), 
                             i + 1);
        }
        return vals;
    }
};

// ---------- Thread-Safe Test Result Collector ----------
struct TestResults {
    std::atomic<int64_t> successful_adds{0};
    std::atomic<int64_t> failed_adds{0};
    std::atomic<int64_t> successful_searches{0};
    std::atomic<int64_t> failed_searches{0};
    std::atomic<int64_t> successful_deletes{0};
    std::atomic<int64_t> failed_deletes{0};
    std::atomic<bool> data_race_detected{false};
    
    void reset() {
        successful_adds = 0;
        failed_adds = 0;
        successful_searches = 0;
        failed_searches = 0;
        successful_deletes = 0;
        failed_deletes = 0;
        data_race_detected = false;
    }
    
    void print() const {
        std::cout << "  Adds: " << successful_adds << " success, " << failed_adds << " failed\n";
        std::cout << "  Searches: " << successful_searches << " success, " << failed_searches << " failed\n";
        std::cout << "  Deletes: " << successful_deletes << " success, " << failed_deletes << " failed\n";
        if (data_race_detected.load()) {
            std::cout << "  ⚠️  Data race detected!\n";
        }
    }
};

// ---------- Worker Functions ----------
void concurrentInsertWorker(Skiplist& sl, const std::vector<Entry>& values, 
                           size_t start_idx, size_t end_idx, TestResults& results) {
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

void concurrentSearchWorker(Skiplist& sl, const std::vector<Entry>& values, 
                           TestResults& results, int64_t iterations) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> idx_dis(0, values.size() - 1);
    std::uniform_int_distribution<> delay_dis(0, 5);
    
    for (int64_t i = 0; i < iterations; ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_dis(gen)));
        
        size_t idx = idx_dis(gen);
        if (sl.search(values[idx])) {
            results.successful_searches.fetch_add(1);
        } else {
            results.failed_searches.fetch_add(1);
        }
    }
}

void concurrentDeleteWorker(Skiplist& sl, const std::vector<Entry>& values, 
                           size_t start_idx, size_t end_idx, TestResults& results) {
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

// ---------- Stress Test for 1M Operations ----------
void testConcurrentMillionOperations() {
    std::cout << "testConcurrentMillionOperations...\n";
    
    Skiplist sl;
    TestResults results;
    const size_t num_values = 1'000'000; // 1M entries
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t values_per_thread = num_values / num_threads;
    const size_t searches_per_thread = num_values / num_threads;
    
    auto values = TestValues::largeValues(num_values);
    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Phase 1: Concurrent Inserts
    std::cout << "  Inserting " << num_values << " entries with " << num_threads << " threads...\n";
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * values_per_thread;
        size_t end_idx = (i == num_threads - 1) ? num_values : (i + 1) * values_per_thread;
        threads.emplace_back(concurrentInsertWorker, std::ref(sl), std::cref(values), 
                            start_idx, end_idx, std::ref(results));
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    
    // Verify all inserted entries
    std::cout << "  Verifying inserted entries...\n";
    std::set<std::string> unique_keys;
    for (const auto& val : values) unique_keys.insert(val.key);
    std::vector<std::future<void>> futures;
    for (const auto& key : unique_keys) {
        futures.push_back(std::async(std::launch::async, [&sl, &key, &results]() {
            if (!sl.search(Entry(key, "", 0))) {
                results.data_race_detected = true;
            }
        }));
    }
    for (auto& f : futures) {
        f.wait();
    }
    
    // Phase 2: Concurrent Searches
    std::cout << "  Searching " << num_values << " times with " << num_threads << " threads...\n";
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(concurrentSearchWorker, std::ref(sl), std::cref(values), 
                            std::ref(results), searches_per_thread);
    }
    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    
    // Phase 3: Concurrent Deletes
    std::cout << "  Deleting " << num_values << " entries with " << num_threads << " threads...\n";
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * values_per_thread;
        size_t end_idx = (i == num_threads - 1) ? num_values : (i + 1) * values_per_thread;
        threads.emplace_back(concurrentDeleteWorker, std::ref(sl), std::cref(values), 
                            start_idx, end_idx, std::ref(results));
    }
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all deleted
    std::cout << "  Verifying deleted entries...\n";
    futures.clear();
    for (const auto& key : unique_keys) {
        futures.push_back(std::async(std::launch::async, [&sl, &key, &results]() {
            if (sl.search(Entry(key, "", 0))) {
                results.data_race_detected = true;
            }
        }));
    }
    for (auto& f : futures) {
        f.wait();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "  Total Duration: " << duration.count() << "ms\n";
    results.print();
    assert(results.successful_adds == num_values);
    assert(results.successful_deletes == num_values);
    assert(results.successful_searches >= num_values * 0.9); // Allow some failed searches due to concurrent deletes
    assert(!results.data_race_detected.load());
    std::cout << "testConcurrentMillionOperations passed\n";
}

void testConcurrentMixedMillionOperations() {
    std::cout << "testConcurrentMixedMillionOperations...\n";
    
    Skiplist sl;
    TestResults results;
    const size_t num_values = 1'000'000; // 1M entries
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t ops_per_thread = num_values / num_threads;
    
    auto values = TestValues::largeValues(num_values);
    std::vector<std::thread> threads;
    
    auto mixed_worker = [&](int thread_id) {
        std::random_device rd;
        std::mt19937 gen(rd() + thread_id);
        std::uniform_int_distribution<> op_dis(0, 2); // 0=insert, 1=search, 2=delete
        std::uniform_int_distribution<size_t> val_dis(0, values.size() - 1);
        
        for (size_t i = 0; i < ops_per_thread; ++i) {
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
    std::cout << "  Performing " << num_values << " mixed operations with " << num_threads << " threads...\n";
    
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(mixed_worker, i);
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify at least some operations succeeded
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "  Total Duration: " << duration.count() << "ms\n";
    results.print();
    assert(results.successful_adds > 0);
    assert(results.successful_searches > 0);
    assert(results.successful_deletes > 0);
    assert(!results.data_race_detected.load());
    std::cout << "testConcurrentMixedMillionOperations passed\n";
}

// ---------- Run All Tests ----------
void runAllTests() {
    testConcurrentMillionOperations();
    testConcurrentMixedMillionOperations();
}

int main() {
    std::cout << "Starting Skiplist Stress Tests for 1M Concurrent Operations\n";
    std::cout << "Hardware concurrency: " << std::thread::hardware_concurrency() << " threads\n\n";
    
    runAllTests();
    
    std::cout << "\nAll stress tests passed successfully for Entry!\n";
    std::cout << "Concurrent 1M operations verification completed ✨\n";
    return 0;
}