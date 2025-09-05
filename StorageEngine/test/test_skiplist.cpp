#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include <typeinfo>
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

// ---------- Helper to generate test values ----------
template <typename T>
struct TestValues;

// Specialization for int
template <>
struct TestValues<int> {
    static std::vector<int> values() { return {1, 5, 10, 15, 20, 25, 30, 42, 100}; }
    static std::vector<int> largeValues(size_t count) {
        std::vector<int> vals;
        for (size_t i = 0; i < count; ++i) {
            vals.push_back(static_cast<int>(i * 7 + 3)); // Generate unique values
        }
        return vals;
    }
};

// Specialization for double
template <>
struct TestValues<double> {
    static std::vector<double> values() { return {1.1, 5.5, 10.0, 15.2, 20.8, 25.0, 30.3, 42.42, 100.01}; }
    static std::vector<double> largeValues(size_t count) {
        std::vector<double> vals;
        for (size_t i = 0; i < count; ++i) {
            vals.push_back(static_cast<double>(i) * 3.14 + 1.0);
        }
        return vals;
    }
};

// Specialization for std::string
template <>
struct TestValues<std::string> {
    static std::vector<std::string> values() {
        return {"one", "five", "ten", "fifteen", "twenty", "twentyfive", "thirty", "forty-two", "hundred"};
    }
    static std::vector<std::string> largeValues(size_t count) {
        std::vector<std::string> vals;
        for (size_t i = 0; i < count; ++i) {
            vals.push_back("str_" + std::to_string(i * 11 + 7));
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

// ---------- Generic Tests (Templated) ----------
template <typename T>
void testInsertAndSearch() {
    auto vals = TestValues<T>::values();
    Skiplist<T> sl;
    sl.add(vals[2]); // 10
    sl.add(vals[4]); // 20
    sl.add(vals[6]); // 30

    assert(sl.search(vals[2]) == true);
    assert(sl.search(vals[4]) == true);
    assert(sl.search(vals[6]) == true);
    assert(sl.search(vals[7]) == false);
    std::cout << "testInsertAndSearch<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testDelete() {
    auto vals = TestValues<T>::values();
    Skiplist<T> sl;
    sl.add(vals[1]); // 5
    sl.add(vals[3]); // 15
    sl.add(vals[5]); // 25

    assert(sl.erase(vals[3]) == true);
    assert(sl.search(vals[3]) == false);

    assert(sl.erase(vals[8]) == false);  // non-existent
    std::cout << "testDelete<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testDuplicates() {
    auto vals = TestValues<T>::values();
    Skiplist<T> sl;
    sl.add(vals[7]); // 42
    sl.add(vals[7]); // duplicate ignored

    assert(sl.search(vals[7]) == true);
    assert(sl.erase(vals[7]) == true);
    assert(sl.search(vals[7]) == false);
    std::cout << "testDuplicates<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testSequentialInsert() {
    Skiplist<T> sl;
    auto vals = TestValues<T>::values();

    for (auto &v : vals) sl.add(v);

    for (auto &v : vals) assert(sl.search(v) == true);
    assert(sl.search(T{}) == false); // Something not in list
    std::cout << "testSequentialInsert<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testSequentialDelete() {
    Skiplist<T> sl;
    auto vals = TestValues<T>::values();

    for (auto &v : vals) sl.add(v);
    for (auto &v : vals) {
        assert(sl.erase(v) == true);
        assert(sl.search(v) == false);
    }
    std::cout << "testSequentialDelete<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testRandomInsertDelete() {
    auto vals = TestValues<T>::values();
    Skiplist<T> sl;

    std::vector<T> subset = {vals[1], vals[3], vals[5]};
    for (auto &v : subset) sl.add(v);

    assert(sl.search(vals[1]) == true);
    assert(sl.search(vals[3]) == true);
    assert(sl.search(vals[5]) == true);

    sl.erase(vals[3]);
    assert(sl.search(vals[3]) == false);

    sl.erase(vals[5]);
    assert(sl.search(vals[5]) == false);

    std::cout << "testRandomInsertDelete<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testEmptySkiplist() {
    auto vals = TestValues<T>::values();
    Skiplist<T> sl;
    assert(sl.search(vals[2]) == false);
    assert(sl.erase(vals[2]) == false);
    std::cout << "testEmptySkiplist<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testStress() {
    Skiplist<T> sl;
    auto vals = TestValues<T>::values();

    for (auto &v : vals) sl.add(v);
    for (auto &v : vals) assert(sl.search(v) == true);
    for (auto &v : vals) assert(sl.erase(v) == true);
    for (auto &v : vals) assert(sl.search(v) == false);

    std::cout << "testStress<" << typeid(T).name() << "> passed\n";
}

// ---------- Multithreaded Tests ----------

template <typename T>
void concurrentInsertWorker(Skiplist<T>& sl, const std::vector<T>& values, 
                           size_t start_idx, size_t end_idx, TestResults& results) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> delay_dis(0, 10);
    
    for (size_t i = start_idx; i < end_idx; ++i) {
        // Add some random delay to increase contention
        std::this_thread::sleep_for(std::chrono::microseconds(delay_dis(gen)));
        
        if (sl.add(values[i])) {
            results.successful_adds.fetch_add(1);
        } else {
            results.failed_adds.fetch_add(1);
        }
    }
}

template <typename T>
void concurrentSearchWorker(Skiplist<T>& sl, const std::vector<T>& values, 
                           TestResults& results, int iterations) {
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

template <typename T>
void concurrentDeleteWorker(Skiplist<T>& sl, const std::vector<T>& values, 
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

template <typename T>
void testConcurrentInserts() {
    std::cout << "testConcurrentInserts<" << typeid(T).name() << ">...\n";
    
    Skiplist<T> sl;
    TestResults results;
    const size_t num_values = 1000;
    const size_t num_threads = 8;
    
    auto values = TestValues<T>::largeValues(num_values);
    std::vector<std::thread> threads;
    
    size_t values_per_thread = num_values / num_threads;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < num_threads; ++i) {
        size_t start_idx = i * values_per_thread;
        size_t end_idx = (i == num_threads - 1) ? num_values : (i + 1) * values_per_thread;
        
        threads.emplace_back(concurrentInsertWorker<T>, std::ref(sl), 
                           std::cref(values), start_idx, end_idx, std::ref(results));
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    // Verify all unique values were inserted
    std::set<T> unique_values(values.begin(), values.end());
    for (const auto& val : unique_values) {
        if (!sl.search(val)) {
            results.data_race_detected = true;
            break;
        }
    }
    
    std::cout << "  Duration: " << duration.count() << "ms\n";
    results.print();
    assert(!results.data_race_detected.load());
    std::cout << "testConcurrentInserts<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testConcurrentMixed() {
    std::cout << "testConcurrentMixed<" << typeid(T).name() << ">...\n";
    
    Skiplist<T> sl;
    TestResults results;
    const size_t num_values = 500;
    auto values = TestValues<T>::largeValues(num_values);
    
    // Pre-populate with half the values
    for (size_t i = 0; i < num_values / 2; ++i) {
        sl.add(values[i]);
    }
    
    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Insert thread
    threads.emplace_back(concurrentInsertWorker<T>, std::ref(sl), std::cref(values), 
                        num_values / 2, num_values, std::ref(results));
    
    // Search threads
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back(concurrentSearchWorker<T>, std::ref(sl), std::cref(values), 
                           std::ref(results), 200);
    }
    
    // Delete thread (delete first quarter)
    threads.emplace_back(concurrentDeleteWorker<T>, std::ref(sl), std::cref(values), 
                        0, num_values / 4, std::ref(results));
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "  Duration: " << duration.count() << "ms\n";
    results.print();
    std::cout << "testConcurrentMixed<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testHighContentionScenario() {
    std::cout << "testHighContentionScenario<" << typeid(T).name() << ">...\n";
    
    Skiplist<T> sl;
    TestResults results;
    const size_t num_operations = 1000;
    const size_t num_threads = std::thread::hardware_concurrency();
    
    auto values = TestValues<T>::largeValues(100); // Small set for high contention
    std::vector<std::thread> threads;
    
    auto mixed_worker = [&](int thread_id) {
        std::random_device rd;
        std::mt19937 gen(rd() + thread_id);
        std::uniform_int_distribution<> op_dis(0, 2); // 0=insert, 1=search, 2=delete
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
    
    for (auto& t : threads) {
        t.join();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "  Threads: " << num_threads << ", Duration: " << duration.count() << "ms\n";
    results.print();
    std::cout << "testHighContentionScenario<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testMemoryConsistency() {
    std::cout << "testMemoryConsistency<" << typeid(T).name() << ">...\n";
    
    const int iterations = 100;
    const int values_per_iteration = 50;
    
    for (int iter = 0; iter < iterations; ++iter) {
        Skiplist<T> sl;
        auto values = TestValues<T>::largeValues(values_per_iteration);
        
        // Insert all values concurrently
        std::vector<std::future<void>> futures;
        for (size_t i = 0; i < values.size(); ++i) {
            futures.push_back(std::async(std::launch::async, [&sl, &values, i]() {
                sl.add(values[i]);
            }));
        }
        
        for (auto& f : futures) {
            f.wait();
        }
        
        // Verify all values are present
        std::atomic<bool> consistency_error{false};
        std::vector<std::future<void>> search_futures;
        
        for (const auto& val : values) {
            search_futures.push_back(std::async(std::launch::async, 
                [&sl, &val, &consistency_error]() {
                    if (!sl.search(val)) {
                        consistency_error = true;
                    }
                }));
        }
        
        for (auto& f : search_futures) {
            f.wait();
        }
        
        assert(!consistency_error.load());
    }
    
    std::cout << "  Completed " << iterations << " consistency iterations\n";
    std::cout << "testMemoryConsistency<" << typeid(T).name() << "> passed\n";
}

template <typename T>
void testRaceConditionDetection() {
    std::cout << "testRaceConditionDetection<" << typeid(T).name() << ">...\n";
    
    // This test attempts to detect race conditions by running operations
    // that should be atomic but might not be in a non-thread-safe implementation
    
    const int test_rounds = 50;
    std::atomic<bool> race_detected{false};
    
    for (int round = 0; round < test_rounds; ++round) {
        Skiplist<T> sl;
        auto values = TestValues<T>::largeValues(20);
        
        // All threads try to insert the same value simultaneously
        const T contested_value = values[0];
        std::atomic<int> successful_inserts{0};
        
        std::vector<std::thread> threads;
        for (int i = 0; i < 10; ++i) {
            threads.emplace_back([&sl, &contested_value, &successful_inserts]() {
                if (sl.add(contested_value)) {
                    successful_inserts.fetch_add(1);
                }
            });
        }
        
        for (auto& t : threads) {
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
    std::cout << "  Completed " << test_rounds << " race condition detection rounds\n";
    std::cout << "testRaceConditionDetection<" << typeid(T).name() << "> passed\n";
}

// ---------- Run All Tests for a Type ----------
template <typename T>
void runAllTests() {
    // Single-threaded tests
    testInsertAndSearch<T>();
    testDelete<T>();
    testDuplicates<T>();
    testSequentialInsert<T>();
    testSequentialDelete<T>();
    testRandomInsertDelete<T>();
    testEmptySkiplist<T>();
    testStress<T>();
    
    // Multithreaded tests
    testConcurrentInserts<T>();
    testConcurrentMixed<T>();
    testHighContentionScenario<T>();
    testMemoryConsistency<T>();
    testRaceConditionDetection<T>();
}

int main() {
    std::cout << "Starting Skiplist Regression Tests with Thread Safety\n";
    std::cout << "Hardware concurrency: " << std::thread::hardware_concurrency() << " threads\n\n";
    
    std::cout << "Running tests on Skiplist with int...\n";
    runAllTests<int>();

    std::cout << "\nRunning tests on Skiplist with double...\n";
    runAllTests<double>();

    std::cout << "\nRunning tests on Skiplist with std::string...\n";
    runAllTests<std::string>();

    std::cout << "\nAll tests passed successfully on all types!\n";
    std::cout << "Thread safety verification completed ✨\n";
    return 0;
}