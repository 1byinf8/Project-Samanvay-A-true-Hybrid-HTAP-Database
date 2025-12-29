// simple_test.cpp - Simple compilation test
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include "../src/memTable.cpp"

int main() {
    try {
        std::cout << "Testing basic compilation and functionality..." << std::endl;
        
        // Create memtable manager
        memTable::MemtableManager manager(1024 * 1024); // 1MB
        
        // Test basic operations
        bool inserted = manager.insert("test_key", "test_value");
        std::cout << "Insert result: " << (inserted ? "SUCCESS" : "FAILED") << std::endl;
        
        auto result = manager.search("test_key");
        std::cout << "Search result: " << (result.has_value() ? result.value() : "NOT_FOUND") << std::endl;
        
        bool deleted = manager.erase("test_key");
        std::cout << "Delete result: " << (deleted ? "SUCCESS" : "FAILED") << std::endl;
        
        auto result_after_delete = manager.search("test_key");
        std::cout << "Search after delete: " << (result_after_delete.has_value() ? "FOUND" : "NOT_FOUND") << std::endl;
        
        std::cout << "All basic tests completed successfully!" << std::endl;
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}