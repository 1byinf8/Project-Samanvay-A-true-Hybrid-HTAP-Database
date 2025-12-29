// test_htap_features.cpp - Comprehensive test for HTAP database features
// Compile: g++ -std=c++17 -O2 -o test_htap
// StorageEngine/test/test_htap_features.cpp Run: ./test_htap

#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

// Include the new HTAP components
#include "../includes/columnar_file.hpp"
#include "../includes/columnar_format.hpp"
#include "../includes/compaction.hpp"
#include "../includes/hybrid_query_router.hpp"
#include "../includes/lsm_levels.hpp"
#include "../includes/skiplist.hpp"
#include "../includes/sstable.hpp"
#include "../includes/storage_engine.hpp"
#include "../includes/wal.hpp"

class HTAPTestSuite {
private:
  int passed = 0;
  int failed = 0;

  void logTest(const std::string &name, bool success,
               const std::string &details = "") {
    if (success) {
      passed++;
      std::cout << "✓ " << name << " PASSED";
    } else {
      failed++;
      std::cout << "✗ " << name << " FAILED";
    }
    if (!details.empty()) {
      std::cout << " - " << details;
    }
    std::cout << std::endl;
  }

public:
  // ==================== LSM Level Manager Tests ====================
  void testLSMTreeManager() {
    std::cout << "\n=== Testing LSM Tree Manager ===" << std::endl;

    lsm::LSMConfig config;
    config.maxLevels = 7;
    config.level0CompactionThreshold = 4;
    config.columnarLevelThreshold = 4;
    config.dataDirectory = "./test_data";

    lsm::LSMTreeManager manager(config);

    // Test 1: Add SSTable
    lsm::SSTableMeta meta1("test1.sst", "aaa", "mmm", 100, 200, 1024, 100, 0,
                           12345);
    manager.addSSTable(meta1);

    auto level0Tables = manager.getSSTablesAtLevel(0);
    logTest("Add SSTable to Level 0", level0Tables.size() == 1);

    // Test 2: Key lookup
    auto tablesForKey = manager.getSSTablesForKey("bbb");
    logTest("Get SSTables for key",
            tablesForKey.size() == 1 &&
                tablesForKey[0].filePath == "test1.sst");

    // Test 3: Key outside range
    auto tablesForMiss = manager.getSSTablesForKey("zzz");
    logTest("Key outside range returns empty", tablesForMiss.empty());

    // Test 4: Range query
    lsm::SSTableMeta meta2("test2.sst", "nnn", "zzz", 201, 300, 2048, 200, 0,
                           12346);
    manager.addSSTable(meta2);

    auto tablesForRange = manager.getSSTablesForRange("aaa", "ooo");
    logTest("Range query finds overlapping tables", tablesForRange.size() == 2);

    // Test 5: Compaction trigger
    manager.addSSTable(
        lsm::SSTableMeta("test3.sst", "a", "b", 301, 400, 512, 50, 0, 12347));
    manager.addSSTable(
        lsm::SSTableMeta("test4.sst", "c", "d", 401, 500, 512, 50, 0, 12348));

    bool shouldCompact = manager.shouldTriggerCompaction(0);
    logTest("Compaction trigger at threshold", shouldCompact == true);

    // Test 6: Level stats
    auto stats = manager.getLevelStats(0);
    logTest("Level stats", stats.numSSTables == 4);

    // Test 7: Is columnar level
    logTest("Columnar level check (level 3)", !manager.isColumnarLevel(3));
    logTest("Columnar level check (level 4)", manager.isColumnarLevel(4));

    // Test 8: Remove SSTable
    bool removed = manager.removeSSTable("test1.sst");
    logTest("Remove SSTable", removed);
    logTest("Level 0 count after remove",
            manager.getSSTablesAtLevel(0).size() == 3);
  }

  // ==================== SSTable Range Query Tests ====================
  void testSSTableRangeQuery() {
    std::cout << "\n=== Testing SSTable Range Query ===" << std::endl;

    // Create test SSTable with known data
    std::vector<std::pair<std::string, skiplist::Entry>> entries;
    for (int i = 0; i < 100; i++) {
      std::string key = "key_" +
                        std::string(3 - std::to_string(i).length(), '0') +
                        std::to_string(i);
      std::string value = "value_" + std::to_string(i);
      entries.emplace_back(key, skiplist::Entry(key, value, i, false));
    }

    // Sort entries by key
    std::sort(entries.begin(), entries.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    std::string testPath = "test_range_query.sst";
    sstable::SSTable sst(testPath);

    bool created = sst.create(entries);
    logTest("Create SSTable for range test", created);

    // Load and query
    sstable::SSTable reader(testPath);
    bool loaded = reader.load();
    logTest("Load SSTable metadata", loaded);

    // Range query
    auto results = reader.rangeQuery("key_010", "key_020");
    logTest("Range query returns correct count",
            results.size() == 11); // 010 to 020 inclusive

    // Verify ordering
    bool ordered = true;
    for (size_t i = 1; i < results.size(); i++) {
      if (results[i].first < results[i - 1].first) {
        ordered = false;
        break;
      }
    }
    logTest("Range query results are ordered", ordered);

    // Point lookup
    auto found = reader.get("key_050");
    logTest("Point lookup finds key",
            found.has_value() && found->value == "value_50");

    auto notFound = reader.get("key_999");
    logTest("Point lookup miss", !notFound.has_value());

    // Clean up
    std::remove(testPath.c_str());
  }

  // ==================== Columnar Format Tests ====================
  void testColumnarFormat() {
    std::cout << "\n=== Testing Columnar Format ===" << std::endl;

    // Test ColumnBlock
    columnar::ColumnBlock intBlock(columnar::ColumnType::INT64);
    for (int i = 0; i < 100; i++) {
      intBlock.appendInt64(i * 10);
    }

    logTest("ColumnBlock row count", intBlock.getNumRows() == 100);
    logTest("ColumnBlock sum", intBlock.sumInt64() == 49500); // 0+10+20+...+990
    logTest("ColumnBlock min", intBlock.minInt64() == 0);
    logTest("ColumnBlock max", intBlock.maxInt64() == 990);
    logTest("ColumnBlock count", intBlock.count() == 100);

    // Test string column
    columnar::ColumnBlock strBlock(columnar::ColumnType::STRING);
    strBlock.appendString("apple");
    strBlock.appendString("banana");
    strBlock.appendString("cherry");

    auto strings = strBlock.getAsString();
    logTest("String column", strings.size() == 3 && strings[1] == "banana");

    // Test RLE encoding
    std::vector<int64_t> repetitive = {1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3};
    auto encoded = columnar::RLEEncoder::encodeInt64(repetitive);
    auto decoded = columnar::RLEEncoder::decodeInt64(encoded);

    logTest("RLE encoding/decoding", decoded == repetitive);
    logTest("RLE compression ratio",
            encoded.size() < repetitive.size() * sizeof(int64_t),
            "Encoded: " + std::to_string(encoded.size()) + " bytes");

    // Test dictionary encoding
    columnar::DictionaryEncoder dict;
    std::vector<std::string> categories = {"red",   "blue", "red",
                                           "green", "blue", "red"};
    auto indices = dict.encode(categories);
    auto restored = dict.decode(indices);

    logTest("Dictionary encoding", restored == categories);
    logTest("Dictionary size", dict.getDictionarySize() == 3);

    // Test TableSchema
    columnar::TableSchema schema("orders");
    schema.addColumn(
        columnar::ColumnSchema("order_id", columnar::ColumnType::INT64, false));
    schema.addColumn(
        columnar::ColumnSchema("customer", columnar::ColumnType::STRING, true));
    schema.addColumn(
        columnar::ColumnSchema("amount", columnar::ColumnType::DOUBLE, true));
    schema.primaryKeyColumn = "order_id";

    logTest("TableSchema column count", schema.columnCount() == 3);
    logTest("TableSchema column lookup", schema.getColumnIndex("amount") == 2);
    logTest("TableSchema missing column",
            schema.getColumnIndex("missing") == -1);
  }

  // ==================== Columnar File Tests ====================
  void testColumnarFile() {
    std::cout << "\n=== Testing Columnar File ===" << std::endl;

    // Create schema
    columnar::TableSchema schema("test_table");
    schema.addColumn(
        columnar::ColumnSchema("id", columnar::ColumnType::INT64, false));
    schema.addColumn(
        columnar::ColumnSchema("name", columnar::ColumnType::STRING, true));
    schema.addColumn(
        columnar::ColumnSchema("score", columnar::ColumnType::DOUBLE, true));

    std::string testPath = "test_columnar.col";

    // Write columnar file
    {
      columnar::ColumnarFile writer(testPath, schema,
                                    100); // 100 rows per group
      bool created = writer.create();
      logTest("Create columnar file", created);

      // Add rows
      for (int i = 0; i < 250; i++) {
        writer.addRow({std::to_string(i), "user_" + std::to_string(i),
                       std::to_string(i * 1.5)});
      }

      writer.finalize();
    }

    // Verify file exists
    std::ifstream checkFile(testPath);
    logTest("Columnar file written", checkFile.good());
    checkFile.close();

    // Read and verify
    auto reader = columnar::ColumnarFile::open(testPath);
    logTest("Open columnar file", reader != nullptr);

    if (reader) {
      logTest("Row group count", reader->getNumRowGroups() >= 2,
              "Expected at least 2 row groups for 250 rows");
    }

    // Clean up
    std::remove(testPath.c_str());
  }

  // ==================== Hybrid Query Router Tests ====================
  void testHybridQueryRouter() {
    std::cout << "\n=== Testing Hybrid Query Router ===" << std::endl;

    lsm::LSMConfig config;
    config.maxLevels = 7;
    config.columnarLevelThreshold = 4;

    auto lsmManager = std::make_shared<lsm::LSMTreeManager>(config);
    query::HybridQueryRouter router(lsmManager, 10000);

    // Test point lookup routing
    auto pointReq = query::QueryRequest::pointLookup("key123");
    auto pointPlan = router.planQuery(pointReq);

    logTest("Point lookup uses row store",
            pointPlan.useRowPath && !pointPlan.useColumnarPath);
    logTest("Point lookup scans memtable", pointPlan.scanMemtable);

    // Test aggregation routing
    auto aggReq =
        query::QueryRequest::aggregation(query::AggregationType::SUM, "amount");
    auto aggPlan = router.planQuery(aggReq);

    logTest("Aggregation prefers columnar", aggPlan.useColumnarPath);
    logTest("Aggregation includes columnar levels",
            !aggPlan.columnarLevelsToScan.empty());

    // Test full scan routing
    auto scanReq = query::QueryRequest::fullScan();
    auto scanPlan = router.planQuery(scanReq);

    logTest("Full scan uses both paths",
            scanPlan.useRowPath && scanPlan.useColumnarPath);

    // Test explain
    std::string explanation = router.explainPlan(pointPlan);
    logTest("Explain plan works", !explanation.empty());
    std::cout << "  Sample explain output:\n" << explanation << std::endl;
  }

  // ==================== Storage Engine Integration Tests ====================
  void testStorageEngine() {
    std::cout << "\n=== Testing Storage Engine ===" << std::endl;

    storage::StorageEngineConfig config;
    config.dataDirectory = "./test_storage_data";
    config.lsmConfig.maxLevels = 7;
    config.lsmConfig.columnarLevelThreshold = 4;

    storage::StorageEngine engine(config);

    // Test status
    auto stats = engine.getStats();
    logTest("Get stats", stats.levelStats.size() == 7);

    // Test query planning
    auto request = query::QueryRequest::rangeScan("a", "z");
    std::string plan = engine.explainQuery(request);
    logTest("Explain query", !plan.empty());

    // Test recovery (will create metadata file)
    // engine.recover() would fail since no file exists, which is expected

    std::cout << "\n  Engine Status:" << std::endl;
    engine.printStatus();
  }

  // ==================== WAL Tests ====================
  void testWAL() {
    std::cout << "\n=== Testing WAL ===" << std::endl;

    std::string walPath = "test_wal.log";

    // Clean up any existing file
    std::remove(walPath.c_str());

    {
      wal::WAL walLog(walPath);

      // Append some entries
      bool appended = true;
      for (int i = 0; i < 10; i++) {
        appended &=
            walLog.append(i, wal::Operation::INSERT, "key_" + std::to_string(i),
                          "value_" + std::to_string(i));
      }
      logTest("WAL append", appended);
    }

    // Test recovery
    {
      wal::WAL walLog(walPath);
      auto entries = walLog.recover();
      logTest("WAL recovery count", entries.size() == 10);

      if (!entries.empty()) {
        logTest("WAL recovery order", entries[0].sequenceNumber == 0);
        logTest("WAL recovery content", entries[5].key == "key_5");
      }

      // Test max sequence
      uint64_t maxSeq = walLog.getMaxSequence();
      logTest("WAL max sequence", maxSeq == 9);
    }

    // Test truncation
    {
      wal::WAL walLog(walPath);
      walLog.truncate(5); // Remove entries 0-5

      auto remaining = walLog.recover();
      logTest("WAL truncation", remaining.size() == 4); // 6, 7, 8, 9 remain

      if (!remaining.empty()) {
        logTest("WAL truncation first entry", remaining[0].sequenceNumber == 6);
      }
    }

    // Clean up
    std::remove(walPath.c_str());
  }

  // Run all tests
  void runAllTests() {
    auto start = std::chrono::high_resolution_clock::now();

    std::cout << "╔═══════════════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║    HTAP Database Features Test Suite                  ║"
              << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════════╝"
              << std::endl;

    testLSMTreeManager();
    testSSTableRangeQuery();
    testColumnarFormat();
    testColumnarFile();
    testHybridQueryRouter();
    testWAL();
    testStorageEngine();

    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "\n╔═══════════════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║    Test Results                                       ║"
              << std::endl;
    std::cout << "╠═══════════════════════════════════════════════════════╣"
              << std::endl;
    std::cout << "║  Passed: " << std::setw(3) << passed
              << "                                        ║" << std::endl;
    std::cout << "║  Failed: " << std::setw(3) << failed
              << "                                        ║" << std::endl;
    std::cout << "║  Total:  " << std::setw(3) << (passed + failed)
              << "                                        ║" << std::endl;
    std::cout << "║  Time:   " << std::setw(5) << duration.count()
              << "ms                                   ║" << std::endl;
    std::cout << "╚═══════════════════════════════════════════════════════╝"
              << std::endl;

    if (failed == 0) {
      std::cout << "\n🎉 ALL TESTS PASSED! 🎉\n" << std::endl;
    } else {
      std::cout << "\n⚠️  Some tests failed. Please review.\n" << std::endl;
    }
  }
};

int main() {
  try {
    HTAPTestSuite suite;
    suite.runAllTests();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Test suite crashed: " << e.what() << std::endl;
    return 1;
  }
}
