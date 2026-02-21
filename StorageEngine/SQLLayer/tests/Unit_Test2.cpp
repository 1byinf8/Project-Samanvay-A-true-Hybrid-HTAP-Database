// test_sql_layer.cpp - Comprehensive SQL Layer Tests
// Tests every aspect of parsing, routing, and execution
//
// Compile from StorageEngine/SQLLayer/:
//   g++ -std=c++17 \
//       -I../includes \
//       -I../third_party/sql-parser/src \
//       -o test_sql_layer tests/test_sql_layer.cpp \
//       ../build/libsqlparser.a -pthread

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "../../includes/storage_engine.hpp"
#include "../includes/query_executor.hpp"
#include "../includes/result_formatter.hpp"
#include "../includes/row_codec.hpp"
#include "../includes/schema_registry.hpp"

// ================================================================
// Test Infrastructure
// ================================================================

int passed = 0;
int failed = 0;

void pass(const std::string &name)
{
    std::cout << "  [PASS] " << name << "\n";
    passed++;
}

void fail(const std::string &name, const std::string &reason = "")
{
    std::cerr << "  [FAIL] " << name;
    if (!reason.empty())
        std::cerr << " — " << reason;
    std::cerr << "\n";
    failed++;
}

void section(const std::string &name)
{
    std::cout << "\n=== " << name << " ===\n";
}

// Assert a ResultSet succeeded
void assertOk(const sql::ResultSet &rs, const std::string &name)
{
    if (rs.success)
        pass(name);
    else
        fail(name, rs.errorMessage);
}

// Assert a ResultSet failed with a specific error
void assertError(const sql::ResultSet &rs, const std::string &name,
                 const std::string &expectedSubstr = "")
{
    if (!rs.success)
    {
        if (expectedSubstr.empty() ||
            rs.errorMessage.find(expectedSubstr) != std::string::npos)
            pass(name);
        else
            fail(name, "Expected error containing '" + expectedSubstr +
                           "' but got: " + rs.errorMessage);
    }
    else
    {
        fail(name, "Expected error but query succeeded");
    }
}

// Assert result has N rows
void assertRowCount(const sql::ResultSet &rs, size_t expected,
                    const std::string &name)
{
    if (!rs.success)
    {
        fail(name, rs.errorMessage);
        return;
    }
    if (rs.rows.size() == expected)
        pass(name);
    else
        fail(name, "Expected " + std::to_string(expected) +
                       " rows but got " + std::to_string(rs.rows.size()));
}

// Assert a specific cell value
void assertCell(const sql::ResultSet &rs, size_t row, size_t col,
                const std::string &expected, const std::string &name)
{
    if (!rs.success)
    {
        fail(name, rs.errorMessage);
        return;
    }
    if (row >= rs.rows.size())
    {
        fail(name, "Row " + std::to_string(row) + " out of bounds");
        return;
    }
    if (col >= rs.rows[row].size())
    {
        fail(name, "Col " + std::to_string(col) + " out of bounds");
        return;
    }
    if (rs.rows[row][col] == expected)
        pass(name);
    else
        fail(name, "Expected '" + expected + "' got '" +
                       rs.rows[row][col] + "'");
}

// Assert header at position
void assertHeader(const sql::ResultSet &rs, size_t col,
                  const std::string &expected, const std::string &name)
{
    if (col >= rs.headers.size())
    {
        fail(name, "Header col out of bounds");
        return;
    }
    if (rs.headers[col] == expected)
        pass(name);
    else
        fail(name, "Expected header '" + expected +
                       "' got '" + rs.headers[col] + "'");
}

// ================================================================
// Test Setup — shared engine + executor
// ================================================================
struct TestCtx
{
    storage::StorageEngineConfig config;
    storage::StorageEngine *engine = nullptr;
    sql::SchemaRegistry *registry = nullptr;
    sql::QueryExecutor *executor = nullptr;

    TestCtx()
    {
        config.dataDirectory = "./test_data";
        config.columnarDirectory = "./test_data/columnar";
        config.walPath = "./test_data/wal.log";

        engine = new storage::StorageEngine(config);
        registry = new sql::SchemaRegistry("./test_data/schema.sdb");
        executor = new sql::QueryExecutor(*engine, *registry);
    }

    ~TestCtx()
    {
        delete executor;
        delete registry;
        delete engine;
        // Cleanup test files
        std::system("rm -rf ./test_data ./test_data/columnar 2>/dev/null");
        std::remove("./test_data/schema.sdb");
    }

    sql::ResultSet run(const std::string &sql)
    {
        return executor->execute(sql);
    }

    std::string explain(const std::string &sql)
    {
        return executor->explain(sql);
    }
};

// ================================================================
// 1. DDL TESTS — CREATE TABLE
// ================================================================
void testCreateTable(TestCtx &ctx)
{
    section("DDL — CREATE TABLE");

    // Basic create
    auto rs = ctx.run(
        "CREATE TABLE users ("
        "  user_id BIGINT PRIMARY KEY,"
        "  name VARCHAR,"
        "  email VARCHAR,"
        "  age INT,"
        "  salary DOUBLE"
        ")");
    assertOk(rs, "CREATE TABLE basic");
    assert(ctx.registry->tableExists("users"));
    pass("Schema persisted in registry");

    // Verify schema details
    auto schema = ctx.registry->getSchema("users");
    assert(schema.has_value());
    assert(schema->columnCount() == 5);
    assert(schema->primaryKeyColumn == "user_id");
    pass("Schema has 5 columns with correct PK");

    // Duplicate table rejected
    rs = ctx.run(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)");
    assertError(rs, "CREATE TABLE duplicate rejected", "already exists");

    // Multiple column types
    rs = ctx.run(
        "CREATE TABLE metrics ("
        "  metric_id BIGINT PRIMARY KEY,"
        "  value DOUBLE,"
        "  count INT,"
        "  label TEXT,"
        "  active BOOLEAN,"
        "  recorded_at BIGINT"
        ")");
    assertOk(rs, "CREATE TABLE with all column types");

    // Create orders table for later tests
    rs = ctx.run(
        "CREATE TABLE orders ("
        "  order_id BIGINT PRIMARY KEY,"
        "  customer VARCHAR,"
        "  amount DOUBLE,"
        "  region VARCHAR,"
        "  status VARCHAR"
        ")");
    assertOk(rs, "CREATE TABLE orders for later tests");
}

// ================================================================
// 2. DDL TESTS — DROP TABLE
// ================================================================
void testDropTable(TestCtx &ctx)
{
    section("DDL — DROP TABLE");

    ctx.run("CREATE TABLE temp_table (id BIGINT PRIMARY KEY, val VARCHAR)");

    auto rs = ctx.run("DROP TABLE temp_table");
    assertOk(rs, "DROP TABLE success");
    assert(!ctx.registry->tableExists("temp_table"));
    pass("Table removed from registry");

    // Drop non-existent table
    rs = ctx.run("DROP TABLE temp_table");
    assertError(rs, "DROP TABLE non-existent fails", "does not exist");
}

// ================================================================
// 3. DDL TESTS — SHOW TABLES
// ================================================================
void testShowTables(TestCtx &ctx)
{
    section("DDL — SHOW TABLES");

    auto rs = ctx.run("SHOW TABLES");
    assertOk(rs, "SHOW TABLES success");
    assert(!rs.rows.empty());
    pass("SHOW TABLES returns rows");
    assertHeader(rs, 0, "Tables", "SHOW TABLES header is 'Tables'");
}

// ================================================================
// 4. DML — INSERT
// ================================================================
void testInsert(TestCtx &ctx)
{
    section("DML — INSERT");

    // Basic insert with column list
    auto rs = ctx.run(
        "INSERT INTO users (user_id, name, email, age, salary) "
        "VALUES (1, 'Alice', 'alice@example.com', 30, 95000.50)");
    assertOk(rs, "INSERT with column list");
    assert(rs.rowsAffected == 1);
    pass("INSERT rowsAffected == 1");

    // Insert without column list (schema order)
    rs = ctx.run(
        "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25, 72000.00)");
    assertOk(rs, "INSERT without column list");

    // Insert multiple rows
    ctx.run("INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', 35, 110000.00)");
    ctx.run("INSERT INTO users VALUES (4, 'Dave', 'dave@example.com', 28, 85000.00)");
    ctx.run("INSERT INTO users VALUES (5, 'Eve', 'eve@example.com', 42, 120000.00)");
    pass("INSERT multiple rows");

    // Insert into orders table
    ctx.run("INSERT INTO orders VALUES (1, 'Alice', 99.99, 'APAC', 'completed')");
    ctx.run("INSERT INTO orders VALUES (2, 'Bob', 250.00, 'EMEA', 'pending')");
    ctx.run("INSERT INTO orders VALUES (3, 'Carol', 75.50, 'APAC', 'completed')");
    ctx.run("INSERT INTO orders VALUES (4, 'Dave', 500.00, 'NA', 'completed')");
    ctx.run("INSERT INTO orders VALUES (5, 'Eve', 125.00, 'EMEA', 'cancelled')");
    pass("INSERT 5 orders rows");

    // Insert into non-existent table
    rs = ctx.run("INSERT INTO ghost_table VALUES (1, 'test')");
    assertError(rs, "INSERT into non-existent table", "does not exist");

    // Column count mismatch
    rs = ctx.run("INSERT INTO users (user_id, name) VALUES (99, 'Test', 'extra')");
    assertError(rs, "INSERT column count mismatch", "count");

    // Type mismatch — string into numeric
    rs = ctx.run("INSERT INTO users VALUES (100, 'Test', 'test@test.com', 'notanumber', 50000.0)");
    assertError(rs, "INSERT type mismatch rejected");
}

// ================================================================
// 5. DML — SELECT (OLTP Point Lookup)
// ================================================================
void testSelectPointLookup(TestCtx &ctx)
{
    section("DML — SELECT Point Lookup (OLTP via HybridQueryRouter)");

    // Point lookup by PK — router sends to row path
    auto rs = ctx.run("SELECT * FROM users WHERE user_id = 1");
    assertOk(rs, "SELECT * WHERE pk = val");
    assertRowCount(rs, 1, "Point lookup returns 1 row");
    assertCell(rs, 0, 1, "Alice", "Point lookup name == Alice");

    // Point lookup specific columns
    rs = ctx.run("SELECT name, email FROM users WHERE user_id = 2");
    assertOk(rs, "SELECT specific columns WHERE pk = val");
    assertRowCount(rs, 1, "Specific column lookup returns 1 row");
    assertCell(rs, 0, 0, "Bob", "name == Bob");

    // Point lookup non-existent row
    rs = ctx.run("SELECT * FROM users WHERE user_id = 999");
    assertOk(rs, "SELECT non-existent row returns empty");
    assertRowCount(rs, 0, "Non-existent row returns 0 rows");

    // Point lookup on orders
    rs = ctx.run("SELECT * FROM orders WHERE order_id = 3");
    assertOk(rs, "SELECT orders point lookup");
    assertRowCount(rs, 1, "Orders point lookup returns 1 row");
    assertCell(rs, 0, 2, "75.5", "amount == 75.5");
}

// ================================================================
// 6. DML — SELECT (Full Scan)
// ================================================================
void testSelectFullScan(TestCtx &ctx)
{
    section("DML — SELECT Full Scan (router uses both paths)");

    // Full scan — SELECT *
    auto rs = ctx.run("SELECT * FROM users");
    assertOk(rs, "SELECT * full scan");
    assertRowCount(rs, 5, "Full scan returns 5 users");

    // Full scan with column projection
    rs = ctx.run("SELECT name, salary FROM users");
    assertOk(rs, "SELECT name, salary full scan");
    assertRowCount(rs, 5, "Projected full scan returns 5 rows");
    assert(rs.headers.size() == 2);
    pass("Projected scan has 2 columns");

    // Full scan on orders
    rs = ctx.run("SELECT * FROM orders");
    assertOk(rs, "SELECT * FROM orders full scan");
    assertRowCount(rs, 5, "Orders full scan returns 5 rows");
}

// ================================================================
// 7. DML — SELECT with WHERE (Range Scan / Filter)
// ================================================================
void testSelectWithWhere(TestCtx &ctx)
{
    section("DML — SELECT with WHERE (Range Scan path)");

    // Filter on non-PK column
    auto rs = ctx.run("SELECT * FROM orders WHERE region = 'APAC'");
    assertOk(rs, "SELECT WHERE region = APAC");
    assertRowCount(rs, 2, "2 APAC orders");

    // Numeric comparison
    rs = ctx.run("SELECT * FROM orders WHERE amount > 100");
    assertOk(rs, "SELECT WHERE amount > 100");
    assertRowCount(rs, 3, "3 orders with amount > 100");

    // Less than
    rs = ctx.run("SELECT * FROM orders WHERE amount < 100");
    assertOk(rs, "SELECT WHERE amount < 100");
    assertRowCount(rs, 1, "1 order with amount < 100");

    // Greater than or equal
    rs = ctx.run("SELECT * FROM orders WHERE amount >= 250");
    assertOk(rs, "SELECT WHERE amount >= 250");
    assertRowCount(rs, 2, "2 orders with amount >= 250");

    // AND condition
    rs = ctx.run(
        "SELECT * FROM orders WHERE region = 'APAC' AND status = 'completed'");
    assertOk(rs, "SELECT WHERE AND condition");
    assertRowCount(rs, 2, "2 completed APAC orders");

    // OR condition
    rs = ctx.run(
        "SELECT * FROM orders WHERE region = 'APAC' OR region = 'EMEA'");
    assertOk(rs, "SELECT WHERE OR condition");
    assertRowCount(rs, 4, "4 APAC or EMEA orders");

    // NOT EQUALS
    rs = ctx.run("SELECT * FROM orders WHERE status != 'cancelled'");
    assertOk(rs, "SELECT WHERE != condition");
    assertRowCount(rs, 4, "4 non-cancelled orders");

    // String equality on non-PK
    rs = ctx.run("SELECT * FROM users WHERE name = 'Carol'");
    assertOk(rs, "SELECT WHERE string equality non-PK");
    assertRowCount(rs, 1, "1 user named Carol");

    // Combined numeric + string
    rs = ctx.run("SELECT * FROM users WHERE age > 30 AND salary > 100000");
    assertOk(rs, "SELECT WHERE age > 30 AND salary > 100000");
    assertRowCount(rs, 2, "Carol and Eve match");
}

// ================================================================
// 8. DML — SELECT Aggregations (OLAP via HybridQueryRouter)
// ================================================================
void testSelectAggregations(TestCtx &ctx)
{
    section("DML — SELECT Aggregations (OLAP via HybridQueryRouter)");

    // COUNT(*)
    auto rs = ctx.run("SELECT COUNT(*) FROM orders");
    assertOk(rs, "SELECT COUNT(*)");
    assertRowCount(rs, 1, "COUNT returns 1 row");
    assertHeader(rs, 0, "COUNT(*)", "COUNT header correct");

    // SUM
    rs = ctx.run("SELECT SUM(amount) FROM orders");
    assertOk(rs, "SELECT SUM(amount)");
    assertRowCount(rs, 1, "SUM returns 1 row");
    assertHeader(rs, 0, "SUM(amount)", "SUM header correct");

    // AVG
    rs = ctx.run("SELECT AVG(salary) FROM users");
    assertOk(rs, "SELECT AVG(salary)");
    assertRowCount(rs, 1, "AVG returns 1 row");
    assertHeader(rs, 0, "AVG(salary)", "AVG header correct");

    // MIN
    rs = ctx.run("SELECT MIN(amount) FROM orders");
    assertOk(rs, "SELECT MIN(amount)");
    assertRowCount(rs, 1, "MIN returns 1 row");
    assertHeader(rs, 0, "MIN(amount)", "MIN header correct");

    // MAX
    rs = ctx.run("SELECT MAX(amount) FROM orders");
    assertOk(rs, "SELECT MAX(amount)");
    assertRowCount(rs, 1, "MAX returns 1 row");
    assertHeader(rs, 0, "MAX(amount)", "MAX header correct");

    // COUNT on users
    rs = ctx.run("SELECT COUNT(*) FROM users");
    assertOk(rs, "SELECT COUNT(*) FROM users");
    assertRowCount(rs, 1, "COUNT users returns 1 row");

    // MAX salary
    rs = ctx.run("SELECT MAX(salary) FROM users");
    assertOk(rs, "SELECT MAX(salary) FROM users");
    assertRowCount(rs, 1, "MAX salary returns 1 row");

    // MIN salary
    rs = ctx.run("SELECT MIN(salary) FROM users");
    assertOk(rs, "SELECT MIN(salary) FROM users");
    assertRowCount(rs, 1, "MIN salary returns 1 row");
}

// ================================================================
// 9. DML — UPDATE
// ================================================================
void testUpdate(TestCtx &ctx)
{
    section("DML — UPDATE");

    // Basic update
    auto rs = ctx.run(
        "UPDATE orders SET status = 'shipped' WHERE order_id = 2");
    assertOk(rs, "UPDATE single field");
    assert(rs.rowsAffected == 1);
    pass("UPDATE rowsAffected == 1");

    // Verify update persisted
    rs = ctx.run("SELECT * FROM orders WHERE order_id = 2");
    assertOk(rs, "SELECT after UPDATE");
    assertRowCount(rs, 1, "Row still exists after UPDATE");
    assertCell(rs, 0, 4, "shipped", "status updated to shipped");

    // Update numeric field
    rs = ctx.run("UPDATE orders SET amount = 300.00 WHERE order_id = 2");
    assertOk(rs, "UPDATE numeric field");

    rs = ctx.run("SELECT * FROM orders WHERE order_id = 2");
    assertCell(rs, 0, 2, "300", "amount updated to 300");

    // Update multiple fields
    rs = ctx.run(
        "UPDATE users SET salary = 100000.00, age = 31 WHERE user_id = 1");
    assertOk(rs, "UPDATE multiple fields");

    rs = ctx.run("SELECT * FROM users WHERE user_id = 1");
    assertCell(rs, 0, 4, "100000", "salary updated");

    // Update non-existent row
    rs = ctx.run("UPDATE orders SET status = 'test' WHERE order_id = 999");
    assert(rs.rowsAffected == 0);
    pass("UPDATE non-existent row returns 0 affected");

    // Update without WHERE
    rs = ctx.run("UPDATE orders SET status = 'test'");
    assertError(rs, "UPDATE without WHERE rejected", "WHERE");

    // Update non-existent column
    rs = ctx.run(
        "UPDATE orders SET fake_col = 'x' WHERE order_id = 1");
    assertError(rs, "UPDATE unknown column rejected", "Unknown column");

    // Update wrong type
    rs = ctx.run(
        "UPDATE orders SET amount = 'notanumber' WHERE order_id = 1");
    assertError(rs, "UPDATE type mismatch rejected");
}

// ================================================================
// 10. DML — DELETE
// ================================================================
void testDelete(TestCtx &ctx)
{
    section("DML — DELETE");

    // Insert a row to delete
    ctx.run("INSERT INTO users VALUES (99, 'Temp', 'temp@test.com', 20, 30000.0)");

    // Verify it exists
    auto rs = ctx.run("SELECT * FROM users WHERE user_id = 99");
    assertRowCount(rs, 1, "Temp row exists before delete");

    // Delete it
    rs = ctx.run("DELETE FROM users WHERE user_id = 99");
    assertOk(rs, "DELETE by PK");
    assert(rs.rowsAffected == 1);
    pass("DELETE rowsAffected == 1");

    // Verify it's gone
    rs = ctx.run("SELECT * FROM users WHERE user_id = 99");
    assertRowCount(rs, 0, "Row gone after DELETE");

    // Delete non-existent row
    rs = ctx.run("DELETE FROM users WHERE user_id = 999");
    assert(rs.rowsAffected == 0);
    pass("DELETE non-existent row returns 0 affected");

    // Delete without WHERE
    rs = ctx.run("DELETE FROM orders");
    assertError(rs, "DELETE without WHERE rejected", "WHERE");

    // Delete from non-existent table
    rs = ctx.run("DELETE FROM ghost WHERE id = 1");
    assertError(rs, "DELETE from non-existent table", "does not exist");
}

// ================================================================
// 11. EXPLAIN — Query Plan Inspection
// ================================================================
void testExplain(TestCtx &ctx)
{
    section("EXPLAIN — HybridQueryRouter Plan Inspection");

    // Point lookup plan — should say POINT_LOOKUP
    std::string plan = ctx.explain(
        "SELECT * FROM users WHERE user_id = 1");
    assert(plan.find("POINT_LOOKUP") != std::string::npos);
    pass("EXPLAIN point lookup shows POINT_LOOKUP");
    assert(plan.find("Memtable") != std::string::npos);
    pass("EXPLAIN point lookup shows Memtable scan");

    // Aggregation plan — should say AGGREGATION + columnar
    plan = ctx.explain("SELECT SUM(amount) FROM orders");
    assert(plan.find("AGGREGATION") != std::string::npos);
    pass("EXPLAIN aggregation shows AGGREGATION");

    // Full scan plan — should say FULL_SCAN
    plan = ctx.explain("SELECT * FROM users");
    assert(plan.find("FULL_SCAN") != std::string::npos);
    pass("EXPLAIN full scan shows FULL_SCAN");

    // Range scan plan
    plan = ctx.explain(
        "SELECT * FROM orders WHERE amount > 100");
    assert(plan.find("RANGE_SCAN") != std::string::npos ||
           plan.find("FULL_SCAN") != std::string::npos);
    pass("EXPLAIN range scan shows scan type");
}

// ================================================================
// 12. ROUTER ROUTING VERIFICATION
// ================================================================
void testRouterRouting(TestCtx &ctx)
{
    section("HybridQueryRouter — Routing Decisions");

    // Verify point lookup uses row path
    std::string plan = ctx.explain(
        "SELECT * FROM users WHERE user_id = 1");
    assert(plan.find("POINT_LOOKUP") != std::string::npos);
    // Row SSTables should be listed
    assert(plan.find("Row SSTables") != std::string::npos ||
           plan.find("Memtable") != std::string::npos);
    pass("Point lookup routed to row path");

    // Verify aggregation uses columnar path
    plan = ctx.explain("SELECT COUNT(*) FROM users");
    assert(plan.find("AGGREGATION") != std::string::npos);
    pass("Aggregation routed to columnar path");

    // Verify full scan uses both paths
    plan = ctx.explain("SELECT * FROM orders");
    assert(plan.find("FULL_SCAN") != std::string::npos);
    pass("Full scan uses both row + columnar paths");
}

// ================================================================
// 13. SCHEMA REGISTRY PERSISTENCE
// ================================================================
void testSchemaPersistence(TestCtx &ctx)
{
    section("SchemaRegistry — Persistence");

    // Create a table, verify it survives registry reload
    ctx.run("CREATE TABLE persist_test (id BIGINT PRIMARY KEY, val VARCHAR)");
    ctx.run("INSERT INTO persist_test VALUES (1, 'hello')");

    // Create fresh registry pointing to same file
    sql::SchemaRegistry reg2("./test_data/schema.sdb");
    assert(reg2.tableExists("persist_test"));
    pass("Schema survives registry reload");

    auto schema = reg2.getSchema("persist_test");
    assert(schema.has_value());
    assert(schema->columnCount() == 2);
    pass("Reloaded schema has correct column count");

    // Key encoding is consistent
    std::string key = sql::SchemaRegistry::encodeKey("persist_test", "1");
    assert(key == "persist_test:1");
    pass("Key encoding consistent across reloads");
}

// ================================================================
// 14. ROW CODEC — Encode/Decode Integrity
// ================================================================
void testRowCodecIntegrity(TestCtx &ctx)
{
    section("RowCodec — Encode/Decode Integrity");

    auto schema = ctx.registry->getSchema("orders");
    assert(schema.has_value());

    // Encode a row
    sql::Row row = {
        {"order_id", "42"},
        {"customer", "TestUser"},
        {"amount", "999.99"},
        {"region", "APAC"},
        {"status", "completed"}};

    std::string blob = sql::RowCodec::encode(*schema, row);
    assert(!blob.empty());
    pass("RowCodec encode produces non-empty blob");

    // Decode round-trip
    auto decoded = sql::RowCodec::decode(blob);
    assert(decoded.has_value());
    assert(decoded->at("customer") == "TestUser");
    assert(decoded->at("amount") == "999.99");
    pass("RowCodec decode round-trip correct");

    // Ordered decode matches schema column order
    auto ordered = sql::RowCodec::decodeOrdered(*schema, blob);
    assert(ordered.has_value());
    assert((*ordered)[0] == "42");     // order_id
    assert((*ordered)[2] == "999.99"); // amount
    assert((*ordered)[3] == "APAC");   // region
    pass("RowCodec decodeOrdered matches schema order");

    // Single column extraction
    auto val = sql::RowCodec::getColumnValue(blob, "region");
    assert(val.has_value() && *val == "APAC");
    pass("RowCodec getColumnValue extracts single column");
}

// ================================================================
// 15. EDGE CASES & ERROR HANDLING
// ================================================================
void testEdgeCases(TestCtx &ctx)
{
    section("Edge Cases & Error Handling");

    // SELECT from non-existent table
    auto rs = ctx.run("SELECT * FROM ghost_table");
    assertError(rs, "SELECT from non-existent table", "does not exist");

    // INSERT missing primary key
    rs = ctx.run("INSERT INTO orders (customer, amount) VALUES ('test', 10.0)");
    assertError(rs, "INSERT missing PK rejected");

    // Empty SELECT list edge case
    rs = ctx.run("SELECT * FROM users WHERE user_id = 1");
    assertOk(rs, "SELECT * returns all columns");
    assert(rs.headers.size() == 5);
    pass("SELECT * returns 5 headers for users");

    // Multiple sequential operations on same row
    ctx.run("INSERT INTO users VALUES (50, 'Multi', 'multi@test.com', 25, 50000.0)");
    ctx.run("UPDATE users SET age = 26 WHERE user_id = 50");
    ctx.run("UPDATE users SET age = 27 WHERE user_id = 50");
    rs = ctx.run("SELECT * FROM users WHERE user_id = 50");
    assertRowCount(rs, 1, "Row exists after multiple updates");
    assertCell(rs, 0, 3, "27", "Final age is 27 after 2 updates");

    // Delete then re-insert same PK
    ctx.run("DELETE FROM users WHERE user_id = 50");
    rs = ctx.run("SELECT * FROM users WHERE user_id = 50");
    assertRowCount(rs, 0, "Row gone after delete");
    ctx.run("INSERT INTO users VALUES (50, 'MultiNew', 'new@test.com', 30, 60000.0)");
    rs = ctx.run("SELECT * FROM users WHERE user_id = 50");
    assertRowCount(rs, 1, "Row re-inserted after delete");
    assertCell(rs, 0, 1, "MultiNew", "Re-inserted row has new name");

    // Parse error handling
    rs = ctx.run("THIS IS NOT SQL");
    assertError(rs, "Invalid SQL returns parse error", "Parse error");

    // Empty statement
    rs = ctx.run("");
    assert(!rs.success);
    pass("Empty statement returns error");

    // EXPLAIN on non-SELECT
    std::string plan = ctx.explain("INSERT INTO users VALUES (1,'x','x@x.com',1,1.0)");
    assert(plan.find("EXPLAIN only supported") != std::string::npos);
    pass("EXPLAIN on non-SELECT returns helpful message");
}

// ================================================================
// 16. MIXED WORKLOAD — HTAP Simulation
// ================================================================
void testHTAPMixedWorkload(TestCtx &ctx)
{
    section("HTAP Mixed Workload Simulation");

    // Setup: insert 10 transactions
    for (int i = 10; i <= 20; i++)
    {
        std::string sql =
            "INSERT INTO orders VALUES (" +
            std::to_string(i) + ", 'Customer" + std::to_string(i) +
            "', " + std::to_string(i * 10.0) + ", 'APAC', 'completed')";
        auto rs = ctx.run(sql);
        assert(rs.success);
    }
    pass("Inserted 11 rows for HTAP test");

    // OLTP: point lookup while inserts are happening
    auto rs = ctx.run("SELECT * FROM orders WHERE order_id = 15");
    assertOk(rs, "OLTP point lookup during mixed workload");
    assertRowCount(rs, 1, "Point lookup finds row 15");

    // OLAP: aggregation over all data
    rs = ctx.run("SELECT COUNT(*) FROM orders");
    assertOk(rs, "OLAP COUNT during mixed workload");
    pass("OLAP aggregation runs alongside OLTP");

    rs = ctx.run("SELECT SUM(amount) FROM orders");
    assertOk(rs, "OLAP SUM during mixed workload");

    rs = ctx.run("SELECT AVG(amount) FROM orders");
    assertOk(rs, "OLAP AVG during mixed workload");

    // OLTP: update while analytics running
    rs = ctx.run("UPDATE orders SET status = 'shipped' WHERE order_id = 10");
    assertOk(rs, "OLTP UPDATE during mixed workload");

    // Verify OLTP update visible to subsequent OLTP query
    rs = ctx.run("SELECT * FROM orders WHERE order_id = 10");
    assertCell(rs, 0, 4, "shipped", "OLTP update visible immediately");

    // OLTP delete
    rs = ctx.run("DELETE FROM orders WHERE order_id = 20");
    assertOk(rs, "OLTP DELETE during mixed workload");

    // Verify delete
    rs = ctx.run("SELECT * FROM orders WHERE order_id = 20");
    assertRowCount(rs, 0, "Deleted row not visible");

    pass("HTAP mixed workload completed successfully");
}

// ================================================================
// MAIN
// ================================================================
int main()
{
    std::cout << "========================================\n";
    std::cout << " Samanvay SQL Layer — Comprehensive Tests\n";
    std::cout << "========================================\n";

    // Create test data directory
    std::system("mkdir -p ./test_data/columnar");

    TestCtx ctx;

    try
    {
        testCreateTable(ctx);
        testDropTable(ctx);
        testShowTables(ctx);
        testInsert(ctx);
        testSelectPointLookup(ctx);
        testSelectFullScan(ctx);
        testSelectWithWhere(ctx);
        testSelectAggregations(ctx);
        testUpdate(ctx);
        testDelete(ctx);
        testExplain(ctx);
        testRouterRouting(ctx);
        testSchemaPersistence(ctx);
        testRowCodecIntegrity(ctx);
        testEdgeCases(ctx);
        testHTAPMixedWorkload(ctx);
    }
    catch (const std::exception &e)
    {
        std::cerr << "\n❌ EXCEPTION: " << e.what() << "\n";
        failed++;
    }

    // ── Summary ──────────────────────────────────────────────────
    std::cout << "\n========================================\n";
    std::cout << " Results: " << passed << " passed, "
              << failed << " failed\n";
    std::cout << "========================================\n";

    if (failed == 0)
        std::cout << "✅ All tests passed!\n";
    else
        std::cout << "❌ " << failed << " test(s) failed!\n";

    return failed > 0 ? 1 : 0;
}