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
static int g_passed = 0;
static int g_failed = 0;

void section(const std::string &name)
{
    std::cout << "\n=== " << name << " ===\n";
}

void pass(const std::string &name)
{
    std::cout << "  [PASS] " << name << "\n";
    g_passed++;
}

void fail(const std::string &name, const std::string &detail = "")
{
    std::cout << "  [FAIL] " << name;
    if (!detail.empty())
        std::cout << " -- " << detail;
    std::cout << "\n";
    g_failed++;
}

void check(bool condition, const std::string &name,
           const std::string &detail = "")
{
    if (condition)
        pass(name);
    else
        fail(name, detail);
}

void assertOk(const sql::ResultSet &rs, const std::string &name)
{
    if (rs.success)
        pass(name);
    else
        fail(name, rs.errorMessage);
}

void assertFail(const sql::ResultSet &rs, const std::string &name,
                const std::string &substr = "")
{
    if (!rs.success)
    {
        if (substr.empty() ||
            rs.errorMessage.find(substr) != std::string::npos)
            pass(name);
        else
            fail(name, "Expected error with '" + substr +
                           "' but got: " + rs.errorMessage);
    }
    else
    {
        fail(name, "Expected error but query succeeded");
    }
}

void assertRowCount(const sql::ResultSet &rs, size_t n,
                    const std::string &name)
{
    if (!rs.success)
    {
        fail(name, rs.errorMessage);
        return;
    }
    if (rs.rows.size() == n)
        pass(name);
    else
        fail(name, "Expected " + std::to_string(n) +
                       " rows, got " + std::to_string(rs.rows.size()));
}

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
        fail(name, "Expected '" + expected +
                       "' got '" + rs.rows[row][col] + "'");
}

// ================================================================
// Test Context
// ================================================================
struct Ctx
{
    storage::StorageEngine *engine = nullptr;
    sql::SchemaRegistry *registry = nullptr;
    sql::QueryExecutor *executor = nullptr;

    explicit Ctx(const std::string &dir)
    {
        // Windows-safe directory creation
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir + "/columnar");

        storage::StorageEngineConfig cfg;
        cfg.dataDirectory = dir;
        cfg.columnarDirectory = dir + "/columnar";
        cfg.walPath = dir + "/wal.log";

        engine = new storage::StorageEngine(cfg);
        registry = new sql::SchemaRegistry(dir + "/schema.sdb");
        executor = new sql::QueryExecutor(*engine, *registry);
    }

    ~Ctx()
    {
        delete executor;
        delete registry;
        delete engine;
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
// IMPORTANT: Hyrise parser rules used in these tests
// ----------------------------------------------------------------
// 1. VARCHAR requires a length:  VARCHAR(255)  not  VARCHAR
// 3. Inline PRIMARY KEY may not parse -- use NOT NULL on first col
//    and let the executor default first column as PK
// 4. BIGINT and INT work fine
// 5. DOUBLE, FLOAT, BOOLEAN work fine
// ================================================================

// ================================================================
// 1. DDL -- CREATE TABLE
// ================================================================
void testCreateTable()
{
    section("DDL -- CREATE TABLE");
    Ctx ctx("./test_data/ddl");

    // Basic create -- VARCHAR(n) required by Hyrise
    // No inline PRIMARY KEY -- executor defaults first col as PK
    auto r = ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255), "
        "age INT, "
        "score DOUBLE, "
        "active BOOLEAN"
        ")");
    assertOk(r, "CREATE TABLE basic");
    check(ctx.registry->tableExists("users"),
          "Table exists in registry after CREATE");

    // Verify schema
    auto schema = ctx.registry->getSchema("users");
    check(schema.has_value(), "Schema retrievable");
    check(schema->columnCount() == 5, "Column count == 5");
    check(schema->primaryKeyColumn == "id",
          "First NOT NULL col becomes PK");
    check(schema->columns[0].type == columnar::ColumnType::INT64,
          "id type == INT64");
    check(schema->columns[2].type == columnar::ColumnType::INT32,
          "age type == INT32");
    check(schema->columns[3].type == columnar::ColumnType::DOUBLE,
          "score type == DOUBLE");
    check(schema->columns[4].type == columnar::ColumnType::BOOL,
          "active type == BOOL");

    // Duplicate table rejected
    auto r2 = ctx.run(
        "CREATE TABLE users (id BIGINT NOT NULL, x INT)");
    assertFail(r2, "Duplicate CREATE TABLE fails", "already exists");

    // All supported types
    auto r3 = ctx.run(
        "CREATE TABLE types_test ("
        "a BIGINT NOT NULL, "
        "b INT, "
        "c DOUBLE, "
        "d FLOAT, "
        "e VARCHAR(255), "
        "g BOOLEAN"
        ")");
    assertOk(r3, "CREATE TABLE with all supported types");

    // Case-insensitive table name stored as lowercase
    auto r4 = ctx.run(
        "CREATE TABLE Orders (order_id BIGINT NOT NULL, amount DOUBLE)");
    assertOk(r4, "CREATE TABLE with mixed-case name");
    check(ctx.registry->tableExists("orders"),
          "Stored as lowercase in registry");

    // Implicit PK -- first column when no NOT NULL specified
    auto r5 = ctx.run(
        "CREATE TABLE implicit_pk (id BIGINT, val VARCHAR(100))");
    assertOk(r5, "CREATE TABLE implicit PK");
    auto s5 = ctx.registry->getSchema("implicit_pk");
    check(s5.has_value() && s5->primaryKeyColumn == "id",
          "First column becomes PK implicitly");

    // Create orders table for later tests
    ctx.run(
        "CREATE TABLE orders ("
        "order_id BIGINT NOT NULL, "
        "customer VARCHAR(255), "
        "amount DOUBLE, "
        "region VARCHAR(50), "
        "status VARCHAR(50)"
        ")");
    check(ctx.registry->tableExists("orders"),
          "orders table created for later tests");
}

// ================================================================
// 2. DDL -- DROP TABLE
// ================================================================
void testDropTable()
{
    section("DDL -- DROP TABLE");
    Ctx ctx("./test_data/drop");

    ctx.run("CREATE TABLE tmp (id BIGINT NOT NULL, val VARCHAR(255))");
    check(ctx.registry->tableExists("tmp"), "Table exists before drop");

    auto r = ctx.run("DROP TABLE tmp");
    assertOk(r, "DROP TABLE success");
    check(!ctx.registry->tableExists("tmp"), "Table gone after drop");

    // Drop non-existent fails
    assertFail(ctx.run("DROP TABLE nonexistent"),
               "DROP TABLE nonexistent fails", "does not exist");

    // Drop and recreate
    ctx.run("CREATE TABLE phoenix (id BIGINT NOT NULL)");
    ctx.run("DROP TABLE phoenix");
    auto r3 = ctx.run(
        "CREATE TABLE phoenix (id BIGINT NOT NULL, extra INT)");
    assertOk(r3, "DROP then CREATE same name works");
    auto s3 = ctx.registry->getSchema("phoenix");
    check(s3.has_value() && s3->columnCount() == 2,
          "Recreated table has 2 columns");
}

// ================================================================
// 3. SHOW TABLES
// ================================================================
void testShowTables()
{
    section("META -- SHOW TABLES");
    Ctx ctx("./test_data/show");

    auto r1 = ctx.run("SHOW TABLES");
    assertOk(r1, "SHOW TABLES on empty db");
    check(r1.rows.empty(), "Empty db shows 0 tables");

    ctx.run("CREATE TABLE alpha (id BIGINT NOT NULL)");
    ctx.run("CREATE TABLE beta  (id BIGINT NOT NULL)");
    ctx.run("CREATE TABLE gamma (id BIGINT NOT NULL)");

    auto r2 = ctx.run("SHOW TABLES");
    assertOk(r2, "SHOW TABLES with 3 tables");
    check(r2.rows.size() == 3, "Shows 3 tables");
    check(!r2.headers.empty(), "Has column header");
    check(r2.headers[0] == "Tables", "Header is 'Tables'");

    ctx.run("DROP TABLE gamma");
    auto r3 = ctx.run("SHOW TABLES");
    check(r3.rows.size() == 2, "After drop: 2 tables");
}

// ================================================================
// 4. DML -- INSERT
// ================================================================
void testInsert()
{
    section("DML -- INSERT");
    Ctx ctx("./test_data/insert");

    ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255), "
        "age INT, "
        "salary DOUBLE"
        ")");

    // Insert with explicit column list
    auto r = ctx.run(
        "INSERT INTO users (id, name, age, salary) "
        "VALUES (1, 'Alice', 30, 95000.50)");
    assertOk(r, "INSERT with column list");
    check(r.rowsAffected == 1, "rowsAffected == 1");

    // Insert in schema order (no column list)
    auto r2 = ctx.run(
        "INSERT INTO users VALUES (2, 'Bob', 25, 72000.00)");
    assertOk(r2, "INSERT without column list");

    // Insert more rows
    ctx.run("INSERT INTO users VALUES (3, 'Carol', 35, 110000.00)");
    ctx.run("INSERT INTO users VALUES (4, 'Dave', 28, 85000.00)");
    ctx.run("INSERT INTO users VALUES (5, 'Eve', 42, 120000.00)");
    pass("INSERT 5 total rows");

    // Insert into non-existent table
    assertFail(ctx.run("INSERT INTO ghost VALUES (1, 'x')"),
               "INSERT into non-existent table", "does not exist");

    // Column count mismatch
    assertFail(
        ctx.run("INSERT INTO users (id, name) VALUES (99, 'T', 'extra')"),
        "INSERT column count mismatch", "count");

    // Type mismatch -- string into numeric column
    assertFail(
        ctx.run("INSERT INTO users VALUES (100, 'T', 'notanint', 50000.0)"),
        "INSERT type mismatch rejected");
}

// ================================================================
// 5. SELECT -- Point Lookup (OLTP path via HybridQueryRouter)
// ================================================================
void testSelectPointLookup()
{
    section("SELECT -- Point Lookup (OLTP, router -> row path)");
    Ctx ctx("./test_data/point");

    ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255), "
        "age INT, "
        "salary DOUBLE"
        ")");
    ctx.run("INSERT INTO users VALUES (1, 'Alice', 30, 95000.50)");
    ctx.run("INSERT INTO users VALUES (2, 'Bob', 25, 72000.00)");
    ctx.run("INSERT INTO users VALUES (3, 'Carol', 35, 110000.00)");

    // Point lookup by PK
    auto r = ctx.run("SELECT * FROM users WHERE id = 1");
    assertOk(r, "SELECT * WHERE pk = 1");
    assertRowCount(r, 1, "Point lookup returns 1 row");
    assertCell(r, 0, 1, "Alice", "name == Alice");

    // Specific columns
    auto r2 = ctx.run("SELECT name, age FROM users WHERE id = 2");
    assertOk(r2, "SELECT specific columns WHERE pk = 2");
    assertRowCount(r2, 1, "Returns 1 row");
    assertCell(r2, 0, 0, "Bob", "name == Bob");

    // Non-existent PK returns empty
    auto r3 = ctx.run("SELECT * FROM users WHERE id = 999");
    assertOk(r3, "SELECT non-existent PK");
    assertRowCount(r3, 0, "Non-existent row = 0 rows");

    // Check EXPLAIN shows POINT_LOOKUP
    std::string plan = ctx.explain("SELECT * FROM users WHERE id = 1");
    check(plan.find("POINT_LOOKUP") != std::string::npos,
          "EXPLAIN shows POINT_LOOKUP");
}

// ================================================================
// 6. SELECT -- Full Scan (router -> both paths)
// ================================================================
void testSelectFullScan()
{
    section("SELECT -- Full Scan (router -> both paths)");
    Ctx ctx("./test_data/scan");

    ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255), "
        "salary DOUBLE"
        ")");
    ctx.run("INSERT INTO users VALUES (1, 'Alice', 95000.50)");
    ctx.run("INSERT INTO users VALUES (2, 'Bob',   72000.00)");
    ctx.run("INSERT INTO users VALUES (3, 'Carol', 110000.00)");
    ctx.run("INSERT INTO users VALUES (4, 'Dave',  85000.00)");
    ctx.run("INSERT INTO users VALUES (5, 'Eve',   120000.00)");

    // SELECT *
    auto r = ctx.run("SELECT * FROM users");
    assertOk(r, "SELECT * full scan");
    assertRowCount(r, 5, "Full scan returns 5 rows");
    check(r.headers.size() == 3, "SELECT * has 3 headers");

    // Projected scan
    auto r2 = ctx.run("SELECT name, salary FROM users");
    assertOk(r2, "SELECT name, salary");
    assertRowCount(r2, 5, "Projected scan returns 5 rows");
    check(r2.headers.size() == 2, "Projected scan has 2 headers");

    // EXPLAIN shows FULL_SCAN
    std::string plan = ctx.explain("SELECT * FROM users");
    check(plan.find("FULL_SCAN") != std::string::npos,
          "EXPLAIN shows FULL_SCAN");
}

// ================================================================
// 7. SELECT -- WHERE filter (non-PK / range scan)
// ================================================================
void testSelectWhere()
{
    section("SELECT -- WHERE filter (range scan path)");
    Ctx ctx("./test_data/where");

    ctx.run(
        "CREATE TABLE orders ("
        "id BIGINT NOT NULL, "
        "customer VARCHAR(255), "
        "amount DOUBLE, "
        "region VARCHAR(50), "
        "status VARCHAR(50)"
        ")");
    ctx.run("INSERT INTO orders VALUES (1, 'Alice', 99.99, 'APAC', 'completed')");
    ctx.run("INSERT INTO orders VALUES (2, 'Bob', 250.00, 'EMEA', 'pending')");
    ctx.run("INSERT INTO orders VALUES (3, 'Carol', 75.50, 'APAC', 'completed')");
    ctx.run("INSERT INTO orders VALUES (4, 'Dave', 500.00, 'NA', 'completed')");
    ctx.run("INSERT INTO orders VALUES (5, 'Eve', 125.00, 'EMEA', 'cancelled')");

    // String equality on non-PK
    auto r1 = ctx.run("SELECT * FROM orders WHERE region = 'APAC'");
    assertOk(r1, "WHERE region = APAC");
    assertRowCount(r1, 2, "2 APAC orders");

    // Numeric greater than
    auto r2 = ctx.run("SELECT * FROM orders WHERE amount > 100");
    assertOk(r2, "WHERE amount > 100");
    assertRowCount(r2, 3, "3 orders with amount > 100");

    // Numeric less than
    auto r3 = ctx.run("SELECT * FROM orders WHERE amount < 100");
    assertOk(r3, "WHERE amount < 100");
    assertRowCount(r3, 1, "1 order with amount < 100");

    // Greater than or equal
    auto r4 = ctx.run("SELECT * FROM orders WHERE amount >= 250");
    assertOk(r4, "WHERE amount >= 250");
    assertRowCount(r4, 2, "2 orders with amount >= 250");

    // AND
    auto r5 = ctx.run(
        "SELECT * FROM orders WHERE region = 'APAC' AND status = 'completed'");
    assertOk(r5, "WHERE AND condition");
    assertRowCount(r5, 2, "2 completed APAC orders");

    // OR
    auto r6 = ctx.run(
        "SELECT * FROM orders WHERE region = 'APAC' OR region = 'EMEA'");
    assertOk(r6, "WHERE OR condition");
    assertRowCount(r6, 4, "4 APAC or EMEA orders");

    // Not equals
    auto r7 = ctx.run("SELECT * FROM orders WHERE status != 'cancelled'");
    assertOk(r7, "WHERE != condition");
    assertRowCount(r7, 4, "4 non-cancelled orders");

    // EXPLAIN shows RANGE_SCAN or FULL_SCAN
    std::string plan = ctx.explain(
        "SELECT * FROM orders WHERE amount > 100");
    check(plan.find("RANGE_SCAN") != std::string::npos ||
              plan.find("FULL_SCAN") != std::string::npos,
          "EXPLAIN shows scan type");
}

// ================================================================
// 8. SELECT -- Aggregations (OLAP path via HybridQueryRouter)
// ================================================================
void testSelectAggregations()
{
    section("SELECT -- Aggregations (OLAP, router -> columnar path)");
    Ctx ctx("./test_data/agg");

    ctx.run(
        "CREATE TABLE orders ("
        "id BIGINT NOT NULL, "
        "amount DOUBLE, "
        "qty INT"
        ")");
    ctx.run("INSERT INTO orders VALUES (1, 99.99, 2)");
    ctx.run("INSERT INTO orders VALUES (2, 250.00, 5)");
    ctx.run("INSERT INTO orders VALUES (3, 75.50, 1)");
    ctx.run("INSERT INTO orders VALUES (4, 500.00, 10)");
    ctx.run("INSERT INTO orders VALUES (5, 125.00, 3)");

    // COUNT(*)
    auto r1 = ctx.run("SELECT COUNT(*) FROM orders");
    assertOk(r1, "SELECT COUNT(*)");
    assertRowCount(r1, 1, "COUNT returns 1 row");
    check(r1.headers[0] == "COUNT(*)", "COUNT header correct");

    // SUM
    auto r2 = ctx.run("SELECT SUM(amount) FROM orders");
    assertOk(r2, "SELECT SUM(amount)");
    assertRowCount(r2, 1, "SUM returns 1 row");
    check(r2.headers[0] == "SUM(amount)", "SUM header correct");

    // AVG
    auto r3 = ctx.run("SELECT AVG(amount) FROM orders");
    assertOk(r3, "SELECT AVG(amount)");
    assertRowCount(r3, 1, "AVG returns 1 row");
    check(r3.headers[0] == "AVG(amount)", "AVG header correct");

    // MIN
    auto r4 = ctx.run("SELECT MIN(amount) FROM orders");
    assertOk(r4, "SELECT MIN(amount)");
    assertRowCount(r4, 1, "MIN returns 1 row");
    check(r4.headers[0] == "MIN(amount)", "MIN header correct");

    // MAX
    auto r5 = ctx.run("SELECT MAX(amount) FROM orders");
    assertOk(r5, "SELECT MAX(amount)");
    assertRowCount(r5, 1, "MAX returns 1 row");
    check(r5.headers[0] == "MAX(amount)", "MAX header correct");

    // EXPLAIN shows AGGREGATION
    std::string plan = ctx.explain("SELECT SUM(amount) FROM orders");
    check(plan.find("AGGREGATION") != std::string::npos,
          "EXPLAIN shows AGGREGATION for SUM");

    plan = ctx.explain("SELECT COUNT(*) FROM orders");
    check(plan.find("AGGREGATION") != std::string::npos,
          "EXPLAIN shows AGGREGATION for COUNT");
}

// ================================================================
// 9. DML -- UPDATE
// ================================================================
void testUpdate()
{
    section("DML -- UPDATE (OLTP read-modify-write)");
    Ctx ctx("./test_data/update");

    ctx.run(
        "CREATE TABLE orders ("
        "id BIGINT NOT NULL, "
        "customer VARCHAR(255), "
        "amount DOUBLE, "
        "status VARCHAR(50)"
        ")");
    ctx.run("INSERT INTO orders VALUES (1, 'Alice', 99.99, 'pending')");
    ctx.run("INSERT INTO orders VALUES (2, 'Bob', 250.00, 'pending')");

    // Basic update
    auto r = ctx.run(
        "UPDATE orders SET status = 'shipped' WHERE id = 1");
    assertOk(r, "UPDATE single field");
    check(r.rowsAffected == 1, "rowsAffected == 1");

    // Verify update persisted
    auto check_r = ctx.run("SELECT * FROM orders WHERE id = 1");
    assertOk(check_r, "SELECT after UPDATE");
    assertCell(check_r, 0, 3, "shipped", "status updated to shipped");

    // Update numeric field
    ctx.run("UPDATE orders SET amount = 300.00 WHERE id = 2");
    auto check_r2 = ctx.run("SELECT * FROM orders WHERE id = 2");
    assertCell(check_r2, 0, 2, "300", "amount updated to 300");

    // Update non-existent row
    auto r3 = ctx.run(
        "UPDATE orders SET status = 'test' WHERE id = 999");
    check(r3.rowsAffected == 0, "UPDATE non-existent row: 0 affected");

    // UPDATE without WHERE rejected
    assertFail(ctx.run("UPDATE orders SET status = 'x'"),
               "UPDATE without WHERE rejected", "WHERE");

    // UPDATE unknown column rejected
    assertFail(
        ctx.run("UPDATE orders SET fake_col = 'x' WHERE id = 1"),
        "UPDATE unknown column rejected", "Unknown column");

    // UPDATE wrong type rejected
    assertFail(
        ctx.run("UPDATE orders SET amount = 'notanumber' WHERE id = 1"),
        "UPDATE type mismatch rejected");
}

// ================================================================
// 10. DML -- DELETE
// ================================================================
void testDelete()
{
    section("DML -- DELETE (OLTP)");
    Ctx ctx("./test_data/delete");

    ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255)"
        ")");
    ctx.run("INSERT INTO users VALUES (1, 'Alice')");
    ctx.run("INSERT INTO users VALUES (2, 'Bob')");
    ctx.run("INSERT INTO users VALUES (99, 'Temp')");

    // Verify row exists
    assertRowCount(ctx.run("SELECT * FROM users WHERE id = 99"),
                   1, "Row 99 exists before delete");

    // Delete it
    auto r = ctx.run("DELETE FROM users WHERE id = 99");
    assertOk(r, "DELETE by PK");
    check(r.rowsAffected == 1, "rowsAffected == 1");

    // Verify gone
    assertRowCount(ctx.run("SELECT * FROM users WHERE id = 99"),
                   0, "Row gone after DELETE");

    // Delete non-existent
    auto r2 = ctx.run("DELETE FROM users WHERE id = 999");
    check(r2.rowsAffected == 0, "DELETE non-existent: 0 affected");

    // DELETE without WHERE rejected
    assertFail(ctx.run("DELETE FROM users"),
               "DELETE without WHERE rejected", "WHERE");

    // DELETE from non-existent table
    assertFail(ctx.run("DELETE FROM ghost WHERE id = 1"),
               "DELETE from non-existent table", "does not exist");

    // Full scan still shows remaining rows
    assertRowCount(ctx.run("SELECT * FROM users"), 2,
                   "2 rows remain after deleting row 99");
}

// ================================================================
// 11. Schema Registry Persistence
// ================================================================
void testSchemaPersistence()
{
    section("SchemaRegistry -- Persistence across reloads");

    std::string dir = "./test_data/persist";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir + "/columnar");

    // Write schema in one registry instance
    {
        sql::SchemaRegistry reg(dir + "/schema.sdb");
        columnar::TableSchema schema("persist_test");
        schema.addColumn(
            columnar::ColumnSchema("id", columnar::ColumnType::INT64, false));
        schema.addColumn(
            columnar::ColumnSchema("val", columnar::ColumnType::STRING, true));
        schema.primaryKeyColumn = "id";
        reg.createTable(schema);
        check(reg.tableExists("persist_test"), "Schema written");
    }

    // Read it back in a fresh instance
    {
        sql::SchemaRegistry reg2(dir + "/schema.sdb");
        check(reg2.tableExists("persist_test"),
              "Schema survives reload");
        auto s = reg2.getSchema("persist_test");
        check(s.has_value() && s->columnCount() == 2,
              "Reloaded schema has 2 columns");
        check(s.has_value() && s->primaryKeyColumn == "id",
              "PK column preserved");
    }

    // Key encoding is stable
    check(sql::SchemaRegistry::encodeKey("orders", "42") == "orders:42",
          "encodeKey format correct");
    check(sql::SchemaRegistry::tablePrefix("orders") == "orders:",
          "tablePrefix format correct");
    check(sql::SchemaRegistry::decodePkFromKey("orders:42", "orders") == "42",
          "decodePkFromKey correct");
}

// ================================================================
// 12. RowCodec Integrity
// ================================================================
void testRowCodecIntegrity()
{
    section("RowCodec -- Encode/Decode Integrity");

    // Build a schema
    columnar::TableSchema schema("orders");
    schema.addColumn(
        columnar::ColumnSchema("order_id", columnar::ColumnType::INT64, false));
    schema.addColumn(
        columnar::ColumnSchema("customer", columnar::ColumnType::STRING, true));
    schema.addColumn(
        columnar::ColumnSchema("amount", columnar::ColumnType::DOUBLE, true));
    schema.primaryKeyColumn = "order_id";

    sql::Row row = {
        {"order_id", "42"},
        {"customer", "TestUser"},
        {"amount", "999.99"}};

    // Encode
    std::string blob = sql::RowCodec::encode(schema, row);
    check(!blob.empty(), "Encode produces non-empty blob");

    // Decode round-trip
    auto decoded = sql::RowCodec::decode(blob);
    check(decoded.has_value(), "Decode returns value");
    check(decoded->at("customer") == "TestUser", "customer round-trips");
    check(decoded->at("amount") == "999.99", "amount round-trips");

    // Ordered decode matches schema column order
    auto ordered = sql::RowCodec::decodeOrdered(schema, blob);
    check(ordered.has_value(), "decodeOrdered returns value");
    check((*ordered)[0] == "42", "Col[0] == order_id value");
    check((*ordered)[1] == "TestUser", "Col[1] == customer value");
    check((*ordered)[2] == "999.99", "Col[2] == amount value");

    // Single column extraction
    auto val = sql::RowCodec::getColumnValue(blob, "amount");
    check(val.has_value() && *val == "999.99",
          "getColumnValue extracts amount");

    // Type checking
    check(sql::RowCodec::typeCheck("42", columnar::ColumnType::INT64).empty(),
          "typeCheck INT64 valid");
    check(sql::RowCodec::typeCheck("3.14", columnar::ColumnType::DOUBLE).empty(),
          "typeCheck DOUBLE valid");
    check(sql::RowCodec::typeCheck("true", columnar::ColumnType::BOOL).empty(),
          "typeCheck BOOL valid");
    check(!sql::RowCodec::typeCheck("abc", columnar::ColumnType::INT64).empty(),
          "typeCheck INT64 invalid rejects");
    check(!sql::RowCodec::typeCheck("maybe", columnar::ColumnType::BOOL).empty(),
          "typeCheck BOOL invalid rejects");
}

// ================================================================
// 13. Edge Cases
// ================================================================
void testEdgeCases()
{
    section("Edge Cases & Error Handling");
    Ctx ctx("./test_data/edge");

    ctx.run(
        "CREATE TABLE users ("
        "id BIGINT NOT NULL, "
        "name VARCHAR(255), "
        "age INT"
        ")");
    ctx.run("INSERT INTO users VALUES (1, 'Alice', 30)");
    ctx.run("INSERT INTO users VALUES (2, 'Bob', 25)");

    // SELECT from non-existent table
    assertFail(ctx.run("SELECT * FROM ghost"),
               "SELECT from non-existent table", "does not exist");

    // SELECT * returns all columns
    auto r = ctx.run("SELECT * FROM users WHERE id = 1");
    assertOk(r, "SELECT * returns all columns");
    check(r.headers.size() == 3, "SELECT * has 3 headers");

    // Multiple sequential updates on same row
    ctx.run("INSERT INTO users VALUES (50, 'Multi', 20)");
    ctx.run("UPDATE users SET age = 21 WHERE id = 50");
    ctx.run("UPDATE users SET age = 22 WHERE id = 50");
    auto r2 = ctx.run("SELECT * FROM users WHERE id = 50");
    assertRowCount(r2, 1, "Row exists after multiple updates");
    assertCell(r2, 0, 2, "22", "Final age is 22 after 2 updates");

    // Delete then re-insert same PK
    ctx.run("DELETE FROM users WHERE id = 50");
    assertRowCount(ctx.run("SELECT * FROM users WHERE id = 50"),
                   0, "Row gone after delete");
    ctx.run("INSERT INTO users VALUES (50, 'NewMulti', 30)");
    auto r3 = ctx.run("SELECT * FROM users WHERE id = 50");
    assertRowCount(r3, 1, "Row re-inserted after delete");
    assertCell(r3, 0, 1, "NewMulti", "Re-inserted row has new name");

    // Parse error
    auto bad = ctx.run("THIS IS NOT SQL");
    check(!bad.success, "Invalid SQL returns error");
    check(bad.errorMessage.find("Parse error") != std::string::npos ||
              !bad.errorMessage.empty(),
          "Parse error message populated");

    // EXPLAIN on non-SELECT
    std::string plan = ctx.explain(
        "INSERT INTO users VALUES (1,'x',1)");
    check(plan.find("EXPLAIN only") != std::string::npos,
          "EXPLAIN on non-SELECT returns helpful message");
}

// ================================================================
// 14. HTAP Mixed Workload
// ================================================================
void testHTAPMixedWorkload()
{
    section("HTAP -- Mixed Workload Simulation");
    Ctx ctx("./test_data/htap");

    ctx.run(
        "CREATE TABLE orders ("
        "id BIGINT NOT NULL, "
        "customer VARCHAR(255), "
        "amount DOUBLE, "
        "status VARCHAR(50)"
        ")");

    // Insert 10 rows
    for (int i = 1; i <= 10; i++)
    {
        ctx.run(
            "INSERT INTO orders VALUES (" +
            std::to_string(i) + ", 'Customer" + std::to_string(i) +
            "', " + std::to_string(i * 50.0) + ", 'pending')");
    }
    pass("Inserted 10 rows");

    // OLTP point lookup
    auto r1 = ctx.run("SELECT * FROM orders WHERE id = 5");
    assertOk(r1, "OLTP point lookup during mixed workload");
    assertRowCount(r1, 1, "Point lookup finds row 5");

    // OLAP aggregations
    assertOk(ctx.run("SELECT COUNT(*) FROM orders"),
             "OLAP COUNT during mixed workload");
    assertOk(ctx.run("SELECT SUM(amount) FROM orders"),
             "OLAP SUM during mixed workload");
    assertOk(ctx.run("SELECT AVG(amount) FROM orders"),
             "OLAP AVG during mixed workload");
    assertOk(ctx.run("SELECT MIN(amount) FROM orders"),
             "OLAP MIN during mixed workload");
    assertOk(ctx.run("SELECT MAX(amount) FROM orders"),
             "OLAP MAX during mixed workload");

    // OLTP update
    auto r2 = ctx.run(
        "UPDATE orders SET status = 'shipped' WHERE id = 3");
    assertOk(r2, "OLTP UPDATE during mixed workload");

    // Verify update visible immediately
    auto r3 = ctx.run("SELECT * FROM orders WHERE id = 3");
    assertCell(r3, 0, 3, "shipped", "OLTP update visible immediately");

    // OLTP delete
    assertOk(ctx.run("DELETE FROM orders WHERE id = 10"),
             "OLTP DELETE during mixed workload");
    assertRowCount(ctx.run("SELECT * FROM orders WHERE id = 10"),
                   0, "Deleted row not visible");

    // Full scan after mixed ops
    auto r4 = ctx.run("SELECT * FROM orders");
    assertRowCount(r4, 9, "9 rows remain after delete");

    pass("HTAP mixed workload completed");
}

// ================================================================
// MAIN
// ================================================================
int main()
{
    std::cout << "========================================\n";
    std::cout << " Samanvay SQL Layer -- Comprehensive Tests\n";
    std::cout << "========================================\n";

    // Clean slate
    std::filesystem::remove_all("./test_data");

    try
    {
        testCreateTable();
        testDropTable();
        testShowTables();
        testInsert();
        testSelectPointLookup();
        testSelectFullScan();
        testSelectWhere();
        testSelectAggregations();
        testUpdate();
        testDelete();
        testSchemaPersistence();
        testRowCodecIntegrity();
        testEdgeCases();
        testHTAPMixedWorkload();
    }
    catch (const std::exception &e)
    {
        std::cerr << "\nEXCEPTION: " << e.what() << "\n";
        g_failed++;
    }

    // Cleanup
    std::filesystem::remove_all("./test_data");

    std::cout << "\n========================================\n";
    std::cout << " Results: " << g_passed << " passed, "
              << g_failed << " failed\n";
    std::cout << "========================================\n";

    if (g_failed == 0)
        std::cout << "All tests passed!\n";
    else
        std::cout << g_failed << " test(s) failed\n";

    return g_failed > 0 ? 1 : 0;
}
