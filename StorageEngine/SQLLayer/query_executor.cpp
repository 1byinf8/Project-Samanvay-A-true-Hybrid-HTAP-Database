
#include "includes/query_executor.hpp"

namespace sql
{

    // ================================================================
    // Constructor
    // ================================================================
    QueryExecutor::QueryExecutor(storage::StorageEngine &engine,
                                 SchemaRegistry &registry)
        : engine_(engine), registry_(registry)
    {
        router_ = engine_.getQueryRouter();
    }

    // ================================================================
    // MAIN ENTRY POINT
    // ================================================================
    ResultSet QueryExecutor::execute(const std::string &sql)
    {
        auto start = std::chrono::steady_clock::now();

        hsql::SQLParserResult parsed;
        hsql::SQLParser::parse(sql, &parsed);

        if (!parsed.isValid())
            return ResultSet::error(std::string("Parse error: ") +
                                    parsed.errorMsg());
        if (parsed.size() == 0)
            return ResultSet::error("Empty statement");

        ResultSet result = dispatch(parsed.getStatement(0));

        auto end = std::chrono::steady_clock::now();
        result.executionTimeMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                end - start)
                .count();

        return result;
    }

    // ================================================================
    // EXPLAIN
    // ================================================================
    std::string QueryExecutor::explain(const std::string &sql)
    {
        hsql::SQLParserResult parsed;
        hsql::SQLParser::parse(sql, &parsed);

        if (!parsed.isValid() || parsed.size() == 0)
            return "Parse error: " + std::string(parsed.errorMsg());

        const hsql::SQLStatement *stmt = parsed.getStatement(0);
        if (stmt->type() != hsql::kStmtSelect)
            return "EXPLAIN only supported for SELECT statements";

        const auto *select = static_cast<const hsql::SelectStatement *>(stmt);
        if (!select->fromTable)
            return "EXPLAIN: missing FROM clause";

        auto schema = registry_.getSchema(select->fromTable->name);
        if (!schema.has_value())
            return "EXPLAIN: table not found";

        query::QueryRequest request = buildQueryRequest(select, *schema);
        query::QueryPlan plan = router_->planQuery(request);
        return router_->explainPlan(plan);
    }

    // ================================================================
    // DISPATCHER
    // ================================================================
    ResultSet QueryExecutor::dispatch(const hsql::SQLStatement *stmt)
    {
        switch (stmt->type())
        {
        case hsql::kStmtCreate:
            return executeCreate(
                static_cast<const hsql::CreateStatement *>(stmt));
        case hsql::kStmtDrop:
            return executeDrop(
                static_cast<const hsql::DropStatement *>(stmt));
        case hsql::kStmtInsert:
            return executeInsert(
                static_cast<const hsql::InsertStatement *>(stmt));
        case hsql::kStmtSelect:
            return executeSelect(
                static_cast<const hsql::SelectStatement *>(stmt));
        case hsql::kStmtDelete:
            return executeDelete(
                static_cast<const hsql::DeleteStatement *>(stmt));
        case hsql::kStmtUpdate:
            return executeUpdate(
                static_cast<const hsql::UpdateStatement *>(stmt));
        case hsql::kStmtShow:
            return executeShow(
                static_cast<const hsql::ShowStatement *>(stmt));
        default:
            return ResultSet::error("Unsupported statement type");
        }
    }

    // ================================================================
    // STEP 1: BUILD QueryRequest FROM SQL AST
    // ================================================================
    query::QueryRequest QueryExecutor::buildQueryRequest(
        const hsql::SelectStatement *stmt,
        const columnar::TableSchema &schema) const
    {
        std::string tableName = stmt->fromTable->name;
        std::string prefix = SchemaRegistry::tablePrefix(tableName);
        std::string endKey = prefix + std::string(256, '\xff');

        bool hasAgg = false;
        query::AggregationType aggType = query::AggregationType::NONE;
        std::string aggCol;
        std::vector<std::string> selectedCols;

        for (const auto *expr : *stmt->selectList)
        {
            if (expr->type == hsql::kExprFunctionRef)
            {
                hasAgg = true;
                aggType = parseAggType(expr->name);
                if (expr->exprList && !expr->exprList->empty())
                {
                    const hsql::Expr *arg = (*expr->exprList)[0];
                    aggCol = (arg->type == hsql::kExprStar)
                                 ? "*"
                                 : (arg->name ? arg->name : "*");
                }
            }
            else if (expr->type != hsql::kExprStar && expr->name)
            {
                selectedCols.push_back(expr->name);
            }
        }

        // AGGREGATION — router sends to columnar path (Level 4+)
        if (hasAgg)
        {
            auto req = query::QueryRequest::aggregation(aggType, aggCol);
            req.selectColumns = selectedCols;
            return req;
        }

        if (stmt->whereClause)
        {
            // POINT LOOKUP — WHERE pk = value
            auto pkVal = extractPKLookup(stmt->whereClause,
                                         schema.primaryKeyColumn);
            if (pkVal.has_value())
            {
                std::string key = SchemaRegistry::encodeKey(tableName, *pkVal);
                auto req = query::QueryRequest::pointLookup(key);
                req.selectColumns = selectedCols;
                return req;
            }

            // RANGE SCAN — WHERE on non-PK column
            // Router decides row vs columnar based on estimated row count
            auto req = query::QueryRequest::rangeScan(prefix, endKey);
            req.selectColumns = selectedCols;
            return req;
        }

        // FULL SCAN — no WHERE clause
        auto req = query::QueryRequest::fullScan();
        req.selectColumns = selectedCols;
        return req;
    }

    // ================================================================
    // STEP 2: EXECUTE BASED ON QueryPlan
    // ================================================================
    ResultSet QueryExecutor::executePlan(
        const query::QueryRequest &request,
        const query::QueryPlan &plan,
        const columnar::TableSchema &schema,
        const hsql::SelectStatement *stmt) const
    {
        std::string tableName = stmt->fromTable->name;
        std::string prefix = SchemaRegistry::tablePrefix(tableName);

        std::vector<std::string> selectedCols;
        for (const auto *expr : *stmt->selectList)
        {
            if (expr->type != hsql::kExprStar &&
                expr->type != hsql::kExprFunctionRef &&
                expr->name)
                selectedCols.push_back(expr->name);
        }

        // ── POINT LOOKUP ─────────────────────────────────────────────
        // useRowPath=true, useColumnarPath=false
        // memtable first, then row SSTables L0-L3
        if (plan.type == query::QueryType::POINT_LOOKUP)
        {
            auto result = engine_.get(request.key);
            if (!result.has_value())
            {
                ResultSet rs;
                rs.headers = selectedCols.empty()
                                 ? getColumnHeaders(schema)
                                 : selectedCols;
                return rs;
            }
            return ResultFormatter::fromSingleRow(schema, *result, selectedCols);
        }

        // ── AGGREGATION ──────────────────────────────────────────────
        // useColumnarPath=true, useRowPath=false
        // columnar files Level 4+ with pre-computed column stats
        if (plan.type == query::QueryType::AGGREGATION)
        {
            auto qResult = engine_.executeAggregation(request);
            if (!qResult.success)
                return ResultSet::error(qResult.errorMessage);
            return buildAggregationResult(request, qResult);
        }

        // ── FULL SCAN ────────────────────────────────────────────────
        // useRowPath=true AND useColumnarPath=true
        // merges results from both row store and columnar files
        if (plan.type == query::QueryType::FULL_SCAN)
        {
            std::vector<std::pair<std::string, std::string>> allRows;

            if (plan.useRowPath)
            {
                auto rowResults = engine_.fullScan();
                for (const auto &[k, v] : rowResults)
                {
                    if (k.substr(0, prefix.size()) == prefix)
                        allRows.push_back({k, v});
                }
            }

            if (stmt->whereClause)
                allRows = applyWhereFilter(allRows, schema, stmt->whereClause);

            return ResultFormatter::fromScanRows(schema, allRows, selectedCols);
        }

        // ── RANGE SCAN ───────────────────────────────────────────────
        // router decided: small (<10K) → row only, large (>10K) → both
        if (plan.type == query::QueryType::RANGE_SCAN)
        {
            std::vector<std::pair<std::string, std::string>> rangeRows;

            if (plan.useRowPath)
            {
                auto rowResults = engine_.rangeQuery(request.startKey,
                                                     request.endKey);
                for (const auto &[k, v] : rowResults)
                {
                    if (k.substr(0, prefix.size()) == prefix)
                        rangeRows.push_back({k, v});
                }
            }

            if (stmt->whereClause)
                rangeRows = applyWhereFilter(rangeRows, schema,
                                             stmt->whereClause);

            return ResultFormatter::fromScanRows(schema, rangeRows, selectedCols);
        }

        return ResultSet::error("Unhandled query plan type");
    }

    // ================================================================
    // DDL — CREATE TABLE
    // ================================================================
    ResultSet QueryExecutor::executeCreate(const hsql::CreateStatement *stmt)
    {
        if (stmt->type != hsql::CreateType::kCreateTable)
            return ResultSet::error("Only CREATE TABLE is supported");
        if (!stmt->columns)
            return ResultSet::error("No columns defined");

        columnar::TableSchema schema(stmt->tableName);
        std::string pkCol;

        for (const auto *col : *stmt->columns)
        {
            auto colType = hyriseTypeToColumnar(col->type);
            if (!colType.has_value())
                return ResultSet::error(
                    "Unsupported type for column '" +
                    std::string(col->name) + "'");

            bool nullable = true, isPK = false;

            if (col->column_constraints)
            {
                for (const auto c : *col->column_constraints)
                {
                    if (c == hsql::ConstraintType::NotNull)
                        nullable = false;
                    if (c == hsql::ConstraintType::PrimaryKey)
                    {
                        isPK = true;
                        nullable = false;
                    }
                }
            }
            if (isPK)
                pkCol = col->name;
            schema.addColumn(
                columnar::ColumnSchema(col->name, *colType, nullable));
        }

        if (pkCol.empty() && !schema.columns.empty())
            pkCol = schema.columns[0].name;
        schema.primaryKeyColumn = pkCol;

        auto err = registry_.createTable(schema);
        if (!err.ok())
            return ResultSet::error(err.message);

        return ResultSet::affected(0, "Table '" +
                                          std::string(stmt->tableName) +
                                          "' created");
    }

    // ================================================================
    // DDL — DROP TABLE
    // ================================================================
    ResultSet QueryExecutor::executeDrop(const hsql::DropStatement *stmt)
    {
        if (stmt->type != hsql::DropType::kDropTable)
            return ResultSet::error("Only DROP TABLE is supported");

        auto err = registry_.dropTable(stmt->name);
        if (!err.ok())
            return ResultSet::error(err.message);

        return ResultSet::affected(0, "Table '" +
                                          std::string(stmt->name) + "' dropped");
    }

    // ================================================================
    // DML — INSERT
    // Always OLTP — straight to memtable, router not involved
    // ================================================================
    ResultSet QueryExecutor::executeInsert(const hsql::InsertStatement *stmt)
    {
        std::string tableName = stmt->tableName;
        auto schema = registry_.getSchema(tableName);
        if (!schema.has_value())
            return ResultSet::error("Table '" + tableName + "' does not exist");

        if (stmt->type != hsql::InsertType::kInsertValues)
            return ResultSet::error("Only INSERT ... VALUES is supported");
        if (!stmt->values)
            return ResultSet::error("No values provided");

        std::vector<std::string> colNames;
        if (stmt->columns)
            for (const auto *col : *stmt->columns)
                colNames.push_back(col);
        else
            for (const auto &col : schema->columns)
                colNames.push_back(col.name);

        if (colNames.size() != stmt->values->size())
            return ResultSet::error("Column count does not match value count");

        Row row;
        for (size_t i = 0; i < colNames.size(); ++i)
        {
            auto val = exprToString((*stmt->values)[i]);
            if (!val.has_value())
                return ResultSet::error(
                    "Could not evaluate value for column '" + colNames[i] + "'");
            row[colNames[i]] = *val;
        }

        std::string typeErr = RowCodec::typeCheckRow(*schema, row);
        if (!typeErr.empty())
            return ResultSet::error(typeErr);

        std::string valErr = RowCodec::validate(*schema, row);
        if (!valErr.empty())
            return ResultSet::error(valErr);

        auto pkIt = row.find(schema->primaryKeyColumn);
        if (pkIt == row.end() || pkIt->second.empty())
            return ResultSet::error("Primary key '" +
                                    schema->primaryKeyColumn + "' is required");

        std::string key = SchemaRegistry::encodeKey(tableName, pkIt->second);
        std::string blob = RowCodec::encode(*schema, row);
        bool ok = engine_.put(key, blob);

        return ok ? ResultSet::affected(1)
                  : ResultSet::error("Storage engine write failed");
    }

    // ================================================================
    // DML — SELECT
    // AST → QueryRequest → router_->planQuery() → QueryPlan → execute
    // ================================================================
    ResultSet QueryExecutor::executeSelect(const hsql::SelectStatement *stmt)
    {
        if (!stmt->fromTable)
            return ResultSet::error("SELECT requires a FROM clause");

        std::string tableName = stmt->fromTable->name;
        auto schema = registry_.getSchema(tableName);
        if (!schema.has_value())
            return ResultSet::error("Table '" + tableName + "' does not exist");

        // Step 1: SQL AST → QueryRequest
        query::QueryRequest request = buildQueryRequest(stmt, *schema);

        // Step 2: QueryRequest → QueryPlan (router decides everything)
        query::QueryPlan plan = router_->planQuery(request);

        // Step 3: Execute what the plan says
        return executePlan(request, plan, *schema, stmt);
    }

    // ================================================================
    // DML — DELETE (OLTP only — PK based)
    // ================================================================
    ResultSet QueryExecutor::executeDelete(const hsql::DeleteStatement *stmt)
    {
        std::string tableName = stmt->tableName;
        auto schema = registry_.getSchema(tableName);
        if (!schema.has_value())
            return ResultSet::error("Table '" + tableName + "' does not exist");
        if (!stmt->expr)
            return ResultSet::error("DELETE without WHERE is not supported");

        auto pkVal = extractPKLookup(stmt->expr, schema->primaryKeyColumn);
        if (!pkVal.has_value())
            return ResultSet::error(
                "DELETE only supports WHERE <pk> = <value>");

        std::string key = SchemaRegistry::encodeKey(tableName, *pkVal);
        bool ok = engine_.del(key);

        return ok ? ResultSet::affected(1)
                  : ResultSet::affected(0, "Row not found");
    }

    // ================================================================
    // DML — UPDATE (OLTP only — read-modify-write via PK)
    // ================================================================
    ResultSet QueryExecutor::executeUpdate(const hsql::UpdateStatement *stmt)
    {
        std::string tableName = stmt->table->name;
        auto schema = registry_.getSchema(tableName);
        if (!schema.has_value())
            return ResultSet::error("Table '" + tableName + "' does not exist");
        if (!stmt->where)
            return ResultSet::error("UPDATE without WHERE is not supported");

        auto pkVal = extractPKLookup(stmt->where, schema->primaryKeyColumn);
        if (!pkVal.has_value())
            return ResultSet::error(
                "UPDATE only supports WHERE <pk> = <value>");

        std::string key = SchemaRegistry::encodeKey(tableName, *pkVal);
        auto existing = engine_.get(key);
        if (!existing.has_value())
            return ResultSet::affected(0, "Row not found");

        auto decoded = RowCodec::decode(*existing);
        if (!decoded.has_value())
            return ResultSet::error("Failed to decode existing row");

        Row row = *decoded;
        for (const auto *upd : *stmt->updates)
        {
            if (schema->getColumnIndex(upd->column) == -1)
                return ResultSet::error("Unknown column '" +
                                        std::string(upd->column) + "'");
            auto val = exprToString(upd->value);
            if (!val.has_value())
                return ResultSet::error("Could not evaluate value for '" +
                                        std::string(upd->column) + "'");
            row[upd->column] = *val;
        }

        std::string typeErr = RowCodec::typeCheckRow(*schema, row);
        if (!typeErr.empty())
            return ResultSet::error(typeErr);

        std::string blob = RowCodec::encode(*schema, row);
        bool ok = engine_.put(key, blob);

        return ok ? ResultSet::affected(1)
                  : ResultSet::error("Storage engine write failed");
    }

    // ================================================================
    // META — SHOW TABLES
    // ================================================================
    ResultSet QueryExecutor::executeShow(const hsql::ShowStatement *stmt)
    {
        if (stmt->type == hsql::ShowType::kShowTables)
            return ResultFormatter::fromTableList(registry_.listTables());
        return ResultSet::error("Unsupported SHOW command");
    }

    // ================================================================
    // HELPERS
    // ================================================================

    ResultSet QueryExecutor::buildAggregationResult(
        const query::QueryRequest &req,
        const query::QueryResult &qr) const
    {
        std::string fn = aggTypeToString(req.aggType);
        switch (req.aggType)
        {
        case query::AggregationType::COUNT:
            return ResultFormatter::fromAggregation(fn, req.columnName,
                                                    qr.countResult);
        case query::AggregationType::SUM:
            return ResultFormatter::fromAggregation(fn, req.columnName,
                                                    qr.sumResult);
        case query::AggregationType::MIN:
            return ResultFormatter::fromAggregation(fn, req.columnName,
                                                    qr.minResult);
        case query::AggregationType::MAX:
            return ResultFormatter::fromAggregation(fn, req.columnName,
                                                    qr.maxResult);
        case query::AggregationType::AVG:
            return ResultFormatter::fromAggregation(fn, req.columnName,
                                                    qr.avgResult);
        default:
            return ResultSet::error("Unknown aggregation type");
        }
    }

    std::optional<std::string> QueryExecutor::extractPKLookup(
        const hsql::Expr *expr,
        const std::string &pkCol) const
    {
        if (!expr)
            return std::nullopt;

        if (expr->opType == hsql::kOpEquals)
        {
            const hsql::Expr *left = expr->expr;
            const hsql::Expr *right = expr->expr2;
            if (!left || !right)
                return std::nullopt;

            if (left->type == hsql::kExprColumnRef && left->name &&
                std::string(left->name) == pkCol)
                return exprToString(right);

            if (right->type == hsql::kExprColumnRef && right->name &&
                std::string(right->name) == pkCol)
                return exprToString(left);
        }
        return std::nullopt;
    }

    std::optional<std::string> QueryExecutor::exprToString(
        const hsql::Expr *expr) const
    {
        if (!expr)
            return std::nullopt;
        switch (expr->type)
        {
        case hsql::kExprLiteralInt:
            return std::to_string(expr->ival);
        case hsql::kExprLiteralFloat:
            return std::to_string(expr->fval);
        case hsql::kExprLiteralString:
            return expr->name ? std::string(expr->name) : "";
        case hsql::kExprLiteralNull:
            return std::string("");
        default:
            return std::nullopt;
        }
    }

    std::vector<std::pair<std::string, std::string>>
    QueryExecutor::applyWhereFilter(
        const std::vector<std::pair<std::string, std::string>> &rows,
        const columnar::TableSchema &schema,
        const hsql::Expr *where) const
    {
        std::vector<std::pair<std::string, std::string>> out;
        for (const auto &[key, blob] : rows)
        {
            auto row = RowCodec::decode(blob);
            if (!row.has_value())
                continue;
            if (evalWhere(where, *row))
                out.push_back({key, blob});
        }
        return out;
    }

    bool QueryExecutor::evalWhere(const hsql::Expr *expr,
                                  const Row &row) const
    {
        if (!expr)
            return true;
        switch (expr->opType)
        {
        case hsql::kOpEquals:
            return compareOp(expr, row, "=");
        case hsql::kOpNotEquals:
            return compareOp(expr, row, "!=");
        case hsql::kOpLess:
            return compareOp(expr, row, "<");
        case hsql::kOpLessEq:
            return compareOp(expr, row, "<=");
        case hsql::kOpGreater:
            return compareOp(expr, row, ">");
        case hsql::kOpGreaterEq:
            return compareOp(expr, row, ">=");
        case hsql::kOpAnd:
            return evalWhere(expr->expr, row) &&
                   evalWhere(expr->expr2, row);
        case hsql::kOpOr:
            return evalWhere(expr->expr, row) ||
                   evalWhere(expr->expr2, row);
        default:
            return true;
        }
    }

    bool QueryExecutor::compareOp(const hsql::Expr *expr,
                                  const Row &row,
                                  const std::string &op) const
    {
        if (!expr->expr || !expr->expr2)
            return false;

        const hsql::Expr *colExpr =
            (expr->expr->type == hsql::kExprColumnRef)
                ? expr->expr
                : expr->expr2;
        const hsql::Expr *litExpr =
            (expr->expr->type == hsql::kExprColumnRef)
                ? expr->expr2
                : expr->expr;

        if (!colExpr->name)
            return false;
        auto it = row.find(colExpr->name);
        if (it == row.end())
            return false;

        auto litVal = exprToString(litExpr);
        if (!litVal.has_value())
            return false;

        try
        {
            double a = std::stod(it->second);
            double b = std::stod(*litVal);
            if (op == "=")
                return a == b;
            if (op == "!=")
                return a != b;
            if (op == "<")
                return a < b;
            if (op == "<=")
                return a <= b;
            if (op == ">")
                return a > b;
            if (op == ">=")
                return a >= b;
        }
        catch (...)
        {
            if (op == "=")
                return it->second == *litVal;
            if (op == "!=")
                return it->second != *litVal;
            if (op == "<")
                return it->second < *litVal;
            if (op == "<=")
                return it->second <= *litVal;
            if (op == ">")
                return it->second > *litVal;
            if (op == ">=")
                return it->second >= *litVal;
        }
        return false;
    }

    std::vector<std::string> QueryExecutor::getColumnHeaders(
        const columnar::TableSchema &schema) const
    {
        std::vector<std::string> h;
        for (const auto &col : schema.columns)
            h.push_back(col.name);
        return h;
    }

    query::AggregationType QueryExecutor::parseAggType(
        const char *name) const
    {
        if (!name)
            return query::AggregationType::NONE;
        std::string fn = toUpper(name);
        if (fn == "COUNT")
            return query::AggregationType::COUNT;
        if (fn == "SUM")
            return query::AggregationType::SUM;
        if (fn == "AVG")
            return query::AggregationType::AVG;
        if (fn == "MIN")
            return query::AggregationType::MIN;
        if (fn == "MAX")
            return query::AggregationType::MAX;
        return query::AggregationType::NONE;
    }

    std::string QueryExecutor::aggTypeToString(
        query::AggregationType t) const
    {
        switch (t)
        {
        case query::AggregationType::COUNT:
            return "COUNT";
        case query::AggregationType::SUM:
            return "SUM";
        case query::AggregationType::AVG:
            return "AVG";
        case query::AggregationType::MIN:
            return "MIN";
        case query::AggregationType::MAX:
            return "MAX";
        default:
            return "UNKNOWN";
        }
    }

    std::optional<columnar::ColumnType>
    QueryExecutor::hyriseTypeToColumnar(const hsql::ColumnType &t) const
    {
        switch (t.data_type)
        {
        case hsql::DataType::INT:
            return columnar::ColumnType::INT32;
        case hsql::DataType::BIGINT:
            return columnar::ColumnType::INT64;
        case hsql::DataType::FLOAT:
            return columnar::ColumnType::FLOAT;
        case hsql::DataType::DOUBLE:
        case hsql::DataType::DECIMAL:
            return columnar::ColumnType::DOUBLE;
        case hsql::DataType::CHAR:
        case hsql::DataType::VARCHAR:
            return columnar::ColumnType::STRING;
        case hsql::DataType::BOOLEAN:
            return columnar::ColumnType::BOOL;
        default:
            return std::nullopt;
        }
    }

    std::string QueryExecutor::toUpper(const std::string &s)
    {
        std::string out = s;
        for (char &c : out)
            c = static_cast<char>(std::toupper(c));
        return out;
    }

} // namespace sql