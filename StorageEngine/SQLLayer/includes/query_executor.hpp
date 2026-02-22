#pragma once
#ifndef QUERY_EXECUTOR_HPP
#define QUERY_EXECUTOR_HPP

#include <chrono>
#include <optional>
#include <string>
#include <vector>

// Hyrise parser
#include "SQLParser.h"
#include "sql/CreateStatement.h"
#include "sql/DeleteStatement.h"
#include "sql/DropStatement.h"
#include "sql/InsertStatement.h"
#include "sql/SelectStatement.h"
#include "sql/UpdateStatement.h"

// Storage engine
#include "../../includes/hybrid_query_router.hpp"
#include "../../includes/storage_engine.hpp"

// SQL layer
#include "result_formatter.hpp"
#include "row_codec.hpp"
#include "schema_registry.hpp"

namespace sql
{

    class QueryExecutor
    {
    public:
        QueryExecutor(storage::StorageEngine &engine, SchemaRegistry &registry);

        // Execute a SQL string, returns a ResultSet
        ResultSet execute(const std::string &sql);

        // Explain the query plan without executing
        std::string explain(const std::string &sql);

    private:
        storage::StorageEngine &engine_;
        SchemaRegistry &registry_;
        query::HybridQueryRouter *router_; // owned by engine

        // ── Dispatcher ───────────────────────────────────────────────
        ResultSet dispatch(const hsql::SQLStatement *stmt);

        // ── Query Planning ───────────────────────────────────────────
        query::QueryRequest buildQueryRequest(
            const hsql::SelectStatement *stmt,
            const columnar::TableSchema &schema) const;

        ResultSet executePlan(
            const query::QueryRequest &request,
            const query::QueryPlan &plan,
            const columnar::TableSchema &schema,
            const hsql::SelectStatement *stmt) const;

        // ── DDL ──────────────────────────────────────────────────────
        ResultSet executeCreate(const hsql::CreateStatement *stmt);
        ResultSet executeDrop(const hsql::DropStatement *stmt);

        // ── DML ──────────────────────────────────────────────────────
        ResultSet executeInsert(const hsql::InsertStatement *stmt);
        ResultSet executeSelect(const hsql::SelectStatement *stmt);
        ResultSet executeDelete(const hsql::DeleteStatement *stmt);
        ResultSet executeUpdate(const hsql::UpdateStatement *stmt);

        // ── Meta ─────────────────────────────────────────────────────
        ResultSet executeShow(const hsql::ShowStatement *stmt);

        // ── Helpers ──────────────────────────────────────────────────
        ResultSet buildAggregationResult(
            const query::QueryRequest &req,
            const query::QueryResult &qr) const;

        std::optional<std::string> extractPKLookup(
            const hsql::Expr *expr,
            const std::string &pkCol) const;

        std::optional<std::string> exprToString(
            const hsql::Expr *expr) const;

        std::vector<std::pair<std::string, std::string>> applyWhereFilter(
            const std::vector<std::pair<std::string, std::string>> &rows,
            const columnar::TableSchema &schema,
            const hsql::Expr *where) const;

        bool evalWhere(const hsql::Expr *expr, const Row &row) const;

        bool compareOp(const hsql::Expr *expr,
                       const Row &row,
                       const std::string &op) const;

        std::vector<std::string> getColumnHeaders(
            const columnar::TableSchema &schema) const;

        query::AggregationType parseAggType(const char *name) const;
        std::string aggTypeToString(query::AggregationType t) const;

        std::optional<columnar::ColumnType> hyriseTypeToColumnar(
            const hsql::ColumnType &t) const;

        static std::string toUpper(const std::string &s);
    };

} // namespace sql

#endif // QUERY_EXECUTOR_HPP