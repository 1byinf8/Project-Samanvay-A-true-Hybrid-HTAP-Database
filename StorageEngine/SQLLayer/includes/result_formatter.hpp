// result_formatter.hpp - Formats query results into ASCII tables for the shell
#pragma once
#ifndef RESULT_FORMATTER_HPP
#define RESULT_FORMATTER_HPP

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../../includes/columnar_format.hpp"
#include "row_codec.hpp"

namespace sql
{

    // A formatted result set — headers + rows of string values
    struct ResultSet
    {
        std::vector<std::string> headers;
        std::vector<std::vector<std::string>> rows;
        std::string errorMessage;
        bool success = true;
        uint64_t executionTimeMs = 0;
        size_t rowsAffected = 0; // for INSERT/UPDATE/DELETE

        static ResultSet error(const std::string &msg)
        {
            ResultSet r;
            r.success = false;
            r.errorMessage = msg;
            return r;
        }

        static ResultSet affected(size_t n, const std::string &msg = "")
        {
            ResultSet r;
            r.success = true;
            r.rowsAffected = n;
            r.errorMessage = msg; // reuse as status message
            return r;
        }
    };

    class ResultFormatter
    {
    public:
        // ── PRINT RESULT SET ─────────────────────────────────────────
        static void print(const ResultSet &result)
        {
            if (!result.success)
            {
                std::cout << "ERROR: " << result.errorMessage << "\n";
                return;
            }

            // DML result (INSERT / UPDATE / DELETE / CREATE / DROP)
            if (result.headers.empty())
            {
                if (!result.errorMessage.empty())
                {
                    std::cout << result.errorMessage << "\n";
                }
                else
                {
                    std::cout << "Query OK, "
                              << result.rowsAffected << " row(s) affected";
                    if (result.executionTimeMs > 0)
                        std::cout << " (" << result.executionTimeMs << " ms)";
                    std::cout << "\n";
                }
                return;
            }

            // SELECT result — print ASCII table
            printTable(result.headers, result.rows);

            std::cout << result.rows.size() << " row(s) in set";
            if (result.executionTimeMs > 0)
                std::cout << " (" << result.executionTimeMs << " ms)";
            std::cout << "\n";
        }

        // ── BUILD ResultSet FROM raw scan rows ───────────────────────
        // Used by SELECT with full/range scan
        static ResultSet fromScanRows(
            const columnar::TableSchema &schema,
            const std::vector<std::pair<std::string, std::string>> &rawRows,
            const std::vector<std::string> &selectedColumns)
        {
            ResultSet rs;

            // Determine output columns
            std::vector<int> colIndices;
            if (selectedColumns.empty() ||
                (selectedColumns.size() == 1 && selectedColumns[0] == "*"))
            {
                // SELECT * — all columns
                rs.headers.reserve(schema.columns.size());
                for (size_t i = 0; i < schema.columns.size(); ++i)
                {
                    rs.headers.push_back(schema.columns[i].name);
                    colIndices.push_back(static_cast<int>(i));
                }
            }
            else
            {
                for (const auto &col : selectedColumns)
                {
                    int idx = schema.getColumnIndex(col);
                    if (idx == -1)
                    {
                        return ResultSet::error("Unknown column '" + col + "'");
                    }
                    rs.headers.push_back(col);
                    colIndices.push_back(idx);
                }
            }

            // Decode each row
            for (const auto &[key, blob] : rawRows)
            {
                auto ordered = RowCodec::decodeOrdered(schema, blob);
                if (!ordered.has_value())
                    continue;

                std::vector<std::string> row;
                row.reserve(colIndices.size());
                for (int idx : colIndices)
                {
                    if (idx < static_cast<int>(ordered->size()))
                        row.push_back((*ordered)[idx]);
                    else
                        row.push_back("NULL");
                }
                rs.rows.push_back(std::move(row));
            }

            return rs;
        }

        // ── BUILD ResultSet FROM single row (point lookup) ───────────
        static ResultSet fromSingleRow(
            const columnar::TableSchema &schema,
            const std::string &blob,
            const std::vector<std::string> &selectedColumns)
        {
            std::vector<std::pair<std::string, std::string>> wrapped = {{"", blob}};
            return fromScanRows(schema, wrapped, selectedColumns);
        }

        // ── BUILD ResultSet FROM aggregation result ──────────────────
        static ResultSet fromAggregation(const std::string &funcName,
                                         const std::string &colName,
                                         double value)
        {
            ResultSet rs;
            rs.headers.push_back(funcName + "(" + colName + ")");
            rs.rows.push_back({formatDouble(value)});
            return rs;
        }

        static ResultSet fromAggregation(const std::string &funcName,
                                         const std::string &colName,
                                         int64_t value)
        {
            ResultSet rs;
            rs.headers.push_back(funcName + "(" + colName + ")");
            rs.rows.push_back({std::to_string(value)});
            return rs;
        }

        // ── SHOW TABLES ──────────────────────────────────────────────
        static ResultSet fromTableList(const std::vector<std::string> &tables)
        {
            ResultSet rs;
            rs.headers.push_back("Tables");
            for (const auto &t : tables)
                rs.rows.push_back({t});
            return rs;
        }

    private:
        // ── ASCII TABLE PRINTER ──────────────────────────────────────
        static void printTable(const std::vector<std::string> &headers,
                               const std::vector<std::vector<std::string>> &rows)
        {
            // Compute column widths
            std::vector<size_t> widths(headers.size());
            for (size_t i = 0; i < headers.size(); ++i)
                widths[i] = headers[i].size();

            for (const auto &row : rows)
            {
                for (size_t i = 0; i < row.size() && i < widths.size(); ++i)
                    widths[i] = std::max(widths[i], row[i].size());
            }

            // Separator line: +-------+-------+
            auto printSep = [&]()
            {
                std::cout << "+";
                for (size_t w : widths)
                    std::cout << std::string(w + 2, '-') << "+";
                std::cout << "\n";
            };

            // Row line: | val   | val   |
            auto printRow = [&](const std::vector<std::string> &cells)
            {
                std::cout << "|";
                for (size_t i = 0; i < widths.size(); ++i)
                {
                    std::string cell = (i < cells.size()) ? cells[i] : "";
                    std::cout << " " << cell
                              << std::string(widths[i] - cell.size(), ' ')
                              << " |";
                }
                std::cout << "\n";
            };

            printSep();
            printRow(headers);
            printSep();
            for (const auto &row : rows)
                printRow(row);
            printSep();
        }

        static std::string formatDouble(double v)
        {
            std::ostringstream oss;
            oss << v;
            return oss.str();
        }
    };

} // namespace sql

#endif // RESULT_FORMATTER_HPP