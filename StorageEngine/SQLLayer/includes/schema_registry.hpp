// schema_registry.hpp - Schema Registry for SQL Layer
// Stores table schemas, persists to disk, and provides the key encoding
// convention for the row store. Reuses columnar::TableSchema and
// columnar::ColumnSchema from the existing storage engine.
#pragma once
#ifndef SCHEMA_REGISTRY_HPP
#define SCHEMA_REGISTRY_HPP

#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// Reuse existing types from storage engine
#include "../../includes/columnar_format.hpp"

namespace sql
{

    // ============================================================
    // SchemaRegistry
    // ============================================================
    // Single source of truth for all table definitions.
    // Persists schemas to a plain-text file so they survive restarts.
    //
    // Key encoding convention (row store):
    //   key   = "<tableName>:<primaryKeyValue>"
    //   value = serialized row blob (handled by RowCodec)
    //
    // Example:
    //   table "orders", pk column "order_id", pk value "42"
    //   → key = "orders:42"
    // ============================================================

    class SchemaRegistry
    {
    public:
        // ── Error type returned by mutating operations ──────────────
        struct Error
        {
            std::string message;
            bool ok() const { return message.empty(); }
            static Error success() { return {""}; }
            static Error fail(const std::string &msg) { return {msg}; }
        };

        // ── Constructor ──────────────────────────────────────────────
        // persistPath: file where schemas are persisted across restarts
        explicit SchemaRegistry(const std::string &persistPath = "./schema_registry.sdb")
            : persistPath_(persistPath)
        {
            loadFromDisk();
        }

        // ── CREATE TABLE ─────────────────────────────────────────────
        // Registers a new table. Fails if table already exists.
        Error createTable(const columnar::TableSchema &schema)
        {
            // why is this required ?
            std::lock_guard<std::mutex> lock(mu_);

            if (schema.tableName.empty())
                return Error::fail("Table name cannot be empty");

            if (schema.columns.empty())
                return Error::fail("Table must have at least one column");

            if (schema.primaryKeyColumn.empty())
                return Error::fail("Table must specify a primary key column");

            // Validate that primary key column actually exists in the schema
            if (schema.getColumnIndex(schema.primaryKeyColumn) == -1)
                return Error::fail("Primary key column '" + schema.primaryKeyColumn +
                                   "' not found in column list");

            std::string name = toLower(schema.tableName);

            if (tables_.count(name))
                return Error::fail("Table '" + schema.tableName + "' already exists");

            tables_[name] = schema;
            tables_[name].tableName = name; // normalize to lowercase

            saveToDisk();
            return Error::success();
        }

        // ── DROP TABLE ───────────────────────────────────────────────
        Error dropTable(const std::string &tableName)
        {
            // why is this required ?
            std::lock_guard<std::mutex> lock(mu_);

            std::string name = toLower(tableName);
            if (!tables_.count(name))
                return Error::fail("Table '" + tableName + "' does not exist");

            tables_.erase(name);
            saveToDisk();
            return Error::success();
        }

        // ── GET SCHEMA ───────────────────────────────────────────────
        // Returns nullopt if table does not exist
        std::optional<columnar::TableSchema> getSchema(const std::string &tableName) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::string name = toLower(tableName);
            auto it = tables_.find(name);
            if (it == tables_.end())
                return std::nullopt;
            return it->second;
        }

        // ── TABLE EXISTS ─────────────────────────────────────────────
        bool tableExists(const std::string &tableName) const
        {
            std::lock_guard<std::mutex> lock(mu_);
            return tables_.count(toLower(tableName)) > 0;
        }

        // ── LIST TABLES ──────────────────────────────────────────────
        std::vector<std::string> listTables() const
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::vector<std::string> names;
            names.reserve(tables_.size());
            for (const auto &[name, _] : tables_)
                names.push_back(name);
            return names;
        }

        // ── KEY ENCODING ─────────────────────────────────────────────
        // Builds the storage key for a row given table name and pk value.
        // Format: "<tableName>:<pkValue>"
        static std::string encodeKey(const std::string &tableName,
                                     const std::string &pkValue)
        {
            return toLower(tableName) + ":" + pkValue;
        }

        // Builds the key prefix for scanning all rows of a table.
        // Format: "<tableName>:"
        // Use with rangeQuery(prefix, prefix + "\xff\xff\xff\xff")
        static std::string tablePrefix(const std::string &tableName)
        {
            return toLower(tableName) + ":";
        }

        // Extracts pk value from an encoded key (strips "tableName:" prefix)
        static std::string decodePkFromKey(const std::string &encodedKey,
                                           const std::string &tableName)
        {
            std::string prefix = toLower(tableName) + ":";
            if (encodedKey.substr(0, prefix.size()) == prefix)
                return encodedKey.substr(prefix.size());
            return encodedKey; // fallback: return as-is
        }

        // ── DESCRIBE TABLE (pretty print) ────────────────────────────
        std::string describeTable(const std::string &tableName) const
        {
            auto schema = getSchema(tableName);
            if (!schema.has_value())
                return "ERROR: Table '" + tableName + "' does not exist\n";

            std::ostringstream oss;
            oss << "Table: " << schema->tableName << "\n";
            oss << "Primary Key: " << schema->primaryKeyColumn << "\n";
            oss << std::string(52, '-') << "\n";
            oss << padRight("Column", 20)
                << padRight("Type", 12)
                << padRight("Nullable", 10)
                << "Compression\n";
            oss << std::string(52, '-') << "\n";

            for (const auto &col : schema->columns)
            {
                oss << padRight(col.name, 20)
                    << padRight(columnTypeToString(col.type), 12)
                    << padRight(col.nullable ? "YES" : "NO", 10)
                    << compressionToString(col.preferredCompression) << "\n";
            }
            return oss.str();
        }

        // ── COLUMN TYPE HELPERS ──────────────────────────────────────
        static std::string columnTypeToString(columnar::ColumnType t)
        {
            switch (t)
            {
            case columnar::ColumnType::INT64:
                return "INT64";
            case columnar::ColumnType::INT32:
                return "INT32";
            case columnar::ColumnType::DOUBLE:
                return "DOUBLE";
            case columnar::ColumnType::FLOAT:
                return "FLOAT";
            case columnar::ColumnType::STRING:
                return "STRING";
            case columnar::ColumnType::BOOL:
                return "BOOL";
            case columnar::ColumnType::BYTES:
                return "BYTES";
            case columnar::ColumnType::TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "UNKNOWN";
            }
        }

        // Parse type string from SQL (e.g. from CREATE TABLE statement)
        static std::optional<columnar::ColumnType> parseColumnType(const std::string &s)
        {
            std::string u = toUpper(s);
            if (u == "INT64" || u == "BIGINT")
                return columnar::ColumnType::INT64;
            if (u == "INT32" || u == "INT")
                return columnar::ColumnType::INT32;
            if (u == "DOUBLE" || u == "DECIMAL")
                return columnar::ColumnType::DOUBLE;
            if (u == "FLOAT" || u == "REAL")
                return columnar::ColumnType::FLOAT;
            if (u == "STRING" || u == "VARCHAR" || u == "TEXT" || u == "CHAR")
                return columnar::ColumnType::STRING;
            if (u == "BOOL" || u == "BOOLEAN")
                return columnar::ColumnType::BOOL;
            if (u == "BYTES" || u == "BLOB" || u == "BINARY")
                return columnar::ColumnType::BYTES;
            if (u == "TIMESTAMP")
                return columnar::ColumnType::TIMESTAMP;
            return std::nullopt;
        }

        // these are the private members and methods of the SchemaRegistry class, which are not accessible from outside the class. They include a mutex for thread safety, an unordered map to store the table schemas, and the file path for persistence.
        // The private methods handle saving and loading schemas to and from disk, as well as some string utilities for formatting and parsing.
    private:
        mutable std::mutex mu_;
        std::unordered_map<std::string, columnar::TableSchema> tables_;
        std::string persistPath_;

        // ── PERSISTENCE ──────────────────────────────────────────────
        // Simple text-based serialization format:
        //
        // TABLE <tableName> <primaryKeyColumn>
        // COLUMN <name> <type> <nullable(0|1)> <compression>
        // COLUMN ...
        // END
        //
        // One TABLE block per table, blocks separated by blank lines.

        void saveToDisk() const
        {
            std::ofstream f(persistPath_, std::ios::trunc);
            if (!f.is_open())
            {
                std::cerr << "[SchemaRegistry] WARNING: could not save to "
                          << persistPath_ << "\n";
                return;
            }

            // Iterate over all tables and write their schemas in the defined format
            for (const auto &[name, schema] : tables_)
            {
                // For each table, write a TABLE line with the table name and primary key column, followed by a COLUMN line for each column in the schema, and an END line to mark the end of the table block.
                f << "TABLE " << schema.tableName
                  << " " << schema.primaryKeyColumn << "\n";

                for (const auto &col : schema.columns)
                {
                    f << "COLUMN "
                      << col.name << " "
                      << static_cast<int>(col.type) << " "
                      << (col.nullable ? 1 : 0) << " "
                      << static_cast<int>(col.preferredCompression) << "\n";
                }
                f << "END\n\n";
            }
        }

        void loadFromDisk()
        {
            std::ifstream f(persistPath_);
            if (!f.is_open())
                return; // fresh start, no file yet

            // variable to hold the current line, current table schema being parsed, and whether we're inside a table block
            std::string line;
            columnar::TableSchema current;
            bool inTable = false;

            // read the file line by line, parsing the schema definitions according to the defined format
            while (std::getline(f, line))
            {

                // skip empty lines
                if (line.empty())
                    continue;

                // use a stringstream to parse the line into tokens
                std::istringstream iss(line);
                // the first token determines the type of line (TABLE, COLUMN, END)
                std::string token;
                // this line pushes the first token into the variable 'token' for processing from the stringstream
                iss >> token;

                if (token == "TABLE")
                {
                    // start a new table schema block, resetting the current schema and reading the table name and primary key column
                    current = columnar::TableSchema();
                    // the next two tokens are the table name and primary key column, which are read into the current schema object
                    iss >> current.tableName >> current.primaryKeyColumn;
                    inTable = true;
                }
                else if (token == "COLUMN" && inTable)
                {
                    // for column lines, read the column name, type, nullable flag, and compression method, and add the column to the current table schema
                    std::string name;
                    int type, nullable, compression;
                    // the next four tokens are the column name, type (as an int), nullable flag (0 or 1), and compression method (as an int), which are read into local variables
                    iss >> name >> type >> nullable >> compression;

                    columnar::ColumnSchema col;
                    col.name = name;
                    col.type = static_cast<columnar::ColumnType>(type);
                    col.nullable = (nullable == 1);
                    // whats happening here is that the compression method is being read as an int from the file, and then cast back to the Compression enum type before being stored in the column schema
                    col.preferredCompression =
                        static_cast<columnar::Compression>(compression);
                    current.addColumn(col);
                }
                else if (token == "END" && inTable)
                {
                    tables_[current.tableName] = current;
                    inTable = false;
                }
            }
        }

        // ── STRING UTILITIES ─────────────────────────────────────────
        static std::string toLower(const std::string &s)
        {
            std::string out = s;
            for (char &c : out)
                c = static_cast<char>(std::tolower(c));
            return out;
        }

        static std::string toUpper(const std::string &s)
        {
            std::string out = s;
            for (char &c : out)
                c = static_cast<char>(std::toupper(c));
            return out;
        }

        static std::string padRight(const std::string &s, size_t width)
        {
            if (s.size() >= width)
                return s;
            return s + std::string(width - s.size(), ' ');
        }

        static std::string compressionToString(columnar::Compression c)
        {
            switch (c)
            {
            case columnar::Compression::NONE:
                return "NONE";
            case columnar::Compression::RLE:
                return "RLE";
            case columnar::Compression::DICTIONARY:
                return "DICTIONARY";
            case columnar::Compression::DELTA:
                return "DELTA";
            case columnar::Compression::BITPACKING:
                return "BITPACKING";
            default:
                return "NONE";
            }
        }
    };

} // namespace sql

#endif // SCHEMA_REGISTRY_HPP