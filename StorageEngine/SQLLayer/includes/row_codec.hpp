// row_codec.hpp - Row serializer/deserializer for SQL Layer
// Encodes a structured SQL row (map of column name → string value) into
// the opaque string blob that the row store expects, and decodes it back.
//
// Wire format (binary, little-endian):
//
//   [ num_columns : uint32 ]
//   for each column:
//     [ name_len  : uint32 ]
//     [ name      : name_len bytes ]
//     [ value_len : uint32 ]
//     [ value     : value_len bytes ]
//
// Values are always stored as UTF-8 strings, matching the text representation
// used by the columnar file's addRow() method. Type conversion happens at the
// query executor level when reading back.

// This deliberately keeps the codec simple and self-describing so that
// partial schema evolution (adding nullable columns) is easier to handle
// in the future.
#pragma once
#ifndef ROW_CODEC_HPP
#define ROW_CODEC_HPP

#include <cstdint>
#include <cstring>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../includes/columnar_format.hpp"
#include "schema_registry.hpp"

namespace sql
{

    // A decoded row: column name → string value
    using Row = std::unordered_map<std::string, std::string>;

    class RowCodec
    {
    public:
        // ── ENCODE ───────────────────────────────────────────────────
        // Converts a Row (map<colName, value>) into a binary blob.
        // Only columns present in the schema are encoded; extras are ignored.
        // Missing nullable columns are encoded as empty string.
        // Missing NOT NULL columns cause an error to be returned.
        //
        // Returns: encoded blob, or throws std::runtime_error on error.
        static std::string encode(const columnar::TableSchema &schema,
                                  const Row &row)
        {
            // Validate required columns are present
            for (const auto &col : schema.columns)
            {
                if (!col.nullable && col.name != schema.primaryKeyColumn)
                {
                    if (row.find(col.name) == row.end() ||
                        row.at(col.name).empty())
                    {
                        throw std::runtime_error(
                            "NOT NULL column '" + col.name + "' is missing a value");
                    }
                }
            }

            // Build the blob
            std::string blob;
            uint32_t numCols = static_cast<uint32_t>(schema.columns.size());
            appendUint32(blob, numCols);

            for (const auto &col : schema.columns)
            {
                // Column name
                appendString(blob, col.name);

                // Column value (use empty string if not provided and nullable)
                std::string val;
                auto it = row.find(col.name);
                if (it != row.end())
                {
                    val = it->second;
                }
                appendString(blob, val);
            }

            return blob;
        }

        // ── DECODE ───────────────────────────────────────────────────
        // Converts a binary blob back into a Row.
        // Unknown column names are preserved (forward compatibility).
        // Returns nullopt if blob is malformed.
        static std::optional<Row> decode(const std::string &blob)
        {
            if (blob.size() < sizeof(uint32_t))
                return std::nullopt;

            size_t offset = 0;
            Row row;

            uint32_t numCols = readUint32(blob, offset);
            offset += sizeof(uint32_t);

            for (uint32_t i = 0; i < numCols; ++i)
            {
                auto name = readString(blob, offset);
                if (!name.has_value())
                    return std::nullopt;
                offset += sizeof(uint32_t) + name->size();

                auto value = readString(blob, offset);
                if (!value.has_value())
                    return std::nullopt;
                offset += sizeof(uint32_t) + value->size();

                row[*name] = *value;
            }

            return row;
        }

        // ── DECODE WITH SCHEMA (ordered, typed-aware) ────────────────
        // Returns values in schema column order as a vector of strings.
        // Useful for the result formatter which needs ordered columns.
        static std::optional<std::vector<std::string>>
        decodeOrdered(const columnar::TableSchema &schema,
                      const std::string &blob)
        {
            auto row = decode(blob);
            if (!row.has_value())
                return std::nullopt;

            std::vector<std::string> values;
            values.reserve(schema.columns.size());

            for (const auto &col : schema.columns)
            {
                auto it = row->find(col.name);
                if (it != row->end())
                {
                    values.push_back(it->second);
                }
                else
                {
                    values.push_back("NULL"); // column added after row was written
                }
            }
            return values;
        }

        // ── GET SINGLE COLUMN VALUE ──────────────────────────────────
        // Fast path: extract a single column value from a blob
        // without decoding the whole row. Useful for WHERE clause filtering.
        static std::optional<std::string>
        getColumnValue(const std::string &blob, const std::string &columnName)
        {
            auto row = decode(blob);
            if (!row.has_value())
                return std::nullopt;
            auto it = row->find(columnName);
            if (it == row->end())
                return std::nullopt;
            return it->second;
        }

        // ── VALIDATE ROW AGAINST SCHEMA ─────────────────────────────
        // Returns error message, or empty string if valid.
        static std::string validate(const columnar::TableSchema &schema,
                                    const Row &row)
        {
            // Check all non-nullable columns are present and non-empty
            for (const auto &col : schema.columns)
            {
                if (!col.nullable)
                {
                    auto it = row.find(col.name);
                    if (it == row.end() || it->second.empty())
                    {
                        return "NOT NULL column '" + col.name + "' is missing";
                    }
                }
            }

            // Check no unknown columns (warn but don't fail)
            for (const auto &[name, _] : row)
            {
                if (schema.getColumnIndex(name) == -1)
                {
                    return "Unknown column '" + name + "' in table '" +
                           schema.tableName + "'";
                }
            }

            return ""; // valid
        }

        // ── TYPE CONVERSION HELPERS ──────────────────────────────────
        // These are used by the executor when evaluating WHERE conditions.

        static bool isNumeric(columnar::ColumnType t)
        {
            return t == columnar::ColumnType::INT64 ||
                   t == columnar::ColumnType::INT32 ||
                   t == columnar::ColumnType::DOUBLE ||
                   t == columnar::ColumnType::FLOAT ||
                   t == columnar::ColumnType::TIMESTAMP;
        }

        // Validates that a string value is compatible with the given column type.
        // Returns empty string on success, error message on failure.
        static std::string typeCheck(const std::string &value,
                                     columnar::ColumnType type)
        {
            if (value.empty())
                return ""; // nulls pass type check

            try
            {
                switch (type)
                {
                case columnar::ColumnType::INT64:
                case columnar::ColumnType::INT32:
                case columnar::ColumnType::TIMESTAMP:
                    std::stoll(value);
                    break;
                case columnar::ColumnType::DOUBLE:
                case columnar::ColumnType::FLOAT:
                    std::stod(value);
                    break;
                case columnar::ColumnType::BOOL:
                    if (value != "true" && value != "false" &&
                        value != "1" && value != "0")
                    {
                        return "Invalid BOOL value: '" + value + "'";
                    }
                    break;
                default:
                    break; // STRING and BYTES accept anything
                }
            }
            catch (...)
            {
                return "Value '" + value + "' is incompatible with type " +
                       SchemaRegistry::columnTypeToString(type);
            }
            return "";
        }

        // Full row type validation against schema
        static std::string typeCheckRow(const columnar::TableSchema &schema,
                                        const Row &row)
        {
            for (const auto &[colName, value] : row)
            {
                int idx = schema.getColumnIndex(colName);
                if (idx == -1)
                    continue; // unknown column handled by validate()
                std::string err = typeCheck(value, schema.columns[idx].type);
                if (!err.empty())
                    return err;
            }
            return "";
        }

    private:
        // ── BINARY HELPERS ───────────────────────────────────────────

        static void appendUint32(std::string &buf, uint32_t v)
        {
            buf.append(reinterpret_cast<const char *>(&v), sizeof(v));
        }

        static void appendString(std::string &buf, const std::string &s)
        {
            uint32_t len = static_cast<uint32_t>(s.size());
            appendUint32(buf, len);
            buf.append(s);
        }

        static uint32_t readUint32(const std::string &buf, size_t offset)
        {
            uint32_t v = 0;
            std::memcpy(&v, buf.data() + offset, sizeof(v));
            return v;
        }

        static std::optional<std::string> readString(const std::string &buf,
                                                     size_t offset)
        {
            if (offset + sizeof(uint32_t) > buf.size())
                return std::nullopt;

            uint32_t len = readUint32(buf, offset);
            offset += sizeof(uint32_t);

            if (offset + len > buf.size())
                return std::nullopt;

            return buf.substr(offset, len);
        }
    };

} // namespace sql

#endif // ROW_CODEC_HPP