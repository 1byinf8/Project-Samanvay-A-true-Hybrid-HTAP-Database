// sql_shell.cpp - Interactive SQL Shell (REPL) for Project Samanvay
// Compile from StorageEngine/SQLLayer/:
//   g++ -std=c++17 -I../includes -I../third_party/sql-parser/src \
//       -o sql_shell sql_shell.cpp \
//       -L../build -lsqlparser -pthread
#include <iostream>
#include <sstream>
#include <string>

#include "../includes/storage_engine.hpp"
#include "includes/query_executor.hpp"
#include "includes/result_formatter.hpp"
#include "includes/schema_registry.hpp"

// ── ANSI colours (disabled on non-POSIX) ────────────────────
#ifdef _WIN32
#define CLR_RESET ""
#define CLR_BOLD ""
#define CLR_GREEN ""
#define CLR_RED ""
#define CLR_CYAN ""
#else
#define CLR_RESET "\033[0m"
#define CLR_BOLD "\033[1m"
#define CLR_GREEN "\033[32m"
#define CLR_RED "\033[31m"
#define CLR_CYAN "\033[36m"
#endif

static void printBanner()
{
    std::cout << CLR_BOLD CLR_CYAN
              << R"(
  ____                                               
 / ___|  __ _ _ __ ___   __ _ _ ____   ____ _ _   _ 
 \___ \ / _` | '_ ` _ \ / _` | '_ \ \ / / _` | | | |
  ___) | (_| | | | | | | (_| | | | \ V / (_| | |_| |
 |____/ \__,_|_| |_| |_|\__,_|_| |_|\_/ \__,_|\__, |
                                                |___/ 
)"
              << CLR_RESET
              << "  Project Samanvay — HTAP SQL Shell\n"
              << "  Type SQL statements ending with ';'\n"
              << "  Special commands: \\q (quit), \\s (engine status), "
                 "\\d <table> (describe)\n"
              << "  SHOW TABLES;  to list all tables\n\n";
}

// ── Handle special backslash commands ───────────────────────
static bool handleMetaCommand(const std::string &input,
                              storage::StorageEngine &engine,
                              sql::SchemaRegistry &registry)
{
    if (input == "\\q" || input == "quit" || input == "exit")
    {
        std::cout << "Bye!\n";
        exit(0);
    }

    if (input == "\\s")
    {
        engine.printStatus();
        return true;
    }

    if (input.size() > 3 && input.substr(0, 3) == "\\d ")
    {
        std::string tableName = input.substr(3);
        std::cout << registry.describeTable(tableName);
        return true;
    }

    if (input == "\\help" || input == "\\h")
    {
        std::cout << "Commands:\n"
                  << "  \\q          — quit\n"
                  << "  \\s          — storage engine status\n"
                  << "  \\d <table>  — describe table\n"
                  << "  SHOW TABLES; — list tables\n"
                  << "  Any SQL statement ending with ';'\n";
        return true;
    }

    return false; // not a meta command
}

int main()
{
    // ── Init storage engine & SQL layer ─────────────────────
    storage::StorageEngineConfig config;
    config.dataDirectory = "./data";
    config.columnarDirectory = "./data/columnar";
    config.walPath = "./data/wal.log";

    storage::StorageEngine engine(config);
    engine.recoverFromWAL();

    sql::SchemaRegistry registry("./data/schema_registry.sdb");
    sql::QueryExecutor executor(engine, registry);

    printBanner();

    // ── REPL loop ────────────────────────────────────────────
    std::string line;
    std::string buffer; // accumulates multi-line SQL

    while (true)
    {
        // Prompt: "samanvay> " for first line, "       -> " for continuation
        if (buffer.empty())
            std::cout << CLR_BOLD CLR_GREEN << "samanvay> " << CLR_RESET;
        else
            std::cout << CLR_BOLD << "       -> " << CLR_RESET;

        if (!std::getline(std::cin, line))
        {
            // EOF (Ctrl+D)
            std::cout << "\nBye!\n";
            break;
        }

        // Trim leading/trailing whitespace
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos)
            continue; // blank line
        line = line.substr(start);
        size_t end = line.find_last_not_of(" \t\r\n");
        if (end != std::string::npos)
            line = line.substr(0, end + 1);

        if (line.empty())
            continue;

        // Handle meta commands (no semicolon needed)
        if (line[0] == '\\' || line == "quit" || line == "exit")
        {
            handleMetaCommand(line, engine, registry);
            continue;
        }

        // Accumulate into buffer
        if (!buffer.empty())
            buffer += " ";
        buffer += line;

        // Execute when we see a semicolon at the end
        if (buffer.back() == ';')
        {
            // Strip the trailing semicolon before sending to parser
            std::string sql = buffer.substr(0, buffer.size() - 1);
            buffer.clear();

            if (sql.empty())
                continue;

            // Execute and print
            auto result = executor.execute(sql);
            sql::ResultFormatter::print(result);
            std::cout << "\n";
        }
        // Otherwise keep accumulating for multi-line SQL
    }

    return 0;
}