// api_server.hpp — REST API Server for Project Samanvay
// Uses cpp-httplib for HTTP and nlohmann/json for serialization
#pragma once
#ifndef API_SERVER_HPP
#define API_SERVER_HPP

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

#include "httplib.h"
#include "nlohmann/json.hpp"

#include "../SQLLayer/includes/query_executor.hpp"
#include "../SQLLayer/includes/result_formatter.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"
#include "../includes/storage_engine.hpp"
#include "json_helpers.hpp"

namespace api {

class SamanvayAPIServer {
public:
  SamanvayAPIServer(storage::StorageEngine &engine,
                    sql::SchemaRegistry &registry, sql::QueryExecutor &executor,
                    int port = 8080)
      : engine_(engine), registry_(registry), executor_(executor), port_(port),
        startTime_(std::chrono::steady_clock::now()) {
    setupRoutes();
  }

  // Start the HTTP server (blocking call)
  bool start() {
    // Bind first to catch port-in-use errors
    if (!server_.bind_to_port("0.0.0.0", port_)) {
      std::cerr << "ERROR: Failed to bind to port " << port_
                << ". Is it already in use?\n";
      return false;
    }

    std::cout << "\n"
              << "╔══════════════════════════════════════════════════╗\n"
              << "║       Project Samanvay — REST API Server         ║\n"
              << "╠══════════════════════════════════════════════════╣\n"
              << "║  Endpoints:                                      ║\n"
              << "║    POST /api/query       — Execute SQL           ║\n"
              << "║    POST /api/explain     — Explain query plan    ║\n"
              << "║    GET  /api/tables      — List all tables       ║\n"
              << "║    GET  /api/tables/:n   — Get table schema      ║\n"
              << "║    GET  /api/status      — Engine stats          ║\n"
              << "║    POST /api/admin/flush — Flush memtables       ║\n"
              << "║    POST /api/admin/compact — Trigger compaction  ║\n"
              << "║    GET  /api/health      — Health check          ║\n"
              << "╚══════════════════════════════════════════════════╝\n"
              << "  Listening on http://localhost:" << port_ << "\n"
              << "  Press Ctrl+C to stop\n\n";

    // Start listening (this blocks until server_.stop() is called)
    return server_.listen_after_bind();
  }

  void stop() { server_.stop(); }

private:
  storage::StorageEngine &engine_;
  sql::SchemaRegistry &registry_;
  sql::QueryExecutor &executor_;
  int port_;
  httplib::Server server_;
  std::chrono::steady_clock::time_point startTime_;

  // ─── Helper: set JSON content type ───────────────────────────────
  void jsonResponse(httplib::Response &res, const json &body,
                    int status = 200) {
    res.set_content(body.dump(2), "application/json");
    res.status = status;
  }

  // ─── Route Setup ─────────────────────────────────────────────────
  void setupRoutes() {
    // ── CORS Middleware ──────────────────────────────────────────
    server_.set_pre_routing_handler(
        [](const httplib::Request &, httplib::Response &res) {
          res.set_header("Access-Control-Allow-Origin", "*");
          res.set_header("Access-Control-Allow-Methods",
                         "GET, POST, PUT, DELETE, OPTIONS");
          res.set_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization");
          res.set_header("Access-Control-Max-Age", "86400");
          return httplib::Server::HandlerResponse::Unhandled;
        });

    // Handle OPTIONS preflight requests (CORS headers applied by pre-routing)
    server_.Options("/(.*)", [](const httplib::Request &,
                                httplib::Response &res) { res.status = 204; });

    // ═════════════════════════════════════════════════════════════
    // CORE SQL EXECUTION
    // ═════════════════════════════════════════════════════════════

    // POST /api/query — Execute any SQL statement
    server_.Post("/api/query", [this](const httplib::Request &req,
                                      httplib::Response &res) {
      try {
        auto body = json::parse(req.body);
        if (!body.contains("sql") || !body["sql"].is_string()) {
          jsonResponse(res,
                       errorResponse("Missing 'sql' field in request body",
                                     "INVALID_REQUEST"),
                       400);
          return;
        }

        std::string sql = body["sql"].get<std::string>();
        if (sql.empty()) {
          jsonResponse(
              res,
              errorResponse("SQL statement cannot be empty", "INVALID_REQUEST"),
              400);
          return;
        }

        // Strip trailing semicolon (shell-style input)
        if (sql.back() == ';') {
          sql = sql.substr(0, sql.size() - 1);
        }

        auto result = executor_.execute(sql);
        auto responseJson = resultSetToJson(result);

        int statusCode = result.success ? 200 : 400;
        jsonResponse(res, responseJson, statusCode);

      } catch (const json::parse_error &e) {
        jsonResponse(res,
                     errorResponse("Invalid JSON: " + std::string(e.what()),
                                   "PARSE_ERROR"),
                     400);
      } catch (const std::exception &e) {
        jsonResponse(res,
                     errorResponse("Internal error: " + std::string(e.what()),
                                   "INTERNAL_ERROR"),
                     500);
      }
    });

    // POST /api/explain — Explain a SELECT query plan
    server_.Post("/api/explain", [this](const httplib::Request &req,
                                        httplib::Response &res) {
      try {
        auto body = json::parse(req.body);
        if (!body.contains("sql") || !body["sql"].is_string()) {
          jsonResponse(res,
                       errorResponse("Missing 'sql' field", "INVALID_REQUEST"),
                       400);
          return;
        }

        std::string sql = body["sql"].get<std::string>();
        if (sql.back() == ';') {
          sql = sql.substr(0, sql.size() - 1);
        }

        std::string plan = executor_.explain(sql);
        jsonResponse(res, successResponse({{"plan", plan}}));

      } catch (const json::parse_error &e) {
        jsonResponse(res,
                     errorResponse("Invalid JSON: " + std::string(e.what()),
                                   "PARSE_ERROR"),
                     400);
      } catch (const std::exception &e) {
        jsonResponse(res,
                     errorResponse("Internal error: " + std::string(e.what()),
                                   "INTERNAL_ERROR"),
                     500);
      }
    });

    // ═════════════════════════════════════════════════════════════
    // SCHEMA / METADATA
    // ═════════════════════════════════════════════════════════════

    // GET /api/tables — List all table names
    server_.Get("/api/tables", [this](const httplib::Request &,
                                      httplib::Response &res) {
      auto tables = registry_.listTables();
      jsonResponse(
          res, successResponse({{"tables", tables}, {"count", tables.size()}}));
    });

    // GET /api/tables/:name — Get full schema for a table
    server_.Get("/api/tables/:name", [this](const httplib::Request &req,
                                            httplib::Response &res) {
      std::string name = req.path_params.at("name");
      auto schema = registry_.getSchema(name);

      if (!schema.has_value()) {
        jsonResponse(
            res,
            errorResponse("Table '" + name + "' does not exist", "NOT_FOUND"),
            404);
        return;
      }

      jsonResponse(res, successResponse(schemaToJson(*schema)));
    });

    // GET /api/tables/:name/describe — Get text description
    server_.Get(
        "/api/tables/:name/describe",
        [this](const httplib::Request &req, httplib::Response &res) {
          std::string name = req.path_params.at("name");

          if (!registry_.tableExists(name)) {
            jsonResponse(res,
                         errorResponse("Table '" + name + "' does not exist",
                                       "NOT_FOUND"),
                         404);
            return;
          }

          std::string description = registry_.describeTable(name);
          jsonResponse(res, successResponse({{"description", description}}));
        });

    // ═════════════════════════════════════════════════════════════
    // ENGINE STATUS / ADMIN
    // ═════════════════════════════════════════════════════════════

    // GET /api/status — Full engine statistics
    server_.Get("/api/status",
                [this](const httplib::Request &, httplib::Response &res) {
                  auto stats = engine_.getStats();
                  jsonResponse(res, successResponse(statsToJson(stats)));
                });

    // POST /api/admin/flush — Force flush memtables to disk
    server_.Post("/api/admin/flush", [this](const httplib::Request &,
                                            httplib::Response &res) {
      try {
        engine_.forceFlush();
        jsonResponse(
            res, successResponse({{"message", "Memtables flushed to disk"}}));
      } catch (const std::exception &e) {
        jsonResponse(res,
                     errorResponse("Flush failed: " + std::string(e.what()),
                                   "INTERNAL_ERROR"),
                     500);
      }
    });

    // POST /api/admin/compact — Trigger compaction
    server_.Post("/api/admin/compact", [this](const httplib::Request &,
                                              httplib::Response &res) {
      try {
        engine_.triggerCompaction();
        jsonResponse(res,
                     successResponse({{"message", "Compaction triggered"}}));
      } catch (const std::exception &e) {
        jsonResponse(
            res,
            errorResponse("Compaction failed: " + std::string(e.what()),
                          "INTERNAL_ERROR"),
            500);
      }
    });

    // POST /api/admin/compact/pause — Pause compaction
    server_.Post("/api/admin/compact/pause", [this](const httplib::Request &,
                                                    httplib::Response &res) {
      engine_.pauseCompaction();
      jsonResponse(res, successResponse({{"message", "Compaction paused"}}));
    });

    // POST /api/admin/compact/resume — Resume compaction
    server_.Post("/api/admin/compact/resume", [this](const httplib::Request &,
                                                     httplib::Response &res) {
      engine_.resumeCompaction();
      jsonResponse(res, successResponse({{"message", "Compaction resumed"}}));
    });

    // ═════════════════════════════════════════════════════════════
    // HEALTH / INFO
    // ═════════════════════════════════════════════════════════════

    // GET /api/health — Simple health check
    server_.Get("/api/health", [this](const httplib::Request &,
                                      httplib::Response &res) {
      auto now = std::chrono::steady_clock::now();
      auto uptime =
          std::chrono::duration_cast<std::chrono::seconds>(now - startTime_)
              .count();

      jsonResponse(res, successResponse({{"status", "ok"},
                                         {"uptime_seconds", uptime},
                                         {"version", "0.1.0"},
                                         {"engine", "Samanvay HTAP"}}));
    });

    // GET /api/info — Capabilities discovery
    server_.Get("/api/info", [](const httplib::Request &,
                                httplib::Response &res) {
      json info = {
          {"version", "0.1.0"},
          {"engine", "Project Samanvay — Hybrid HTAP Database"},
          {"supportedStatements",
           {"CREATE TABLE", "DROP TABLE", "INSERT", "SELECT", "UPDATE",
            "DELETE", "SHOW TABLES", "EXPLAIN"}},
          {"supportedTypes",
           {"INT", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "TEXT", "BOOLEAN",
            "TIMESTAMP", "BLOB"}},
          {"supportedAggregations", {"COUNT", "SUM", "AVG", "MIN", "MAX"}},
          {"supportedWhereOps", {"=", "!=", "<", "<=", ">", ">=", "AND", "OR"}},
          {"features",
           {"HTAP (Hybrid OLTP+OLAP)", "LSM-tree storage with compaction",
            "Columnar format for analytical queries", "WAL for crash recovery",
            "Hybrid query router"}},
      };

      res.set_content(successResponse(info).dump(2), "application/json");
    });

    // ── 404 catch-all ────────────────────────────────────────────
    server_.set_error_handler(
        [this](const httplib::Request &, httplib::Response &res) {
          if (res.status == 404) {
            jsonResponse(res, errorResponse("Endpoint not found", "NOT_FOUND"),
                         404);
          }
        });
  }
};

} // namespace api

#endif // API_SERVER_HPP
