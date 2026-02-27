// main.cpp — Entry point for Samanvay REST API Server
// Initializes StorageEngine, SchemaRegistry, QueryExecutor, and starts HTTP
// server
//
// Build with CMake:
//   cmake --build build --target samanvay_api
//
// Usage:
//   ./samanvay_api [port] [data_dir]
//   Default: port=8080, data_dir=./data

#include <csignal>
#include <iostream>
#include <string>

#include "../SQLLayer/includes/query_executor.hpp"
#include "../SQLLayer/includes/schema_registry.hpp"
#include "../includes/storage_engine.hpp"
#include "api_server.hpp"

// Global pointer for signal handler
static api::SamanvayAPIServer *g_server = nullptr;

void signalHandler(int signum) {
  std::cout << "\n\nShutting down API server (signal " << signum << ")...\n";
  if (g_server) {
    g_server->stop();
  }
}

int main(int argc, char *argv[]) {
  // ── Parse command-line arguments ─────────────────────────────
  int port = 8080;
  std::string dataDir = "./data";

  if (argc >= 2) {
    try {
      port = std::stoi(argv[1]);
    } catch (...) {
      std::cerr << "Invalid port: " << argv[1] << "\n";
      return 1;
    }
  }
  if (argc >= 3) {
    dataDir = argv[2];
  }

  std::cout << "Initializing Samanvay HTAP Database...\n";
  std::cout << "  Data directory: " << dataDir << "\n";
  std::cout << "  API port: " << port << "\n\n";

  // ── Initialize Storage Engine ────────────────────────────────
  storage::StorageEngineConfig config;
  config.dataDirectory = dataDir;
  config.columnarDirectory = dataDir + "/columnar";
  config.walPath = dataDir + "/wal.log";

  storage::StorageEngine engine(config);
  engine.recoverFromWAL();

  // ── Initialize SQL Layer ─────────────────────────────────────
  sql::SchemaRegistry registry(dataDir + "/schema_registry.sdb");
  sql::QueryExecutor executor(engine, registry);

  // ── Start API Server ─────────────────────────────────────────
  api::SamanvayAPIServer server(engine, registry, executor, port);
  g_server = &server;

  // Register signal handlers for graceful shutdown
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  if (!server.start()) { // Blocks until server stops
    std::cerr << "Failed to start server. Exiting.\n";
    return 1;
  }

  std::cout << "Server stopped. Goodbye!\n";
  return 0;
}
