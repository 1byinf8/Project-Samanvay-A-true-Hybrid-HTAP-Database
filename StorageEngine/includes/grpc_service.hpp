// grpc_service.hpp - gRPC Service Implementation for StorageEngine
// This wraps the StorageEngine to expose it via gRPC for SQL parser integration
#pragma once
#ifndef GRPC_SERVICE_HPP
#define GRPC_SERVICE_HPP

#include <memory>
#include <string>
#include <vector>

#include "status.hpp"
#include "storage_engine.hpp"

namespace storage {
namespace grpc {

// Note: This is a mock implementation since actual gRPC requires generated
// code. In production, you would:
// 1. Run protoc to generate storage_service.grpc.pb.h
// 2. Inherit from the generated service class
// 3. Implement the pure virtual methods

// Status conversion helper
inline StatusCode toProtoStatus(storage::StatusCode code) {
  // Direct mapping since our StatusCode mirrors gRPC codes
  return code;
}

// Request/Response structures (mirrors proto definitions)
struct GetRequest {
  std::string key;
};

struct GetResponse {
  Status status;
  std::string value;
  bool found;
};

struct PutRequest {
  std::string key;
  std::string value;
};

struct PutResponse {
  Status status;
};

struct DeleteRequest {
  std::string key;
};

struct DeleteResponse {
  Status status;
  bool deleted;
};

struct BatchGetRequest {
  std::vector<std::string> keys;
};

struct BatchGetResponse {
  Status status;
  std::vector<std::pair<std::string, std::string>> results;
  std::vector<std::string> missing_keys;
};

struct BatchPutRequest {
  std::vector<std::pair<std::string, std::string>> entries;
};

struct BatchPutResponse {
  Status status;
  int32_t success_count;
  int32_t failure_count;
};

struct StatsResponse {
  Status status;
  uint64_t total_keys;
  uint64_t write_count;
  uint64_t read_count;
  uint64_t sstable_count;
};

// ==================== Service Implementation ====================

class StorageServiceImpl {
private:
  std::shared_ptr<StorageEngine> engine_;

public:
  explicit StorageServiceImpl(std::shared_ptr<StorageEngine> engine)
      : engine_(std::move(engine)) {}

  // ==================== Point Operations ====================

  GetResponse Get(const GetRequest &request) {
    GetResponse response;

    if (request.key.empty()) {
      response.status = Status::InvalidArgument("Key cannot be empty");
      response.found = false;
      return response;
    }

    auto result = engine_->get(request.key);
    if (result.has_value()) {
      response.status = Status::OK();
      response.value = result.value();
      response.found = true;
    } else {
      response.status = Status::NotFound(request.key);
      response.found = false;
    }

    return response;
  }

  PutResponse Put(const PutRequest &request) {
    PutResponse response;

    if (request.key.empty()) {
      response.status = Status::InvalidArgument("Key cannot be empty");
      return response;
    }

    if (engine_->put(request.key, request.value)) {
      response.status = Status::OK();
    } else {
      response.status = Status::Internal("Failed to put key");
    }

    return response;
  }

  DeleteResponse Delete(const DeleteRequest &request) {
    DeleteResponse response;

    if (request.key.empty()) {
      response.status = Status::InvalidArgument("Key cannot be empty");
      response.deleted = false;
      return response;
    }

    // Check if key exists first
    auto exists = engine_->get(request.key).has_value();

    if (engine_->del(request.key)) {
      response.status = Status::OK();
      response.deleted = exists;
    } else {
      response.status = Status::Internal("Failed to delete key");
      response.deleted = false;
    }

    return response;
  }

  // ==================== Batch Operations ====================

  BatchGetResponse BatchGet(const BatchGetRequest &request) {
    BatchGetResponse response;

    if (request.keys.empty()) {
      response.status = Status::OK();
      return response;
    }

    auto results = engine_->batchGet(request.keys);

    for (size_t i = 0; i < request.keys.size(); ++i) {
      if (results[i].has_value()) {
        response.results.emplace_back(request.keys[i], results[i].value());
      } else {
        response.missing_keys.push_back(request.keys[i]);
      }
    }

    response.status = Status::OK();
    return response;
  }

  BatchPutResponse BatchPut(const BatchPutRequest &request) {
    BatchPutResponse response;

    if (request.entries.empty()) {
      response.status = Status::OK();
      response.success_count = 0;
      response.failure_count = 0;
      return response;
    }

    size_t success = engine_->batchPut(request.entries);

    response.status = Status::OK();
    response.success_count = static_cast<int32_t>(success);
    response.failure_count =
        static_cast<int32_t>(request.entries.size() - success);

    return response;
  }

  // ==================== Admin Operations ====================

  Status Flush() {
    engine_->forceFlush();
    return Status::OK();
  }

  Status Compact(int level) {
    engine_->triggerCompaction();
    return Status::OK();
  }

  StatsResponse GetStats() {
    StatsResponse response;

    auto stats = engine_->getStats();

    response.status = Status::OK();
    response.total_keys = 0; // Would need to track this
    response.write_count = stats.totalWrites;
    response.read_count = stats.totalReads;
    response.sstable_count = stats.totalSSTables;

    return response;
  }
};

// ==================== Server Setup (Pseudo-code) ====================

// In a real implementation, you would use grpc::ServerBuilder:
//
// void RunServer(const std::string& address, std::shared_ptr<StorageEngine>
// engine) {
//   StorageServiceImpl service(engine);
//   grpc::ServerBuilder builder;
//   builder.AddListeningPort(address, grpc::InsecureServerCredentials());
//   builder.RegisterService(&service);
//   std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
//   server->Wait();
// }

} // namespace grpc
} // namespace storage

#endif // GRPC_SERVICE_HPP
