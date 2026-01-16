// status.hpp - Error handling and status codes for gRPC integration
#pragma once
#ifndef STATUS_HPP
#define STATUS_HPP

#include <string>
#include <utility>

namespace storage {

// Status codes that map to gRPC status codes
enum class StatusCode {
  OK = 0,                 // Success
  CANCELLED = 1,          // Operation was cancelled
  UNKNOWN = 2,            // Unknown error
  INVALID_ARGUMENT = 3,   // Client specified an invalid argument
  NOT_FOUND = 5,          // Requested key was not found
  ALREADY_EXISTS = 6,     // Key already exists
  PERMISSION_DENIED = 7,  // Permission denied
  RESOURCE_EXHAUSTED = 8, // Resource exhausted (e.g., disk full)
  ABORTED = 10,           // Operation aborted (e.g., conflict)
  OUT_OF_RANGE = 11,      // Operation out of valid range
  UNIMPLEMENTED = 12,     // Operation not implemented
  INTERNAL = 13,          // Internal error
  UNAVAILABLE = 14,       // Service unavailable
  DATA_LOSS = 15,         // Unrecoverable data loss or corruption
  IO_ERROR = 100,         // Custom: I/O error
  CORRUPTION = 101,       // Custom: Data corruption detected
};

// Status class for error handling - similar to gRPC Status
class Status {
private:
  StatusCode code_;
  std::string message_;

public:
  // Default constructor - creates OK status
  Status() : code_(StatusCode::OK) {}

  // Constructor with code and message
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  // Check if status is OK
  bool ok() const { return code_ == StatusCode::OK; }

  // Get status code
  StatusCode code() const { return code_; }

  // Get error message
  const std::string &message() const { return message_; }

  // Convert to string for logging
  std::string toString() const {
    if (ok())
      return "OK";
    return "Error[" + std::to_string(static_cast<int>(code_)) +
           "]: " + message_;
  }

  // ==================== Factory Methods ====================

  static Status OK() { return Status(); }

  static Status NotFound(const std::string &key) {
    return Status(StatusCode::NOT_FOUND, "Key not found: " + key);
  }

  static Status AlreadyExists(const std::string &key) {
    return Status(StatusCode::ALREADY_EXISTS, "Key already exists: " + key);
  }

  static Status Corruption(const std::string &msg) {
    return Status(StatusCode::CORRUPTION, "Corruption: " + msg);
  }

  static Status IOError(const std::string &msg) {
    return Status(StatusCode::IO_ERROR, "I/O error: " + msg);
  }

  static Status InvalidArgument(const std::string &msg) {
    return Status(StatusCode::INVALID_ARGUMENT, "Invalid argument: " + msg);
  }

  static Status Internal(const std::string &msg) {
    return Status(StatusCode::INTERNAL, "Internal error: " + msg);
  }

  static Status ResourceExhausted(const std::string &msg) {
    return Status(StatusCode::RESOURCE_EXHAUSTED, "Resource exhausted: " + msg);
  }

  static Status Unavailable(const std::string &msg) {
    return Status(StatusCode::UNAVAILABLE, "Unavailable: " + msg);
  }
};

// Result type that combines a value with status
template <typename T> class StatusOr {
private:
  Status status_;
  T value_;
  bool has_value_;

public:
  // Construct with error status
  StatusOr(Status status) : status_(std::move(status)), has_value_(false) {}

  // Construct with value (implies OK status)
  StatusOr(T value)
      : status_(Status::OK()), value_(std::move(value)), has_value_(true) {}

  bool ok() const { return status_.ok() && has_value_; }
  const Status &status() const { return status_; }

  const T &value() const { return value_; }
  T &value() { return value_; }

  // Convenience operators
  explicit operator bool() const { return ok(); }
  const T &operator*() const { return value_; }
  T &operator*() { return value_; }
};

} // namespace storage

#endif // STATUS_HPP
