cmake_minimum_required(VERSION 3.16)

project(Samanvay 
    VERSION 0.1.0 
    DESCRIPTION "A True Hybrid HTAP Database"
    LANGUAGES CXX
)

# ============================================================================
# C++ Standard
# ============================================================================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============================================================================
# Compiler Flags
# ============================================================================
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    message(STATUS "Detected ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
    add_compile_options(-Wall -Wextra -Wpedantic)
elseif(MSVC)
    add_compile_options(/W4 /permissive- /Zc:__cplusplus)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
endif()

# ============================================================================
# Dependencies
# ============================================================================
find_package(Threads REQUIRED)

# ============================================================================
# Hyrise SQL Parser (built from source)
# FIX: Use GLOB with explicit subfolders instead of GLOB_RECURSE
#      to avoid accidentally pulling in test/example files
# ============================================================================
file(GLOB_RECURSE SQLPARSER_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/third_party/sql-parser/src/*.cpp
)
add_library(sqlparser STATIC ${SQLPARSER_SRC})

target_include_directories(sqlparser PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/third_party/sql-parser/src
)

# Suppress warnings from third-party parser code
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(sqlparser PRIVATE -w)
endif()

# ============================================================================
# Storage Engine (Header-only Core)
# ============================================================================
add_library(samanvay_storage INTERFACE)

target_include_directories(samanvay_storage INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/includes
)

target_link_libraries(samanvay_storage INTERFACE Threads::Threads)

# ============================================================================
# SQL Layer (Query Executor Library)
# FIX: sqlparser must be PUBLIC not PRIVATE because query_executor.hpp
#      includes Hyrise headers — consumers of samanvay_sql need those too
# ============================================================================
add_library(samanvay_sql STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/SQLLayer/query_executor.cpp
)

target_include_directories(samanvay_sql PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/SQLLayer/includes
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/third_party/sql-parser/src
)

target_link_libraries(samanvay_sql
    PUBLIC
        samanvay_storage
        sqlparser
)

# ============================================================================
# SQL Shell Executable
# ============================================================================
add_executable(samanvay_shell
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/SQLLayer/sql_shell.cpp
)

target_link_libraries(samanvay_shell
    PRIVATE
        samanvay_sql
)

# ============================================================================
# Tests
# ============================================================================
option(BUILD_TESTS "Build test executables" ON)

if(BUILD_TESTS)

    # ── Storage Engine Tests ─────────────────────────────────────
    add_executable(test_htap
        StorageEngine/test/test_htap_features.cpp)
    target_link_libraries(test_htap PRIVATE samanvay_storage)

    add_executable(test_skiplist
        StorageEngine/test/test_skiplist.cpp)
    target_link_libraries(test_skiplist PRIVATE samanvay_storage)

    add_executable(stress_test
        StorageEngine/test/stress_test_benchmark.cpp)
    target_link_libraries(stress_test PRIVATE samanvay_storage)
    target_compile_options(stress_test PRIVATE
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-O3>
        $<$<CXX_COMPILER_ID:MSVC>:/O2>
    )

    add_executable(test_memtable
        StorageEngine/test/test_almost.cpp)
    target_link_libraries(test_memtable PRIVATE samanvay_storage)

    add_executable(simple_bench
        StorageEngine/test/simple.cpp)
    target_link_libraries(simple_bench PRIVATE samanvay_storage)

    # ── SQL Layer Tests ──────────────────────────────────────────
    add_executable(Unit_Test2
        StorageEngine/SQLLayer/tests/Unit_Test2.cpp)
    target_link_libraries(Unit_Test2 PRIVATE samanvay_sql)

    # Phase 1 unit tests (schema registry + row codec, no engine needed)
    add_executable(test_phase1
        StorageEngine/SQLLayer/tests/test_phase1.cpp)
    target_link_libraries(test_phase1 PRIVATE samanvay_storage)
    target_include_directories(test_phase1 PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/SQLLayer/includes
    )

endif()