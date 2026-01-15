cmake_minimum_required(VERSION 3.16)
project(Samanvay 
    VERSION 0.1.0 
    DESCRIPTION "A True Hybrid HTAP Database"
    LANGUAGES CXX
)

# ============================================================================
# C++ Standard and Compiler Settings
# ============================================================================
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Export compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ============================================================================
# Compiler-Specific Flags
# ============================================================================
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    message(STATUS "Detected GCC ${CMAKE_CXX_COMPILER_VERSION}")
    add_compile_options(-Wall -Wextra -Wpedantic)
    
    # GCC < 9 requires linking stdc++fs for std::filesystem
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.0")
        message(STATUS "GCC < 9 detected, linking stdc++fs")
        link_libraries(stdc++fs)
    endif()
    
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Detected Clang ${CMAKE_CXX_COMPILER_VERSION}")
    add_compile_options(-Wall -Wextra -Wpedantic)
    
    # AppleClang may need different handling
    if(CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
        message(STATUS "Using AppleClang")
    endif()
    
elseif(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    message(STATUS "Detected MSVC ${CMAKE_CXX_COMPILER_VERSION}")
    add_compile_options(/W4 /permissive- /Zc:__cplusplus)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
endif()

# ============================================================================
# Thread Support
# ============================================================================
find_package(Threads REQUIRED)

# ============================================================================
# Storage Engine Library (Header-Only)
# ============================================================================
add_library(samanvay_storage INTERFACE)
target_include_directories(samanvay_storage INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/StorageEngine/includes
)
target_link_libraries(samanvay_storage INTERFACE Threads::Threads)

# ============================================================================
# Test Executables
# ============================================================================
option(BUILD_TESTS "Build test executables" ON)

if(BUILD_TESTS)
    # HTAP Features Test
    add_executable(test_htap 
        StorageEngine/test/test_htap_features.cpp
    )
    target_link_libraries(test_htap PRIVATE samanvay_storage)
    
    # Skiplist Unit Tests
    add_executable(test_skiplist 
        StorageEngine/test/test_skiplist.cpp
    )
    target_link_libraries(test_skiplist PRIVATE samanvay_storage)
    
    # Stress Test Benchmark (with optimizations)
    add_executable(stress_test 
        StorageEngine/test/stress_test_benchmark.cpp
    )
    target_link_libraries(stress_test PRIVATE samanvay_storage)
    target_compile_options(stress_test PRIVATE 
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-O3>
        $<$<CXX_COMPILER_ID:MSVC>:/O2>
    )
    
    # Memtable Test
    add_executable(test_memtable 
        StorageEngine/test/test_almost.cpp
    )
    target_link_libraries(test_memtable PRIVATE samanvay_storage)
    
    # Simple Benchmark
    add_executable(simple_bench 
        StorageEngine/test/simple.cpp
    )
    target_link_libraries(simple_bench PRIVATE samanvay_storage)
endif()

# ============================================================================
# Installation (optional)
# ============================================================================
install(DIRECTORY StorageEngine/includes/
    DESTINATION include/samanvay
    FILES_MATCHING PATTERN "*.hpp"
)

# ============================================================================
# Summary
# ============================================================================
message(STATUS "")
message(STATUS "=== Samanvay HTAP Database ===")
message(STATUS "Version:      ${PROJECT_VERSION}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Compiler:     ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "Build Tests:  ${BUILD_TESTS}")
message(STATUS "")
