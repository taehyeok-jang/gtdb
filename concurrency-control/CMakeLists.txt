# ---------------------------------------------------------------------------
# BUZZDB
# ---------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.10)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

set(CMAKE_CXX_STANDARD 17)              # C++17
set(CMAKE_CXX_STANDARD_REQUIRED ON)     # C++17 support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # Clang-tidy.
set(BUILD_SHARED_LIBS ON)

project(BuzzDB
        VERSION 2020
        DESCRIPTION "BuzzDB Database Management System (Educational)"
        LANGUAGES C CXX
)

# ---------------------------------------------------------------------------
# Platform
# ---------------------------------------------------------------------------

if(UNIX)
else()
    message(SEND_ERROR "Unsupported platform")
endif()

# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

find_package(Threads REQUIRED)


set(THREADS_PREFER_PTHREAD_FLAG ON)

add_definitions(${LLVM_DEFINITIONS})


include("${CMAKE_SOURCE_DIR}/third_party/googletest.cmake")
include("${CMAKE_SOURCE_DIR}/third_party/gflags.cmake")
include("${CMAKE_SOURCE_DIR}/third_party/benchmark.cmake")
include("${CMAKE_SOURCE_DIR}/third_party/rapidjson.cmake")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra -Werror ")    

# ---------------------------------------------------------------------------
# Scripts
# ---------------------------------------------------------------------------

set(SCRIPT_DIR "${CMAKE_SOURCE_DIR}/script")

include("${SCRIPT_DIR}/clang-tidy.cmake")
include("${SCRIPT_DIR}/clang-format.cmake")
include("${SCRIPT_DIR}/valgrind.cmake")

# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/src/CMakeLists.txt")

# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/test/tool/CMakeLists.txt")

# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/test/benchmark/CMakeLists.txt")

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

include("${CMAKE_SOURCE_DIR}/test/CMakeLists.txt")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

message(STATUS "BuzzDB settings")
message(STATUS "    LLVM_LDFLAGS                = ${LLVM_LDFLAGS}")
message(STATUS "    LLVM_LIBS                   = ${LLVM_LIBS}")
message(STATUS "    LLVM_INCLUDE_DIRS           = ${LLVM_INCLUDE_DIRS}")
message(STATUS "    GFLAGS_INCLUDE_DIR          = ${GFLAGS_INCLUDE_DIR}")
message(STATUS "    GFLAGS_LIBRARY_PATH         = ${GFLAGS_LIBRARY_PATH}")
message(STATUS "[TEST] settings")
message(STATUS "    GTEST_INCLUDE_DIR           = ${GTEST_INCLUDE_DIR}")
message(STATUS "    GTEST_LIBRARY_PATH          = ${GTEST_LIBRARY_PATH}")
message(STATUS "    VALGRIND_BIN                = ${VALGRIND_BIN}")
message(STATUS "    VALGRIND_SUPPRESSIONS_FILE  = ${VALGRIND_SUPPRESSIONS_FILE}")
message(STATUS "    CLANG_TIDY_BIN              = ${CLANG_TIDY_BIN}")
message(STATUS "    CLANG_FORMAT_BIN            = ${CLANG_FORMAT_BIN}")
message(STATUS "    CPPLINT_BIN                 = ${CPPLINT_BIN}")