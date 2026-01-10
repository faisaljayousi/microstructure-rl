cmake_minimum_required(VERSION 3.20)
project(L2Ingestor LANGUAGES CXX)

# ============================================================
# Project-wide settings
# ============================================================
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Local CMake modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(msrl_options)    # defines MSRL_* options + wheel-mode behaviour
include(msrl_toolchain)  # msrl_apply_warnings(), msrl_apply_opt()

# ============================================================
# Dependencies
# ============================================================
if (MSRL_BUILD_TOOLS)
  find_package(ZLIB REQUIRED)
  find_package(FastFloat CONFIG REQUIRED)
endif()

if (MSRL_BUILD_BENCH)
  find_package(benchmark CONFIG REQUIRED)
endif()

# Python + nanobind (vendored) 
# Only needed when building bindings/wheel
if (MSRL_BUILD_PYTHON OR MSRL_PYTHON_WHEEL)
  find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/nanobind)
endif()

# ============================================================
# Core libraries
# ============================================================
add_library(replay
  core/replay.cpp
)
target_include_directories(replay PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
msrl_apply_warnings(replay)
msrl_apply_opt(replay)
add_library(msrl::replay ALIAS replay)

add_library(sim
  md/sim.cpp
  md/sim_stp.cpp
  md/sim_orders.cpp
  md/sim_activate_set.cpp
  md/sim_bucket_index.cpp
  md/sim_bucket_list.cpp
  md/sim_passive_fills.cpp
  md/sim_fills.cpp
  md/sim_aggressive_fills.cpp
)
target_include_directories(sim PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(sim PUBLIC msrl::replay)
msrl_apply_warnings(sim)
msrl_apply_opt(sim)
add_library(msrl::sim ALIAS sim)

# ============================================================
# Tools
# ============================================================
if (MSRL_BUILD_TOOLS)
  add_executable(converter
    core/converter.cpp
  )
  target_include_directories(converter PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(converter PRIVATE
    ZLIB::ZLIB
    FastFloat::fast_float
    msrl::replay
  )
  msrl_apply_warnings(converter)
  msrl_apply_opt(converter)
endif()

# ============================================================
# Benchmarks
# ============================================================
if (MSRL_BUILD_BENCH)
  add_executable(bench_replay
    bench/bench_replay.cpp
  )
  target_include_directories(bench_replay PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(bench_replay PRIVATE
    msrl::replay
    benchmark::benchmark
  )
  msrl_apply_warnings(bench_replay)
  msrl_apply_opt(bench_replay)
endif()

# ============================================================
# Tests (optional)
# ============================================================
if (MSRL_BUILD_TESTS)
  enable_testing()

  add_executable(test_sim_invariants
    tests/test_sim.cpp
  )
  target_include_directories(test_sim_invariants PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )
  target_link_libraries(test_sim_invariants PRIVATE
    msrl::sim
  )
  msrl_apply_warnings(test_sim_invariants)
  msrl_apply_opt(test_sim_invariants)

  add_test(NAME sim_invariants COMMAND $<TARGET_FILE:test_sim_invariants>)
endif()

# ============================================================
# Python bindings (nanobind)
# ============================================================
if (MSRL_BUILD_PYTHON OR MSRL_PYTHON_WHEEL)
  add_subdirectory(bindings)
endif()
