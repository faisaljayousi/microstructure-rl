cmake_minimum_required(VERSION 3.20)
project(L2Ingestor LANGUAGES CXX)

# ============================================================
# Build switches
# ============================================================
option(MSRL_PYTHON_WHEEL "Build Python wheel (only core libs + extension)" OFF)
option(MSRL_BUILD_TOOLS  "Build auxiliary tools (converter, etc.)" ON)
option(MSRL_BUILD_BENCH  "Build benchmarks" ON)
option(MSRL_BUILD_TESTS  "Build tests" ON)

# Wheel build should be minimal: no tools/benches/tests.
if (MSRL_PYTHON_WHEEL)
  set(MSRL_BUILD_TOOLS OFF)
  set(MSRL_BUILD_BENCH OFF)
  set(MSRL_BUILD_TESTS OFF)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Extension modules should be linkable against static libs on all platforms
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ============================================================
# Dependencies
# ============================================================
# Only needed if corresponding targets are enabled.
if (MSRL_BUILD_TOOLS)
  find_package(ZLIB REQUIRED)
  find_package(FastFloat CONFIG REQUIRED)
endif()

if (MSRL_BUILD_BENCH)
  find_package(benchmark CONFIG REQUIRED)
endif()

# Python + nanobind (vendored)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/nanobind)

# ============================================================
# Replay kernel library
# ============================================================
add_library(replay
  core/replay.cpp
)

target_include_directories(replay PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if (MSVC)
  target_compile_options(replay PRIVATE /W4 /permissive- /EHsc)
else()
  target_compile_options(replay PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Provide stable alias to avoid accidental filename-based linking
add_library(msrl::replay ALIAS replay)

# ============================================================
# Converter executable (optional)
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
    replay
  )

  if (MSVC)
    target_compile_options(converter PRIVATE /W4 /permissive- /EHsc)
    target_compile_options(converter PRIVATE
      $<$<CONFIG:Release>:/O2>
      $<$<CONFIG:RelWithDebInfo>:/O2>
      $<$<CONFIG:MinSizeRel>:/O1>
    )
  else()
    target_compile_options(converter PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endif()

# ============================================================
# Benchmark executable (optional)
# ============================================================
if (MSRL_BUILD_BENCH)
  add_executable(bench_replay
    bench/bench_replay.cpp
  )

  target_include_directories(bench_replay PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
  )

  target_link_libraries(bench_replay PRIVATE
    replay
    benchmark::benchmark
  )

  if (MSVC)
    target_compile_options(bench_replay PRIVATE /W4 /permissive- /EHsc)
    target_compile_options(bench_replay PRIVATE
      $<$<CONFIG:Release>:/O2>
      $<$<CONFIG:RelWithDebInfo>:/O2>
      $<$<CONFIG:MinSizeRel>:/O1>
    )
  else()
    target_compile_options(bench_replay PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endif()

# ============================================================
# Simulator library
# ============================================================
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

target_link_libraries(sim PUBLIC
  msrl::replay
)

if (MSVC)
  target_compile_options(sim PRIVATE /W4 /permissive- /EHsc)
else()
  target_compile_options(sim PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Stable alias
add_library(msrl::sim ALIAS sim)

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

  if (MSVC)
    target_compile_options(test_sim_invariants PRIVATE /W4 /permissive- /EHsc)
    target_compile_options(test_sim_invariants PRIVATE
      $<$<CONFIG:Release>:/O2>
      $<$<CONFIG:RelWithDebInfo>:/O2>
      $<$<CONFIG:MinSizeRel>:/O1>
    )
  else()
    target_compile_options(test_sim_invariants PRIVATE -Wall -Wextra -Wpedantic)
  endif()

  add_test(NAME sim_invariants COMMAND $<TARGET_FILE:test_sim_invariants>)
endif()

# ============================================================
# Python bindings (nanobind)
# ============================================================
nanobind_add_module(_core
  bindings/microstructure_nb.cpp
)

# Ensure the produced file is _core.<abi>.pyd and installs into the Python package
set_target_properties(_core PROPERTIES
  OUTPUT_NAME "_core"
)

target_include_directories(_core PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Link to targets (not filenames) so build order + paths are correct under pip/scikit-build
target_link_libraries(_core PRIVATE
  msrl::sim
  msrl::replay
)

if (MSVC)
  target_compile_options(_core PRIVATE /W4 /permissive- /EHsc)
  target_compile_options(_core PRIVATE
    $<$<CONFIG:Release>:/O2>
    $<$<CONFIG:RelWithDebInfo>:/O2>
    $<$<CONFIG:MinSizeRel>:/O1>
  )
else()
  target_compile_options(_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Install into the Python package directory inside the wheel:
#   microstructure_rl/_core.pyd
install(TARGETS _core
  LIBRARY DESTINATION microstructure_rl
  RUNTIME DESTINATION microstructure_rl
)
