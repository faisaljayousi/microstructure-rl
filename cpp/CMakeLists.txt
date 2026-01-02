cmake_minimum_required(VERSION 3.20)
project(L2Ingestor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# =========================
# Dependencies
# =========================
find_package(ZLIB REQUIRED)
find_package(FastFloat CONFIG REQUIRED)
find_package(benchmark CONFIG REQUIRED)

# =========================
# Replay kernel library
# =========================
add_library(replay
    core/replay.cpp
)

target_include_directories(replay PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(replay PRIVATE /W4 /permissive- /EHsc)
endif()

# =========================
# Converter executable
# =========================
add_executable(converter
    core/converter.cpp
)

target_include_directories(converter PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(converter PRIVATE
    ZLIB::ZLIB
    FastFloat::fast_float
)

if(MSVC)
    target_compile_options(converter PRIVATE /W4 /permissive- /EHsc)
    target_compile_options(converter PRIVATE
        $<$<CONFIG:Release>:/O2>
        $<$<CONFIG:RelWithDebInfo>:/O2>
    )
endif()

# =========================
# Benchmark executable
# =========================
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

if(MSVC)
    target_compile_options(bench_replay PRIVATE /W4 /permissive- /EHsc)
    target_compile_options(bench_replay PRIVATE
        $<$<CONFIG:Release>:/O2>
        $<$<CONFIG:RelWithDebInfo>:/O2>
    )
endif()

# =========================
# Simulator
# =========================
add_library(sim 
    md/sim.cpp
    md/sim_stp.cpp
    md/sim_orders.cpp
    md/sim_activate_set.cpp
    md/sim_bucket_index.cpp
    md/sim_bucket_list.cpp
)
target_link_libraries(sim PUBLIC replay)
target_include_directories(sim PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)


# =========================
# Tests (phase-1 invariants)
# =========================
enable_testing()

add_executable(test_sim_invariants
    tests/test_sim.cpp
)

target_include_directories(test_sim_invariants PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(test_sim_invariants PRIVATE
    sim
)


if(MSVC)
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
