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
    target_compile_options(converter PRIVATE /W4 /O2 /permissive- /EHsc)
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
    target_compile_options(bench_replay PRIVATE /W4 /O2 /permissive- /EHsc)
endif()
