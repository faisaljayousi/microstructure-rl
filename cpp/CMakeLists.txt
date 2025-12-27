cmake_minimum_required(VERSION 3.20)
project(L2Ingestor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ZLIB REQUIRED)
find_package(FastFloat CONFIG REQUIRED)

add_executable(converter
    core/converter.cpp
)

target_include_directories(converter PRIVATE include)

target_link_libraries(converter PRIVATE
    ZLIB::ZLIB
    FastFloat::fast_float
)

if(MSVC)
    target_compile_options(converter PRIVATE /W4 /O2)
    # target_compile_options(converter PRIVATE $<$<CONFIG:Release>:/GL>)
    # target_link_options(converter PRIVATE $<$<CONFIG:Release>:/LTCG>)
endif()
