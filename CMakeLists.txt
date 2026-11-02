# Redis CMake build (proof of concept).
#
#   cmake --preset release && cmake --build --preset release
#
# Options mirror the make variables of src/Makefile; see cmake/RedisOptions.cmake.
# Binaries are built in the build directory and copied into src/ after each
# build, so the Tcl test suite (which starts ./src/redis-server) works unchanged.
cmake_minimum_required(VERSION 3.13)
project(redis LANGUAGES C)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(RedisOptions)

add_subdirectory(deps)
add_subdirectory(src)
include(RedisTests)
