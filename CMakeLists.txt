cmake_minimum_required(VERSION 3.20)
project(bench-hash-join)

set(CMAKE_CXX_STANDARD 17)

aux_source_directory(. DIR_SRCS)

add_executable(bench-hash-join ${DIR_SRCS})

