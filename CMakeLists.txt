cmake_minimum_required(VERSION 3.13)
project(cppqlite)

set(CMAKE_CXX_STANDARD 17)

add_executable(cppqlite main.cpp db.cpp)

add_subdirectory(tests)