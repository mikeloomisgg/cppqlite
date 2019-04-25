cmake_minimum_required(VERSION 3.13)

find_package(Catch2 REQUIRED)

add_executable(cppqlitetests
               main.cpp
               dbtests.cpp
               ../db.cpp
               )
target_link_libraries(cppqlitetests
                      Catch2::Catch2)
target_compile_features(cppqlitetests PUBLIC cxx_std_17)