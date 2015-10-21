cmake_minimum_required (VERSION 2.6)

enable_language (C)
enable_testing ()

find_package (Threads)

# Project
project (occ)

# Set directories
set (GTEST_SOURCE_DIR gtest-1.6.0)
set (PROJECT_SOURCE_DIR src)
set (PROJECT_BUILD_DIR build)

## Add C++0x suport ##
# Initialize CXXFLAGS.
set(CMAKE_CXX_FLAGS
#  "-Wall -std=c++0x -D__STDC_LIMIT_MACROS ${CMAKE_CXX_FLAGS_RELEASE}")
"-Wall -std=c++0x -D__STDC_LIMIT_MACROS -O3")
set(CMAKE_CXX_FLAGS_DEBUG          "-O0 -g")
set(CMAKE_CXX_FLAGS_MINSIZEREL     "-Os -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE        "-O3 -DNDEBUG")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")

# Add source file
include_directories (${PROJECT_SOURCE_DIR})
add_subdirectory (${PROJECT_SOURCE_DIR} ${PROJECT_BUILD_DIR})
