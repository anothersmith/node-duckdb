set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(duckdb_source_files
  duckdb.cpp
  duckdb.h
  duckdb.hpp
)
add_library(duckdb ${duckdb_source_files})
