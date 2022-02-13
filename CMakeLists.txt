cmake_minimum_required(VERSION 3.9)
cmake_policy(SET CMP0042 NEW)
project(node-duckdb-addon)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(WIN32)
# Expecting duckdb to be downloaded at the parent directory
  file(GLOB DUCKDBIN "./duckdb/build/src/release/duckdb_static.lib"  "./duckdb/build/extension/httpfs/release/httpfs_extension.lib" "./duckdb/build/extension/parquet/release/parquet_extension.lib" "./duckdb/build/third_party/fmt/release/duckdb_fmt.lib" "./duckdb/build/third_party/libpg_query/release/duckdb_pg_query.lib"  "./duckdb/build/third_party/re2/release/duckdb_re2.lib" "./duckdb/build/third_party/miniz/Release/duckdb_miniz.lib" "./duckdb/build/third_party/utf8proc/Release/duckdb_utf8proc.lib" "./duckdb/build/third_party/hyperloglog/Release/duckdb_hyperloglog.lib" "./duckdb/build/third_party/fastpforlib/Release/duckdb_fastpforlib.lib")
else()
  file(GLOB DUCKDBIN "./duckdb/build/release/src/libduckdb.dylib" "./duckdb/build/release/src/libduckdb.so" "./duckdb/build/release/extension/parquet/libparquet_extension.a" "./duckdb/build/release/extension/httpfs/libhttpfs_extension.a")
endif()

file(COPY ${DUCKDBIN}  DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/Release)
include_directories(${CMAKE_JS_INC} ./duckdb/src/include ./duckdb/extension/parquet/include ./duckdb/extension/httpfs/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/Release)
file(GLOB SOURCE_FILES "./addon/*")
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES} ${CMAKE_JS_SRC})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
find_package(OpenSSL REQUIRED)

if (WIN32)
target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} duckdb_static duckdb_fmt duckdb_pg_query duckdb_re2 duckdb_miniz duckdb_utf8proc duckdb_hyperloglog duckdb_fastpforlib parquet_extension httpfs_extension  ${OPENSSL_CRYPTO_LIBRARY} ${OPENSSL_SSL_LIBRARY})
else ()
target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} duckdb parquet_extension httpfs_extension ${OPENSSL_CRYPTO_LIBRARY} ${OPENSSL_SSL_LIBRARY})
endif()
if(APPLE)
  message("Building for MacOS")
  set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS "-Wl,-rpath,@loader_path/.")
endif()
if(UNIX AND NOT APPLE)
  message("Building for Linux")
  set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS "-Wl,-rpath,'$ORIGIN'")
endif()
if(WIN32)
  message("Building for Windows")
  set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
endif()
# Include N-API wrappers
execute_process(COMMAND node -p "require('node-addon-api').include"
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE NODE_ADDON_API_DIR
        )

#This CMakeLists.txt is not expected to be called directly through CMake.
#Doing so can throw parameter mismatch for string in the below two lines.
string(REPLACE "\n" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})
string(REPLACE "\"" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})  

target_include_directories(${PROJECT_NAME} PRIVATE ${NODE_ADDON_API_DIR})

# define NPI_VERSION
add_compile_definitions(NAPI_VERSION=${napi_build_version})
