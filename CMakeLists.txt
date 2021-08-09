cmake_minimum_required(VERSION 3.9)
cmake_policy(SET CMP0042 NEW)
project(node-duckdb-addon)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
file(GLOB DUCKDBIN
  "./duckdb/build/release/src/libduckdb.dylib"
  "./duckdb/build/release/src/libduckdb.so"
  "./duckdb/build/release/extension/parquet/libparquet_extension.a"
  "./duckdb/build/release/extension/httpfs/libhttpfs_extension.a"
)
file(COPY ${DUCKDBIN}  DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/Release)
include_directories(${CMAKE_JS_INC} ./duckdb/src/include ./duckdb/extension/parquet/include ./duckdb/extension/httpfs/include)
link_directories(${CMAKE_CURRENT_BINARY_DIR}/Release)
file(GLOB SOURCE_FILES "./addon/*")
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES} ${CMAKE_JS_SRC})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
set(OPENSSL_CRYPTO_LIBRARY "/usr/lib64/libcrypto.so")
set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} duckdb parquet_extension httpfs_extension OpenSSL::Crypto "/usr/lib64/libssl.so")
if(APPLE)
  message("Building for MacOS")
  set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS "-Wl,-rpath,@loader_path/.")
endif()
if(UNIX AND NOT APPLE)
  message("Building for Linux")
  set_target_properties(${PROJECT_NAME} PROPERTIES LINK_FLAGS "-Wl,-rpath,'$ORIGIN'")
endif()

# Include N-API wrappers
execute_process(COMMAND node -p "require('node-addon-api').include"
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE NODE_ADDON_API_DIR
        )
string(REPLACE "\n" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})
string(REPLACE "\"" "" NODE_ADDON_API_DIR ${NODE_ADDON_API_DIR})
target_include_directories(${PROJECT_NAME} PRIVATE ${NODE_ADDON_API_DIR})

# define NPI_VERSION
add_compile_definitions(NAPI_VERSION=${napi_build_version})
