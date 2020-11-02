#include "connection.h"
#include "result_wrapper.h"
#include "database.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_executor.h"
#include <iostream>
using namespace std;

namespace NodeDuckDB {
  Napi::FunctionReference Database::constructor;

  Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "DuckDB", {});
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("DuckDB", func);
    return exports;
  }

  Database::Database(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Database>(info) {
    Napi::Env env = info.Env();

    bool read_only = false;
    string database_name = "";

    duckdb::DBConfig config;
    if (read_only)
      config.access_mode = duckdb::AccessMode::READ_ONLY;

    database = duckdb::make_unique<duckdb::DuckDB>(database_name, &config);
    database->LoadExtension<duckdb::ParquetExtension>();
  }

  Napi::Value Database::Close(const Napi::CallbackInfo& info) {
    database = nullptr;

    return info.Env().Undefined();
  }
}
