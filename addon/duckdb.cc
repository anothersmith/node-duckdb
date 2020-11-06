#include "connection.h"
#include "result_iterator.h"
#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_executor.h"
#include <iostream>
#include "type-converters.h"
using namespace std;

namespace NodeDuckDB {
  using namespace TypeConverters;

  Napi::FunctionReference DuckDB::constructor;

  Napi::Object DuckDB::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func = DefineClass(env, "DuckDB", 
                    {
                      InstanceMethod("close", &DuckDB::Close),
                      InstanceAccessor<&DuckDB::IsClosed>("isClosed"),
                      InstanceAccessor<&DuckDB::GetAccessMode>("accessMode"),
                      InstanceAccessor<&DuckDB::GetCheckPointWALSize>("checkPointWALSize"),
                      InstanceAccessor<&DuckDB::GetUseDirectIO>("useDirectIO"),
                      InstanceAccessor<&DuckDB::GetMaximumMemory>("maximumMemory"),
                      InstanceAccessor<&DuckDB::GetUseTemporaryDirectory>("useTemporaryDirectory"),
                      InstanceAccessor<&DuckDB::GetTemporaryDirectory>("temporaryDirectory"),
                      InstanceAccessor<&DuckDB::GetCollation>("collation"),
                      InstanceAccessor<&DuckDB::GetDefaultOrderType>("defaultOrderType"),
                      InstanceAccessor<&DuckDB::GetDefaultNullOrder>("defaultNullOrder"),
                      InstanceAccessor<&DuckDB::GetEnableCopy>("enableCopy"),
                    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("DuckDB", func);
    return exports;
  }

  DuckDB::DuckDB(const Napi::CallbackInfo& info) : Napi::ObjectWrap<DuckDB>(info) {
    Napi::Env env = info.Env();

    string path;
    duckdb::DBConfig nativeConfig;

    if (!info[0].IsUndefined()) {
      if (!info[0].IsObject()) {
        throw Napi::TypeError::New(env, "Invalid argument: must be an object");
      }
      auto config = info[0].ToObject();

      if (!config.Get("path").IsUndefined()) {
          path = convertString(env, config, "path");
      }

      if (!config.Get("options").IsUndefined()) {
        setDBConfig(env, config, nativeConfig);
      }
    }
    database = duckdb::make_unique<duckdb::DuckDB>(path, &nativeConfig);
    database->LoadExtension<duckdb::ParquetExtension>();
  }

  Napi::Value DuckDB::Close(const Napi::CallbackInfo& info) {
    database = nullptr;
    return info.Env().Undefined();
  }
  Napi::Value DuckDB::IsClosed(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Boolean::New(env, IsClosed());
  }

  bool DuckDB::IsClosed() {
    return database == nullptr;
  }

  Napi::Value DuckDB::GetAccessMode(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, static_cast<double>(database->config.access_mode));
  }
  Napi::Value DuckDB::GetCheckPointWALSize(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, database->config.checkpoint_wal_size);
  }
  Napi::Value DuckDB::GetUseDirectIO(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Boolean::New(env, database->config.use_direct_io);
  }
  Napi::Value DuckDB::GetMaximumMemory(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, database->config.maximum_memory);
  }
  Napi::Value DuckDB::GetUseTemporaryDirectory(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Boolean::New(env, database->config.use_temporary_directory);
  }
  Napi::Value DuckDB::GetTemporaryDirectory(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::String::New(env, database->config.temporary_directory);
  }
  Napi::Value DuckDB::GetCollation(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::String::New(env, database->config.collation);
  }
  Napi::Value DuckDB::GetDefaultOrderType(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, static_cast<double>(database->config.default_order_type));
  }
  Napi::Value DuckDB::GetDefaultNullOrder(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Number::New(env, static_cast<double>(database->config.default_null_order));
  }
  Napi::Value DuckDB::GetEnableCopy(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    return Napi::Boolean::New(env, database->config.enable_copy);
  }
}
