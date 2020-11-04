#include "connection.h"
#include "result_iterator.h"
#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_executor.h"
#include <iostream>
using namespace std;

namespace NodeDuckDB {
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

    auto config = info[0];
    if (!config.IsUndefined() && !config.IsObject()) {
      throw Napi::TypeError::New(env, "First argument is an optional config object");
    }

    string path;
    duckdb::DBConfig nativeConfig;


    if (!config.IsUndefined()) {
      // TODO: validation
      auto configObject = config.ToObject();
      path = !configObject.Get("path").IsUndefined() ? configObject.Get("path").ToString().Utf8Value() : "";

      if (!configObject.Get("options").IsUndefined()) {
        auto optionsObject = configObject.Get("options").ToObject();

        nativeConfig.access_mode = !optionsObject.Get("accessMode").IsUndefined() ? static_cast<duckdb::AccessMode>(optionsObject.Get("accessMode").ToNumber().Int32Value()) : nativeConfig.access_mode;
        nativeConfig.checkpoint_wal_size = !optionsObject.Get("checkPointWALSize").IsUndefined() ? optionsObject.Get("checkPointWALSize").ToNumber().Int32Value() : nativeConfig.checkpoint_wal_size;
        nativeConfig.maximum_memory = !optionsObject.Get("maximumMemory").IsUndefined() ? optionsObject.Get("maximumMemory").ToNumber().Int32Value() : nativeConfig.maximum_memory;
        nativeConfig.use_temporary_directory = !optionsObject.Get("useTemporaryDirectory").IsUndefined() ? optionsObject.Get("useTemporaryDirectory").ToBoolean().Value() : nativeConfig.use_temporary_directory;
        nativeConfig.temporary_directory = !optionsObject.Get("temporaryDirectory").IsUndefined() ? optionsObject.Get("temporaryDirectory").ToString().Utf8Value() : nativeConfig.temporary_directory;
        nativeConfig.collation = !optionsObject.Get("collation").IsUndefined() ? optionsObject.Get("collation").ToString().Utf8Value() : nativeConfig.collation;
        nativeConfig.default_order_type = !optionsObject.Get("defaultOrderType").IsUndefined() ? static_cast<duckdb::OrderType>(optionsObject.Get("defaultOrderType").ToNumber().Int32Value()) : nativeConfig.default_order_type;
        nativeConfig.default_null_order = !optionsObject.Get("defaultNullOrder").IsUndefined() ? static_cast<duckdb::OrderByNullType>(optionsObject.Get("defaultNullOrder").ToNumber().Int32Value()) : nativeConfig.default_null_order;
        nativeConfig.enable_copy = !optionsObject.Get("enableCopy").IsUndefined() ? optionsObject.Get("enableCopy").ToBoolean().Value() : nativeConfig.enable_copy;
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
