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

  string convertString(const Napi::Env &env, const Napi::Object &options, const string propertyName) {
    if (!options.Get(propertyName).IsString()) {
      throw Napi::TypeError::New(env, "Invalid " + propertyName + ": must be a string");
    }
    return options.Get(propertyName).ToString().Utf8Value();
  }

  int32_t convertNumber(const Napi::Env &env, const Napi::Object &options, const string propertyName) {
    if (!options.Get(propertyName).IsNumber()) {
      throw Napi::TypeError::New(env, "Invalid " + propertyName + ": must be a number");
    }
    return options.Get(propertyName).ToNumber().Int32Value();
  }

  bool convertBoolean(const Napi::Env &env, const Napi::Object &options, const string propertyName) {
    if (!options.Get(propertyName).IsBoolean()) {
      throw Napi::TypeError::New(env, "Invalid " + propertyName + ": must be a boolean");
    }
    return options.Get(propertyName).ToBoolean().Value();
  }

  int32_t convertEnum(const Napi::Env &env, const Napi::Object &options, const string propertyName, const int min, const int max) {
      const string errorMessage = "Invalid " + propertyName + ": must be of appropriate enum type";
      if (!options.Get(propertyName).IsNumber()) {
        throw Napi::TypeError::New(env, errorMessage);
      }
      auto value = options.Get(propertyName).ToNumber().Int32Value();
      if (value < min || value > max) {
        throw Napi::TypeError::New(env, errorMessage);
      }
      return value;
  }

  void setDBConfig(const Napi::Env &env, const Napi::Object &config, duckdb::DBConfig &nativeConfig) {
    if (!config.Get("options").IsObject()) {
      throw Napi::TypeError::New(env, "Invalid options: must be an object");
    }
    auto optionsObject = config.Get("options").ToObject();

    if (!optionsObject.Get("accessMode").IsUndefined()) {
      nativeConfig.access_mode = static_cast<duckdb::AccessMode>(convertEnum(env, optionsObject, "accessMode", static_cast<int>(duckdb::AccessMode::UNDEFINED), static_cast<int>(duckdb::AccessMode::READ_WRITE)));
    }

    if (!optionsObject.Get("checkPointWALSize").IsUndefined()) {
      nativeConfig.checkpoint_wal_size = convertNumber(env, optionsObject, "checkPointWALSize");
    }
    
    if (!optionsObject.Get("maximumMemory").IsUndefined()) {
      nativeConfig.maximum_memory = convertNumber(env, optionsObject, "maximumMemory");
    }
    
    if (!optionsObject.Get("useTemporaryDirectory").IsUndefined()) {
      nativeConfig.use_temporary_directory = convertBoolean(env, optionsObject, "useTemporaryDirectory");
    }

    if (!optionsObject.Get("temporaryDirectory").IsUndefined()) {
      nativeConfig.temporary_directory = convertString(env, optionsObject, "temporaryDirectory");
    }

    if (!optionsObject.Get("collation").IsUndefined()) {
      nativeConfig.collation = convertString(env, optionsObject, "collation");
    }

    if (!optionsObject.Get("defaultOrderType").IsUndefined()) {
      nativeConfig.default_order_type = static_cast<duckdb::OrderType>(convertEnum(env, optionsObject, "defaultOrderType", static_cast<int>(duckdb::OrderType::INVALID), static_cast<int>(duckdb::OrderType::DESCENDING)));
    }

    if (!optionsObject.Get("defaultNullOrder").IsUndefined()) {
      nativeConfig.default_null_order = static_cast<duckdb::OrderByNullType>(convertEnum(env, optionsObject, "defaultNullOrder", static_cast<int>(duckdb::OrderByNullType::INVALID), static_cast<int>(duckdb::OrderByNullType::NULLS_LAST)));
    }

    if (!optionsObject.Get("enableCopy").IsUndefined()) {
      nativeConfig.enable_copy = convertBoolean(env, optionsObject, "enableCopy");
    }
  }

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
