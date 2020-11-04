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


    string path;
    duckdb::DBConfig nativeConfig;

    if (!config.IsUndefined()) {
      if (!config.IsObject()) {
        throw Napi::TypeError::New(env, "Invalid argument: must be an object");
      }

      auto configObject = config.ToObject();

      if (!configObject.Get("path").IsUndefined()) {
        if (!configObject.Get("path").IsString()) {
          throw Napi::TypeError::New(env, "Invalid path: must be a string");
        }
        path = configObject.Get("path").ToString().Utf8Value();
      }

      if (!configObject.Get("options").IsUndefined()) {
        if (!configObject.Get("options").IsObject()) {
          throw Napi::TypeError::New(env, "Invalid options: must be an object");
        }
        auto optionsObject = configObject.Get("options").ToObject();

        if (!optionsObject.Get("accessMode").IsUndefined()) {
          if (!optionsObject.Get("accessMode").IsNumber()) {
            throw Napi::TypeError::New(env, "Invalid accessMode: must be of type AccessMode enum");
          }
          auto accessModeValue = optionsObject.Get("accessMode").ToNumber().Int32Value();
          if (accessModeValue < static_cast<int>(duckdb::AccessMode::UNDEFINED) || accessModeValue > static_cast<int>(duckdb::AccessMode::READ_WRITE)) {
            throw Napi::TypeError::New(env, "Invalid accessMode: must be of type AccessMode enum");
          }
          nativeConfig.access_mode = static_cast<duckdb::AccessMode>(accessModeValue);
        }

        if (!optionsObject.Get("checkPointWALSize").IsUndefined()) {
          if (!optionsObject.Get("checkPointWALSize").IsNumber()) {
            throw Napi::TypeError::New(env, "Invalid checkPointWALSize: must be a number");
          }
          nativeConfig.checkpoint_wal_size = optionsObject.Get("checkPointWALSize").ToNumber().Int32Value();
        }
        
        if (!optionsObject.Get("maximumMemory").IsUndefined()) {
          if (!optionsObject.Get("maximumMemory").IsNumber()) {
            throw Napi::TypeError::New(env, "Invalid maximumMemory: must be a number");
          }
          nativeConfig.maximum_memory = optionsObject.Get("maximumMemory").ToNumber().Int32Value();
        }
        
        if (!optionsObject.Get("useTemporaryDirectory").IsUndefined()) {
          if (!optionsObject.Get("useTemporaryDirectory").IsBoolean()) {
            throw Napi::TypeError::New(env, "Invalid useTemporaryDirectory: must be a boolean");
          }
          nativeConfig.use_temporary_directory = optionsObject.Get("useTemporaryDirectory").ToBoolean().Value();
        }

        if (!optionsObject.Get("temporaryDirectory").IsUndefined()) {
          if (!optionsObject.Get("temporaryDirectory").IsString()) {
            throw Napi::TypeError::New(env, "Invalid temporaryDirectory: must be a string");
          }
          nativeConfig.temporary_directory = optionsObject.Get("temporaryDirectory").ToString().Utf8Value();
        }

        if (!optionsObject.Get("collation").IsUndefined()) {
          if (!optionsObject.Get("collation").IsString()) {
            throw Napi::TypeError::New(env, "Invalid collation: must be a string");
          }
          nativeConfig.collation = optionsObject.Get("collation").ToString().Utf8Value();
        }

        if (!optionsObject.Get("defaultOrderType").IsUndefined()) {
          if (!optionsObject.Get("defaultOrderType").IsNumber()) {
            throw Napi::TypeError::New(env, "Invalid defaultOrderType: must be of type OrderType enum");
          }
          auto defaultOrderTypeValue = optionsObject.Get("defaultOrderType").ToNumber().Int32Value();
          if (defaultOrderTypeValue < static_cast<int>(duckdb::OrderType::INVALID) || defaultOrderTypeValue > static_cast<int>(duckdb::OrderType::DESCENDING)) {
            throw Napi::TypeError::New(env, "Invalid defaultOrderType: must be of type OrderType enum");
          }
          nativeConfig.default_order_type = static_cast<duckdb::OrderType>(defaultOrderTypeValue);
        }

        if (!optionsObject.Get("defaultNullOrder").IsUndefined()) {
          if (!optionsObject.Get("defaultNullOrder").IsNumber()) {
            throw Napi::TypeError::New(env, "Invalid defaultNullOrder: must be of type OrderByNullType enum");
          }
          auto defaultNullOrderValue = optionsObject.Get("defaultNullOrder").ToNumber().Int32Value();
          if (defaultNullOrderValue < static_cast<int>(duckdb::OrderByNullType::INVALID) || defaultNullOrderValue > static_cast<int>(duckdb::OrderByNullType::NULLS_LAST)) {
            throw Napi::TypeError::New(env, "Invalid defaultNullOrder: must be of type OrderByNullType enum");
          }
          nativeConfig.default_null_order = static_cast<duckdb::OrderByNullType>(defaultNullOrderValue);
        }

        if (!optionsObject.Get("enableCopy").IsUndefined()) {
          if (!optionsObject.Get("enableCopy").IsBoolean()) {
            throw Napi::TypeError::New(env, "Invalid enableCopy: must be a boolean");
          }
          nativeConfig.enable_copy = optionsObject.Get("enableCopy").ToBoolean().Value();
        }
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
