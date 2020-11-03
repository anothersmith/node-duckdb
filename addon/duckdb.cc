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
                      InstanceAccessor<&DuckDB::IsClosed>("isClosed")
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

    if (config.IsObject()) {
      // TODO: validation
      auto configObject = config.ToObject();
      path = configObject.Get("path").ToString().Utf8Value();

      if (!configObject.Get("options").IsUndefined()) {
        auto optionsObject = configObject.Get("options").ToObject();
        cout << optionsObject.Get("accessMode").ToNumber().Int32Value() << endl;
        nativeConfig.access_mode = optionsObject.Get("accessMode").ToNumber().Int32Value() ? static_cast<duckdb::AccessMode>(optionsObject.Get("accessMode").ToNumber().Int32Value()) : nativeConfig.access_mode;
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
}
