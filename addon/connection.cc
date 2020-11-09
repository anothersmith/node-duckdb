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
  Napi::FunctionReference Connection::constructor;

  Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {

    Napi::Function func =
        DefineClass(env,
                    "Connection",
                    {
                      InstanceMethod("execute", &Connection::Execute),
                      InstanceMethod("close", &Connection::Close),
                      InstanceAccessor<&Connection::IsClosed>("isClosed")
                    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("Connection", func);
    return exports;
  }

  Connection::Connection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Connection>(info) {
    Napi::Env env = info.Env();

    if (!info[0].IsObject() || !info[0].ToObject().InstanceOf(DuckDB::constructor.Value())) {
      throw Napi::TypeError::New(env, "Must provide a valid DuckDB object");
    }

    bool read_only = false;
    string database_name = "";

    duckdb::DBConfig config;
    if (read_only)
      config.access_mode = duckdb::AccessMode::READ_ONLY;
    
    auto unwrappedDb = DuckDB::Unwrap(info[0].ToObject());
    if (unwrappedDb->IsClosed()) {
      throw Napi::TypeError::New(env, "Database is closed");
    }
    connection = duckdb::make_shared<duckdb::Connection>(*unwrappedDb->database);
  }

  Napi::Value Connection::Execute(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    try {
      if (!info[0].IsString()) {
        throw Napi::TypeError::New(env, "First argument must be a string");
      }

      if (!info[1].IsUndefined() && !info[1].IsObject()) {
        throw Napi::TypeError::New(env, "Second argument is an optional object");
      }

      if (this->connection == nullptr) {
        throw Napi::TypeError::New(env, "Connection is closed");
      }

      auto query = info[0].ToString().Utf8Value();
      auto forceMaterializedValue = false;
      ResultFormat rowResultFormatValue = ResultFormat::OBJECT;
      if (!info[1].IsUndefined()) {
        auto options = info[1].ToObject();
        if (!options.Get("forceMaterialized").IsUndefined()) {
          forceMaterializedValue = TypeConverters::convertBoolean(env, options, "forceMaterialized");
        } 

        if (!options.Get("rowResultFormat").IsUndefined()) {
          rowResultFormatValue = static_cast<ResultFormat>(TypeConverters::convertEnum(env, options, "rowResultFormat", static_cast<int>(ResultFormat::OBJECT), static_cast<int>(ResultFormat::ARRAY)));
        } 
      }

      AsyncExecutor* wk = new AsyncExecutor(env, query, connection, deferred, forceMaterializedValue, rowResultFormatValue);
      wk->Queue();
    } catch (Napi::Error& e) {
      deferred.Reject(e.Value());
    } catch (...) {
      deferred.Reject(Napi::Error::New(env, "Unknown Error: Something happened when preparing to run the query").Value());
    }

    return deferred.Promise();
  }

  Napi::Value Connection::Close(const Napi::CallbackInfo& info) {
    connection = nullptr;
    database = nullptr;
    return info.Env().Undefined();
  }
  Napi::Value Connection::IsClosed(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();
    bool isClosed = connection == nullptr || connection->context->is_invalidated;
    return Napi::Boolean::New(env, isClosed);
  }
}
