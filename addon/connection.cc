#include "connection.h"
#include "result_iterator.h"
#include "database.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_executor.h"
#include <iostream>
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
                    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("Connection", func);
    return exports;
  }

  Connection::Connection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Connection>(info) {
    Napi::Env env = info.Env();

    if (!info[0].IsObject() || !info[0].ToObject().InstanceOf(Database::constructor.Value())) {
      throw Napi::TypeError::New(env, "Must provide a valid Database object");
    }

    bool read_only = false;
    string database_name = "";

    duckdb::DBConfig config;
    if (read_only)
      config.access_mode = duckdb::AccessMode::READ_ONLY;
    
    auto unwrappedDb = Database::Unwrap(info[0].ToObject());
    connection = duckdb::make_shared<duckdb::Connection>(*unwrappedDb->database);
  }

  Napi::Value Connection::Execute(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
    try {
      if (!info[0].IsString()) {
        throw Napi::TypeError::New(env, "First argument must be a string");
      }

      if (!info[1].IsUndefined() && !info[1].IsBoolean()) {
        throw Napi::TypeError::New(env, "Second argument is an optional boolean");
      }
      string query = info[0].ToString();
      bool forceMaterialized = info[1].IsEmpty() ? false : info[1].ToBoolean().Value();
      AsyncExecutor* wk = new AsyncExecutor(env, query, connection, deferred, forceMaterialized);
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
}
