#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "database.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_worker.h"
#include <iostream>
using namespace std;

Napi::FunctionReference ConnectionWrapper::constructor;

Napi::Object ConnectionWrapper::Init(Napi::Env env, Napi::Object exports) {

  Napi::Function func =
      DefineClass(env,
                  "ConnectionWrapper",
                  {
                    InstanceMethod("execute", &ConnectionWrapper::Execute),
                    InstanceMethod("close", &ConnectionWrapper::Close),
                  });

  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();

  exports.Set("ConnectionWrapper", func);
  return exports;
}

ConnectionWrapper::ConnectionWrapper(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ConnectionWrapper>(info) {
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

Napi::Value ConnectionWrapper::Execute(const Napi::CallbackInfo& info) {
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

Napi::Value ConnectionWrapper::Close(const Napi::CallbackInfo& info) {
  connection = nullptr;
  database = nullptr;

  return info.Env().Undefined();
}
