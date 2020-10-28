#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include "async_worker.h"
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

  bool read_only = false;
  string database_name = "";

  duckdb::DBConfig config;
  if (read_only)
    config.access_mode = duckdb::AccessMode::READ_ONLY;

  database = duckdb::make_unique<duckdb::DuckDB>(database_name, &config);
  database->LoadExtension<duckdb::ParquetExtension>();
  connection = duckdb::make_shared<duckdb::Connection>(*database);
}

Napi::Value ConnectionWrapper::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  Napi::Promise::Deferred deferred = Napi::Promise::Deferred::New(env);
  try {
    if (!info[0].IsString()) {
      throw Napi::TypeError::New(env, "String expected");
    }
    string query = info[0].ToString();
    AsyncExecutor* wk = new AsyncExecutor(env, query, connection, deferred);
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
