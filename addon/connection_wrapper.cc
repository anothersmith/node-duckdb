#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "duckdb.hpp"
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
  // TODO: database->LoadExtension<duckdb::ParquetExtension>();
  connection = duckdb::make_unique<duckdb::Connection>(*database);
}

Napi::Value ConnectionWrapper::Execute(const Napi::CallbackInfo& info) {
  Napi::Env env = info.Env();
  if (!info[0].IsString()) {
    Napi::TypeError::New(env, "String expected").ThrowAsJavaScriptException();
    return env.Undefined();
  }

  string query = info[0].ToString();

  auto prep = connection->Prepare(query);
  if (!prep->success) {
    Napi::Error::New(env, prep->error).ThrowAsJavaScriptException();
    return env.Undefined();
  }

  Napi::Object result_value = ResultWrapper::Create(info);
  ResultWrapper* result = ResultWrapper::Unwrap(result_value);

  vector<duckdb::Value> args; // TODO: take arguments
  result->result = prep->Execute(args, true);

  return result_value;
}

Napi::Value ConnectionWrapper::Close(const Napi::CallbackInfo& info) {
  connection = nullptr;
  database = nullptr;

  return info.Env().Undefined();
}
