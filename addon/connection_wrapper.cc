#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include <thread>
#include <napi.h>
#include <iostream>

using namespace std;
using namespace Napi;

Napi::FunctionReference ConnectionWrapper::constructor;
std::thread nativeThread;
ThreadSafeFunction tsfn;

Napi::Object ConnectionWrapper::Init(Napi::Env env, Napi::Object exports)
{

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

ConnectionWrapper::ConnectionWrapper(const Napi::CallbackInfo &info) : Napi::ObjectWrap<ConnectionWrapper>(info)
{
  cout << "xxxxxxx!" << endl;
  Napi::Env env = info.Env();

  bool read_only = false;
  string database_name = "";

  duckdb::DBConfig config;
  if (read_only)
    config.access_mode = duckdb::AccessMode::READ_ONLY;

  database = duckdb::make_unique<duckdb::DuckDB>(database_name, &config);
  database->LoadExtension<duckdb::ParquetExtension>();
  connection = duckdb::make_unique<duckdb::Connection>(*database);
}

void ConnectionWrapper::Execute(const Napi::CallbackInfo &info)
{
  cout << "fdsfdsafdsdfs!" << endl;
  Napi::Env env = info.Env();
  if (!info[0].IsString())
  {
    throw Napi::TypeError::New(env, "String expected");
  }

  if (!info[1].IsFunction())
  {
    throw Napi::TypeError::New(env, "Expected second arg to be function");
  }

  string query = info[0].ToString();

  // Create a ThreadSafeFunction
  tsfn = Napi::ThreadSafeFunction::New(
      env,
      info[1].As<Function>(), // JavaScript function called asynchronously
      "Resource Name",        // Name
      0,                      // Unlimited queue
      1,                      // Only one thread will use this initially
      [](Napi::Env) {         // Finalizer used to clean threads up
        nativeThread.join();
      });
  nativeThread = std::thread([this, query] {
    auto callback = [this, query](Napi::Env env, Function jsCallback, int *value) {
      cout << "pppppppp" << endl;
      // TODO: should be callback instead of exception probably
      auto prep = connection->Prepare(query);
      if (!prep->success)
      {
        Napi::Error::New(env, prep->error).ThrowAsJavaScriptException();
        return env.Undefined();
      }
      Napi::Object result_value = ResultWrapper::Create();
      ResultWrapper *result = ResultWrapper::Unwrap(result_value);

      vector<duckdb::Value> args; // TODO: take arguments
      result->result = prep->Execute(args, true);

      // Transform native data into JS data, passing it to the provided
      // `jsCallback` -- the TSFN's JavaScript function.
      cout << result_value << endl;
      cout << jsCallback << endl;
      jsCallback.Call({result_value});

      // We're finished with the data.
      delete value;
      // return;
    };
    int* value = new int( clock() );
    // Perform a blocking call
    napi_status status = tsfn.BlockingCall( value, callback );

    // Release the thread-safe function
    tsfn.Release();
    return;
  });
};

Napi::Value ConnectionWrapper::Close(const Napi::CallbackInfo &info)
{
  connection = nullptr;
  database = nullptr;

  return info.Env().Undefined();
}
