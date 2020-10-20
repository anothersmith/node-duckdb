#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <node_api.h>

using namespace std;


// Limit ourselves to this many primes, starting at 2
#define PRIME_COUNT 10000
#define CHECK(expr) \
  { \
    if ((expr) == 0) { \
      fprintf(stderr, "%s:%d: failed assertion `%s'\n", __FILE__, __LINE__, #expr); \
      fflush(stderr); \
      abort(); \
    } \
  }

typedef struct linked_box_s linked_box_t;
struct linked_box_s {
  linked_box_t* prev;
  int the_prime;
};

typedef struct {
  napi_async_work work;
  napi_deferred deferred;
  int data[PRIME_COUNT];
} AddonData;

// This function runs on a worker thread. It has no access to the JavaScript.
static void ExecuteWork(napi_env env, void* data) {
  AddonData* addon_data = (AddonData*)data;
  int idx_inner, idx_outer;
  int prime_count = 0;

  // Find the first 1000 prime numbers using an extremely inefficient algorithm.
  for (idx_outer = 2; prime_count < PRIME_COUNT; idx_outer++) {
    for (idx_inner = 2; idx_inner < idx_outer; idx_inner++) {
      if (idx_outer % idx_inner == 0) {
        break;
      }
    }
    if (idx_inner < idx_outer) {
      continue;
    }

    // Save the prime number to the heap. The JavaScript marshaller (CallJs)
    // will free this item after having sent it to JavaScript.
    addon_data->data[prime_count] = idx_outer;
    ++prime_count;
  }
}

// This function runs on the main thread after `ExecuteWork` exits.
static void WorkComplete(napi_env env, napi_status status, void* data) {
  if (status != napi_ok) {
    return;
  }

  AddonData* addon_data = (AddonData*)data;
  napi_value array;
  CHECK(napi_create_array(env, &array) == napi_ok);

  for (uint32_t idx = 0; idx < PRIME_COUNT; idx++) {
    napi_value js_the_prime;

    // Convert the integer to a napi_value.
    CHECK(napi_create_int32(env, addon_data->data[idx], &js_the_prime) == napi_ok);

    // Store the js value in the array.
    CHECK(napi_set_element(env, array, idx, js_the_prime) == napi_ok);
  }
  CHECK(napi_resolve_deferred(env, addon_data->deferred, array) == napi_ok);

  // Clean up the work item associated with this run.
  CHECK(napi_delete_async_work(env, addon_data->work) == napi_ok);

  // Set both values to NULL so JavaScript can order a new run of the thread.
  addon_data->work = NULL;
  addon_data->deferred = NULL;
}

// Create a deferred promise and an async queue work item.
static napi_value StartWork(napi_env env, napi_callback_info info) {
  napi_value work_name, promise;
  AddonData* addon_data;

  // Retrieve the per-addon data.
  CHECK(napi_get_cb_info(env,
                          info,
                          NULL,
                          NULL,
                          NULL,
                          (void**)(&addon_data)) == napi_ok);

  // Ensure that no work is currently in progress.
  // CHECK(addon_data->work == NULL && "Only one work item must exist at a time");

  // Create a string to describe this asynchronous operation.
  CHECK(napi_create_string_utf8(env,
                                 "N-API Deferred Promise from Async Work Item",
                                 NAPI_AUTO_LENGTH,
                                 &work_name) == napi_ok);

  // Create a deferred promise which we will resolve at the completion of the work.
  CHECK(napi_create_promise(env,
                             &(addon_data->deferred),
                             &promise) == napi_ok);

  // Create an async work item, passing in the addon data, which will give the
  // worker thread access to the above-created deferred promise.
  CHECK(napi_create_async_work(env,
                                NULL,
                                work_name,
                                ExecuteWork,
                                WorkComplete,
                                addon_data,
                                &(addon_data->work)) == napi_ok);

  // Queue the work item for execution.
  CHECK(napi_queue_async_work(env, addon_data->work) == napi_ok);

  // This causes created `promise` to be returned to JavaScript.
  return promise;
}

// Free the per-addon-instance data.
static void addon_getting_unloaded(napi_env env, void* data, void* hint) {
  AddonData* addon_data = (AddonData*)data;
  CHECK(addon_data->work == NULL &&
      "No work item in progress at module unload");
  free(addon_data);
}


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

    // Define addon-level data associated with this instance of the addon.
  AddonData* addon_data = (AddonData*)malloc(sizeof(*addon_data));
  addon_data->work = NULL;

  // Define the properties that will be set on exports.
  napi_property_descriptor start_work = {
    "startWork",
    NULL,
    StartWork,
    NULL,
    NULL,
    NULL,
    napi_default,
    addon_data
  };

  // Decorate exports with the above-defined properties.
  CHECK(napi_define_properties(env, exports, 1, &start_work) == napi_ok);

  // Associate the addon data with the exports object, to make sure that when
  // the addon gets unloaded our data gets freed.
  CHECK(napi_wrap(env,
                   exports,
                   addon_data,
                   addon_getting_unloaded,
                   NULL,
                   NULL) == napi_ok);

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
