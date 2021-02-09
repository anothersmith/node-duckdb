#include "async_executor.h"
#include "duckdb.hpp"
#include "result_iterator.h"
#include <iostream>
#include <napi.h>
#include <string>

using namespace duckdb;

namespace NodeDuckDB {
AsyncExecutor::AsyncExecutor(
    const Napi::CallbackInfo &info, std::string &query,
    std::shared_ptr<duckdb::Connection> &connection,
    bool forceMaterialized,
    ResultFormat &rowResultFormat)
    : info{info}, query{query}, connection{connection},
      forceMaterialized{forceMaterialized},
      rowResultFormat{rowResultFormat} {}

// AsyncExecutor::~AsyncExecutor() {
//   cout << "herexx" << endl;
//   if (connection) {
//     cout << "heryyx" << endl;
//     connection.reset();
//   }
//   cout << "herezz" << endl;
//   execute_tsfn.Release();
//   cout << "hereqqq" << endl;
// }

void AsyncExecutor::Execute() {
  execute_tsfn = Napi::ThreadSafeFunction::New(
      info.Env(),
      info[1]
          .As<Napi::Function>(), // JavaScript function called asynchronously
      "AsyncExecutor Execute",           // Name
      0,                         // Unlimited queue
      1,                         // Only one thread will use this initially
      [&](Napi::Env) {           // Finalizer used to clean threads up
        nativeThread.join();
      });

  nativeThread = std::thread([&] {
    auto threadsafe_fn_callback = [&](Napi::Env env,
                                      Napi::Function js_callback) {
      Napi::Object result_iterator = ResultIterator::Create();
      ResultIterator *result_unwrapped = ResultIterator::Unwrap(result_iterator);
      result_unwrapped->result = std::move(result);
      result_unwrapped->rowResultFormat = rowResultFormat;
      js_callback.Call(
          {result_iterator});
    };

    try {
      if (forceMaterialized) {
        result = connection->Query(query);
      } else {
        result = connection->SendQuery(query);
      }
      if (!result.get()->success) {
        // SetError(result.get()->error);
      }
      napi_status status = execute_tsfn.BlockingCall(threadsafe_fn_callback);
      execute_tsfn.Release();

    } catch (...) {
      // SetError(
      //     "Unknown Error: Something happened during execution of the query");
    }

  });
}
} // namespace NodeDuckDB
