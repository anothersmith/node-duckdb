#include "async_executor.h"
#include "duckdb.hpp"
#include "result_iterator.h"
#include <iostream>
#include <napi.h>
#include <string>

using namespace duckdb;

namespace NodeDuckDB {
AsyncExecutor::AsyncExecutor(
    Napi::Env &env, std::string &query,
    std::shared_ptr<duckdb::Connection> &connection,
    Napi::Promise::Deferred &deferred, bool forceMaterialized,
    ResultFormat &rowResultFormat,
    std::shared_ptr<std::vector<ResultIterator *>> results)
    : Napi::AsyncWorker(env), query(query), connection(connection),
      deferred(deferred), forceMaterialized(forceMaterialized),
      rowResultFormat(rowResultFormat), results(std::move(results)) {}

AsyncExecutor::~AsyncExecutor() {}

void AsyncExecutor::Execute() {
  try {
    if (forceMaterialized) {
      result = connection->Query(query);
    } else {
      result = connection->SendQuery(query);
    }
    if (!result.get()->success) {
      SetError(result.get()->error);
    }
  } catch (...) {
    SetError("Unknown Error: Something happened during execution of the query");
  }
}

void AsyncExecutor::OnOK() {
  Napi::HandleScope scope(Env());
  Napi::Object result_iterator = ResultIterator::Create();
  ResultIterator *result_unwrapped = ResultIterator::Unwrap(result_iterator);
  result_unwrapped->result = std::move(result);
  result_unwrapped->rowResultFormat = rowResultFormat;
  results->push_back(result_unwrapped);
  deferred.Resolve(result_iterator);
}

void AsyncExecutor::OnError(const Napi::Error &e) {
  deferred.Reject(e.Value());
}
} // namespace NodeDuckDB
