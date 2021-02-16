#include "duckdb.hpp"
#include "result_iterator.h"
#include <memory>
#include <napi.h>
#include <string>
#include <thread>

namespace NodeDuckDB {
class AsyncExecutor {
public:
  AsyncExecutor(const Napi::CallbackInfo &info, std::string &query,
                std::shared_ptr<duckdb::Connection> &connection,
                bool forceMaterialized, ResultFormat &rowResultFormat);
  void Execute();

private:
  const Napi::CallbackInfo &info;
  std::string query;
  ResultFormat rowResultFormat;
  std::shared_ptr<duckdb::Connection> connection;
  std::unique_ptr<duckdb::QueryResult> result;
  bool forceMaterialized;
  Napi::ThreadSafeFunction execute_tsfn;
  std::thread nativeThread;
};
} // namespace NodeDuckDB
