#ifndef connection_H
#define connection_H

#include "duckdb.hpp"
// #include "result_iterator.h"
#include <napi.h>
#include <thread>
#include <vector>
#include "async_executor.h"

namespace NodeDuckDB {
class Connection : public Napi::ObjectWrap<Connection> {
public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  Connection(const Napi::CallbackInfo &info);

private:
  static Napi::FunctionReference constructor;
  Napi::Value Execute(const Napi::CallbackInfo &info);
  Napi::Value Close(const Napi::CallbackInfo &info);
  Napi::Value IsClosed(const Napi::CallbackInfo &info);

  shared_ptr<duckdb::DuckDB> database;
  shared_ptr<duckdb::Connection> connection;
  Napi::ThreadSafeFunction execute_tsfn;
  std::thread nativeThread;
  duckdb::unique_ptr<std::vector<duckdb::unique_ptr<NodeDuckDB::AsyncExecutor>>> async_executors;
};
} // namespace NodeDuckDB
#endif
