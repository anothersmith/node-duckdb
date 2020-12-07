#include "connection.h"
#include "duckdb.h"
#include "result_iterator.h"
#include <napi.h>

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  NodeDuckDB::DuckDB::Init(env, exports);
  NodeDuckDB::Connection::Init(env, exports);
  NodeDuckDB::ResultIterator::Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
