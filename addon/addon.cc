#include <napi.h>
#include "duckdb.h"
#include "connection.h"
#include "result_iterator.h"

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  NodeDuckDB::DuckDB::Init(env, exports);
  NodeDuckDB::Connection::Init(env, exports);
  NodeDuckDB::ResultIterator::Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
