#include "connection.h"
#include "duckdb.h"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "parquet-extension.hpp"
#include "result_iterator.h"
#include "type-converters.h"
#include <iostream>
#include <memory>
#include <vector>
using namespace std;

namespace NodeDuckDB {
Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {

  Napi::Function func =
      DefineClass(env, "Connection",
                  {InstanceMethod("execute", &Connection::Execute),
                   InstanceMethod("close", &Connection::Close),
                   InstanceAccessor<&Connection::IsClosed>("isClosed")});

  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();

  exports.Set("Connection", func);
  return exports;
}

Connection::Connection(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<Connection>(info) {
  Napi::Env env = info.Env();
  async_executors =
      duckdb::make_unique<std::vector<duckdb::unique_ptr<AsyncExecutor>>>();

  if (!info[0].IsObject() ||
      !info[0].ToObject().InstanceOf(DuckDB::constructor.Value())) {
    throw Napi::TypeError::New(env, "Must provide a valid DuckDB object");
  }

  bool read_only = false;
  string database_name = "";
  // results = std::make_shared<std::vector<ResultIterator *>>();

  duckdb::DBConfig config;
  if (read_only)
    config.access_mode = duckdb::AccessMode::READ_ONLY;

  auto unwrappedDb = DuckDB::Unwrap(info[0].ToObject());
  if (unwrappedDb->IsClosed()) {
    throw Napi::TypeError::New(env, "Database is closed");
  }
  if (!unwrappedDb->IsInitialized()) {
    throw Napi::TypeError::New(env, "Database hasn't been initialized");
  }
  connection = duckdb::make_shared<duckdb::Connection>(*unwrappedDb->database);
}

Napi::Value Connection::Execute(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  try {
    if (!info[0].IsString()) {
      throw Napi::TypeError::New(env, "First argument must be a string");
    }

    if (!info[1].IsFunction()) {
      throw Napi::TypeError::New(env, "Second argument must be a callback");
    }

    if (!info[2].IsUndefined() && !info[2].IsObject()) {
      throw Napi::TypeError::New(env, "Second argument is an optional object");
    }

    if (this->connection == nullptr) {
      throw Napi::TypeError::New(env, "Connection is closed");
    }

    auto query = info[0].ToString().Utf8Value();
    auto forceMaterializedValue = false;
    ResultFormat rowResultFormatValue = ResultFormat::OBJECT;
    if (!info[2].IsUndefined()) {
      auto options = info[2].ToObject();
      if (!options.Get("forceMaterialized").IsUndefined()) {
        forceMaterializedValue =
            TypeConverters::convertBoolean(env, options, "forceMaterialized");
      }

      if (!options.Get("rowResultFormat").IsUndefined()) {
        rowResultFormatValue = static_cast<ResultFormat>(
            TypeConverters::convertEnum(env, options, "rowResultFormat",
                                        static_cast<int>(ResultFormat::OBJECT),
                                        static_cast<int>(ResultFormat::ARRAY)));
      }
    }
    async_executors->push_back(duckdb::make_unique<NodeDuckDB::AsyncExecutor>(
        info, query, connection, forceMaterializedValue, rowResultFormatValue));
    async_executors->back()->Execute();
  } catch (Napi::Error &e) {
    throw e;
  } catch (...) {
    throw Napi::Error::New(env, "Unknown error occured during execution");
  }
  return info.Env().Undefined();
}

Napi::Value Connection::Close(const Napi::CallbackInfo &info) {
  // for (auto &result : *results) {
  //   if (result != nullptr) {
  //     result->close();
  //   }
  // }
  connection.reset();
  database.reset();
  // if (async_executor) {
  //   cout << "444" << endl;
  //   // async_executor.reset();
  // }
  return info.Env().Undefined();
}
Napi::Value Connection::IsClosed(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  bool isClosed = connection == nullptr || connection->context->is_invalidated;
  return Napi::Boolean::New(env, isClosed);
}
} // namespace NodeDuckDB
