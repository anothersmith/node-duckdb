#include "duckdb.h"
#include "async_executor.h"
#include "connection.h"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "node_filesystem.h"
#include "parquet-extension.hpp"
#include "result_iterator.h"
#include "type-converters.h"
#include <iostream>
using namespace std;

namespace NodeDuckDB {
using namespace TypeConverters;

Napi::FunctionReference DuckDB::constructor;

Napi::Object DuckDB::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func = DefineClass(
      env, "DuckDB",
      {
          InstanceMethod("close", &DuckDB::Close),
          InstanceAccessor<&DuckDB::IsClosed>("isClosed"),
          InstanceAccessor<&DuckDB::GetAccessMode>("accessMode"),
          InstanceAccessor<&DuckDB::GetCheckPointWALSize>("checkPointWALSize"),
          InstanceAccessor<&DuckDB::GetUseDirectIO>("useDirectIO"),
          InstanceAccessor<&DuckDB::GetMaximumMemory>("maximumMemory"),
          InstanceAccessor<&DuckDB::GetUseTemporaryDirectory>(
              "useTemporaryDirectory"),
          InstanceAccessor<&DuckDB::GetTemporaryDirectory>(
              "temporaryDirectory"),
          InstanceAccessor<&DuckDB::GetCollation>("collation"),
          InstanceAccessor<&DuckDB::GetDefaultOrderType>("defaultOrderType"),
          InstanceAccessor<&DuckDB::GetDefaultNullOrder>("defaultNullOrder"),
          InstanceAccessor<&DuckDB::GetEnableCopy>("enableCopy"),
      });
  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();
  exports.Set("DuckDB", func);
  return exports;
}

DuckDB::DuckDB(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<DuckDB>(info) {
  Napi::Env env = info.Env();

  string path;
  duckdb::DBConfig nativeConfig;

  if (!info[0].IsUndefined()) {
    if (!info[0].IsObject()) {
      throw Napi::TypeError::New(env, "Invalid argument: must be an object");
    }
    auto config = info[0].ToObject();

    if (!config.Get("path").IsUndefined()) {
      path = convertString(env, config, "path");
    }

    if (!config.Get("options").IsUndefined()) {
      setDBConfig(env, config, nativeConfig);
    }
  }
  file_system_object = Napi::Persistent(info[1].As<Napi::Object>());
  read_with_location_callback_ref = Napi::Persistent(
      file_system_object.Value().Get("readWithLocation").As<Napi::Function>());
  read_callback_ref = Napi::Persistent(
      file_system_object.Value().Get("read").As<Napi::Function>());
  glob_callback_ref = Napi::Persistent(
      file_system_object.Value().Get("glob").As<Napi::Function>());
  get_file_size_callback_ref = Napi::Persistent(
      file_system_object.Value().Get("getFileSize").As<Napi::Function>());
  open_file_callback_ref = Napi::Persistent(
      file_system_object.Value().Get("openFile").As<Napi::Function>());

  read_with_location_callback_tsfn = Napi::ThreadSafeFunction::New(
      read_with_location_callback_ref.Env(),
      read_with_location_callback_ref.Value(), "Node Filesystem Callback",
      0,              // Unlimited queue
      1,              // Only one thread will use this initially
      [](Napi::Env) { // Finalizer used to clean threads up
        // nativeThread.join();
      });
  read_tsfn = Napi::ThreadSafeFunction::New(
      read_callback_ref.Env(), read_callback_ref.Value(),
      "Node Filesystem Callback",
      0,              // Unlimited queue
      1,              // Only one thread will use this initially
      [](Napi::Env) { // Finalizer used to clean threads up
        // nativeThread.join();
      });
  glob_tsfn = Napi::ThreadSafeFunction::New(
      glob_callback_ref.Env(), glob_callback_ref.Value(),
      "Node Filesystem Callback",
      0,              // Unlimited queue
      1,              // Only one thread will use this initially
      [](Napi::Env) { // Finalizer used to clean threads up
        // nativeThread.join();
      });
  get_file_size_tsfn = Napi::ThreadSafeFunction::New(
      get_file_size_callback_ref.Env(), get_file_size_callback_ref.Value(),
      "Node Filesystem Callback",
      0,              // Unlimited queue
      1,              // Only one thread will use this initially
      [](Napi::Env) { // Finalizer used to clean threads up
        // nativeThread.join();
      });
  open_file_tsfn = Napi::ThreadSafeFunction::New(
      open_file_callback_ref.Env(), open_file_callback_ref.Value(),
      "Node Filesystem Callback",
      0,              // Unlimited queue
      1,              // Only one thread will use this initially
      [](Napi::Env) { // Finalizer used to clean threads up
        // nativeThread.join();
      });

  nativeConfig.file_system = duckdb::make_unique<NodeFileSystem>(
      read_with_location_callback_tsfn, read_tsfn, glob_tsfn,
      get_file_size_tsfn, open_file_tsfn);
  database = duckdb::make_unique<duckdb::DuckDB>(path, &nativeConfig);
  database->LoadExtension<duckdb::ParquetExtension>();
}

Napi::Value DuckDB::Close(const Napi::CallbackInfo &info) {
  database.reset();
  return info.Env().Undefined();
}
Napi::Value DuckDB::IsClosed(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Boolean::New(env, IsClosed());
}

bool DuckDB::IsClosed() { return database == nullptr; }

Napi::Value DuckDB::GetAccessMode(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Number::New(env,
                           static_cast<double>(database->config.access_mode));
}
Napi::Value DuckDB::GetCheckPointWALSize(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Number::New(env, database->config.checkpoint_wal_size);
}
Napi::Value DuckDB::GetUseDirectIO(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Boolean::New(env, database->config.use_direct_io);
}
Napi::Value DuckDB::GetMaximumMemory(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Number::New(env, database->config.maximum_memory);
}
Napi::Value DuckDB::GetUseTemporaryDirectory(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Boolean::New(env, database->config.use_temporary_directory);
}
Napi::Value DuckDB::GetTemporaryDirectory(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::String::New(env, database->config.temporary_directory);
}
Napi::Value DuckDB::GetCollation(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::String::New(env, database->config.collation);
}
Napi::Value DuckDB::GetDefaultOrderType(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Number::New(
      env, static_cast<double>(database->config.default_order_type));
}
Napi::Value DuckDB::GetDefaultNullOrder(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Number::New(
      env, static_cast<double>(database->config.default_null_order));
}
Napi::Value DuckDB::GetEnableCopy(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  return Napi::Boolean::New(env, database->config.enable_copy);
}
} // namespace NodeDuckDB
