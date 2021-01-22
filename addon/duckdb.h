#ifndef DUCKDB_H
#define DUCKDB_H

#include "duckdb.hpp"
#include <napi.h>

namespace NodeDuckDB {
class DuckDB : public Napi::ObjectWrap<DuckDB> {
public:
  static Napi::Object Init(Napi::Env env, Napi::Object exports);
  DuckDB(const Napi::CallbackInfo &info);
  shared_ptr<duckdb::DuckDB> database;
  static Napi::FunctionReference constructor;
  bool IsClosed(void);

private:
  Napi::Reference<Napi::Object> file_system_object;
  Napi::FunctionReference read_with_location_callback_ref;
  Napi::FunctionReference read_callback_ref;
  Napi::FunctionReference glob_callback_ref;
  Napi::ThreadSafeFunction read_with_location_callback_tsfn;
  Napi::ThreadSafeFunction read_tsfn;
  Napi::ThreadSafeFunction glob_tsfn;
  Napi::Value Close(const Napi::CallbackInfo &info);
  Napi::Value IsClosed(const Napi::CallbackInfo &info);
  Napi::Value GetAccessMode(const Napi::CallbackInfo &info);
  Napi::Value GetCheckPointWALSize(const Napi::CallbackInfo &info);
  Napi::Value GetUseDirectIO(const Napi::CallbackInfo &info);
  Napi::Value GetMaximumMemory(const Napi::CallbackInfo &info);
  Napi::Value GetUseTemporaryDirectory(const Napi::CallbackInfo &info);
  Napi::Value GetTemporaryDirectory(const Napi::CallbackInfo &info);
  Napi::Value GetCollation(const Napi::CallbackInfo &info);
  Napi::Value GetDefaultOrderType(const Napi::CallbackInfo &info);
  Napi::Value GetDefaultNullOrder(const Napi::CallbackInfo &info);
  Napi::Value GetEnableCopy(const Napi::CallbackInfo &info);
};
} // namespace NodeDuckDB

#endif
