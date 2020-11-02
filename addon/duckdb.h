#ifndef DUCKDB_H
#define DUCKDB_H

#include <napi.h>
#include "duckdb.hpp"

namespace NodeDuckDB {
  class DuckDB : public Napi::ObjectWrap<DuckDB> {
    public:
      static Napi::Object Init(Napi::Env env, Napi::Object exports);
      DuckDB(const Napi::CallbackInfo& info);
      shared_ptr<duckdb::DuckDB> database;
      static Napi::FunctionReference constructor;
      bool IsClosed(void);

    private:
      Napi::Value Close(const Napi::CallbackInfo& info);
			Napi::Value IsClosed(const Napi::CallbackInfo &info);

  };
}

#endif
