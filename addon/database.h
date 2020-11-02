#ifndef DATABASE_H
#define DATABASE_H

#include <napi.h>
#include "duckdb.hpp"

namespace NodeDuckDB {
  class Database : public Napi::ObjectWrap<Database> {
    public:
      static Napi::Object Init(Napi::Env env, Napi::Object exports);
      Database(const Napi::CallbackInfo& info);
      shared_ptr<duckdb::DuckDB> database;
      static Napi::FunctionReference constructor;
      bool IsClosed(void);

    private:
      Napi::Value Close(const Napi::CallbackInfo& info);
			Napi::Value IsClosed(const Napi::CallbackInfo &info);

  };
}

#endif
