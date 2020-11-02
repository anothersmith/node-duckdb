#ifndef connection_H
#define connection_H

#include <napi.h>
#include "duckdb.hpp"

namespace NodeDuckDB {
  class Connection : public Napi::ObjectWrap<Connection> {
    public:
      static Napi::Object Init(Napi::Env env, Napi::Object exports);
      Connection(const Napi::CallbackInfo& info);

    private:
      static Napi::FunctionReference constructor;
      Napi::Value Execute(const Napi::CallbackInfo& info);
      Napi::Value Close(const Napi::CallbackInfo& info);

      shared_ptr<duckdb::DuckDB> database;
      shared_ptr<duckdb::Connection> connection;
  };
}
#endif
