#ifndef CONNECTION_WRAPPER_H
#define CONNECTION_WRAPPER_H

#include <napi.h>
#include "duckdb.hpp"

class ConnectionWrapper : public Napi::ObjectWrap<ConnectionWrapper> {
  public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    ConnectionWrapper(const Napi::CallbackInfo& info);

  private:
    static Napi::FunctionReference constructor;
    Napi::Value Execute(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);

    shared_ptr<duckdb::DuckDB> database;
    shared_ptr<duckdb::Connection> connection;
};

#endif
