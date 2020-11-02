#ifndef DATABASE_H
#define DATABASE_H

#include <napi.h>
#include "duckdb.hpp"

class Database : public Napi::ObjectWrap<Database> {
  public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    Database(const Napi::CallbackInfo& info);
    shared_ptr<duckdb::DuckDB> database;
    static Napi::FunctionReference constructor;

  private:
    Napi::Value Close(const Napi::CallbackInfo& info);

};

#endif
