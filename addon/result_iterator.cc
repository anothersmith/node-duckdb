#include <iostream>
#include "result_iterator.h"
#include "duckdb.hpp"
using namespace std;

namespace NodeDuckDB {
  Napi::FunctionReference ResultIterator::constructor;

  Napi::Object ResultIterator::Init(Napi::Env env, Napi::Object exports) {
    Napi::Function func =
        DefineClass(env,
                    "ResultIterator",
                    {
                      InstanceMethod("fetchRow", &ResultIterator::FetchRow),
                      InstanceMethod("describe", &ResultIterator::Describe),
                      InstanceMethod("close", &ResultIterator::Close),
                      InstanceAccessor<&ResultIterator::GetType>("type"),
                      InstanceAccessor<&ResultIterator::IsClosed>("isClosed")
                    });

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("ResultIterator", func);
    return exports;
  }

  ResultIterator::ResultIterator(const Napi::CallbackInfo& info) : Napi::ObjectWrap<ResultIterator>(info) {
    Napi::Env env = info.Env();
  }

  Napi::Object ResultIterator::Create() {
    return constructor.New({});
  }

  typedef uint64_t idx_t;

  int32_t GetDate(int64_t timestamp) {
    return (int32_t)(((int64_t)timestamp) >> 32);
  }

  int32_t GetTime(int64_t timestamp) {
    return (int32_t)(timestamp & 0xFFFFFFFF);
  }

  #define EPOCH_DATE 719528
  #define SECONDS_PER_DAY (60 * 60 * 24)

  int64_t Epoch(int32_t date) {
    return ((int64_t)date - EPOCH_DATE) * SECONDS_PER_DAY;
  }

  Napi::Value ResultIterator::FetchRow(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!result) {
      Napi::RangeError::New(env, "Result closed").ThrowAsJavaScriptException();
      return env.Undefined();
    }  
    if (!current_chunk || chunk_offset >= current_chunk->size()) {
      current_chunk = result->Fetch();
      chunk_offset = 0;
    }
    if (!current_chunk) {
      Napi::Error::New(env, "No data has been returned (possibly stream has been closed: only one stream can be active on one connection at a time)").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    if (current_chunk->size() == 0) {
      return env.Null();
    }
    idx_t col_count = result->types.size();
    Napi::Array row = Napi::Array::New(env, col_count);

    for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
      auto &nullmask = duckdb::FlatVector::Nullmask(current_chunk->data[col_idx]);
      if (nullmask[chunk_offset]) {
        row.Set(col_idx, env.Null());
        continue;
      }
      auto val = current_chunk->data[col_idx].GetValue(chunk_offset);
      switch (result->types[col_idx].id()) {
      case duckdb::LogicalTypeId::BOOLEAN:
        row.Set(col_idx, Napi::Boolean::New(env, val.GetValue<bool>()));
        break;
      case duckdb::LogicalTypeId::TINYINT:
        row.Set(col_idx, Napi::Number::New(env, val.GetValue<int8_t>()));
        break;
      case duckdb::LogicalTypeId::SMALLINT:
        row.Set(col_idx, Napi::Number::New(env, val.GetValue<int16_t>()));
        break;
      case duckdb::LogicalTypeId::INTEGER:
        row.Set(col_idx, Napi::Number::New(env, val.GetValue<int32_t>()));
        break;
      case duckdb::LogicalTypeId::BIGINT:
        #ifdef NAPI_EXPERIMENTAL
          row.Set(col_idx, Napi::BigInt::New(env, val.GetValue<int64_t>()));
        #else
          row.Set(col_idx, Napi::Number::New(env, val.GetValue<int64_t>()));
        #endif
        break;
      case duckdb::LogicalTypeId::FLOAT:
        row.Set(col_idx, Napi::Number::New(env, val.GetValue<float>()));
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        row.Set(col_idx, Napi::Number::New(env, val.GetValue<double>()));
        break;
      case duckdb::LogicalTypeId::DECIMAL:
        row.Set(col_idx, Napi::Number::New(env, val.CastAs(duckdb::LogicalType::DOUBLE).GetValue<double>()));
        break;      
      case duckdb::LogicalTypeId::VARCHAR:
        row.Set(col_idx, Napi::String::New(env, val.GetValue<string>()));
        break;

      case duckdb::LogicalTypeId::TIMESTAMP: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT64) {
          throw runtime_error("expected int64 for timestamp");
        }
        int64_t tval = val.GetValue<int64_t>();
        int64_t date = Epoch(GetDate(tval)) * 1000;
        int32_t time = GetTime(tval);
        row.Set(col_idx, Napi::Number::New(env, date + time));
        break;
      }
      case duckdb::LogicalTypeId::DATE: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT32) {
          throw runtime_error("expected int32 for date");
        }
        row.Set(col_idx, Napi::Number::New(env, Epoch(val.GetValue<int32_t>()) * 1000));
        break;
      }
      case duckdb::LogicalTypeId::TIME: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT32) {
          throw runtime_error("expected int32 for time");
        }
        int64_t tval = val.GetValue<int64_t>();      
        row.Set(col_idx, Napi::Number::New(env, GetTime(tval)));
        break;
      }

      default:
        throw runtime_error("unsupported type: " + result->types[col_idx].ToString());
      }
    }
    chunk_offset++;
    return row;
  }

  Napi::Value ResultIterator::Describe(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    if (!result) {
      Napi::TypeError::New(env, "Result closed").ThrowAsJavaScriptException();
      return env.Undefined();
    }
    idx_t col_count = result->names.size();
    Napi::Array row = Napi::Array::New(env, col_count);
    idx_t name_idx = 0;
    idx_t type_idx = 1;
    for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
      Napi::Array col = Napi::Array::New(env, 2);
      col.Set(name_idx, Napi::String::New(env, result->names[col_idx]));
      col.Set(type_idx, Napi::String::New(env, result->types[col_idx].ToString()));
      row.Set(col_idx, col);
    }
    return row;
  }

  Napi::Value ResultIterator::GetType(const Napi::CallbackInfo &info) {
      Napi::Env env = info.Env();
      string type = this->result->type == duckdb::QueryResultType::STREAM_RESULT ? "Streaming" : "Materialized";
      return Napi::String::New(env, type);
  }

  Napi::Value ResultIterator::IsClosed(const Napi::CallbackInfo &info) {
      Napi::Env env = info.Env();
      bool isClosed = this->result == nullptr;
      return Napi::Boolean::New(env, isClosed);
  }
}