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
    Napi::Value row;
    if (rowResultFormat == ResultFormat::OBJECT) {
      row = getRowObject(env);
    } else {
      row = getRowArray(env);
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

  Napi::Value ResultIterator::getRowArray(Napi::Env env) {
    idx_t col_count = result->types.size();
    Napi::Array row = Napi::Array::New(env, col_count);

    for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
      auto cellValue = getCellValue(env, col_idx);
      row.Set(col_idx, cellValue);
    }
    return row;
  }

  Napi::Value ResultIterator::getRowObject(Napi::Env env) {
    idx_t col_count = result->types.size();
    Napi::Object row = Napi::Object::New(env);

    for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
      auto cellValue = getCellValue(env, col_idx);
      row.Set(result->names[col_idx], cellValue);
    }
    return row;
  }

  Napi::Value ResultIterator::getCellValue(Napi::Env env, duckdb::idx_t col_idx) {
      auto &nullmask = duckdb::FlatVector::Nullmask(current_chunk->data[col_idx]);
      if (nullmask[chunk_offset]) {
        return env.Null();
      }

      auto val = current_chunk->data[col_idx].GetValue(chunk_offset);
      switch (result->types[col_idx].id()) {
      case duckdb::LogicalTypeId::BOOLEAN:
        return Napi::Boolean::New(env, val.GetValue<bool>());
      case duckdb::LogicalTypeId::TINYINT:
        return  Napi::Number::New(env, val.GetValue<int8_t>());
      case duckdb::LogicalTypeId::SMALLINT:
        return  Napi::Number::New(env, val.GetValue<int16_t>());
      case duckdb::LogicalTypeId::INTEGER:
        return  Napi::Number::New(env, val.GetValue<int32_t>());
      case duckdb::LogicalTypeId::BIGINT:
        // for now return as string: BigInt is supported by Napi v5+
        return  Napi::String::New(env, val.ToString());
      case duckdb::LogicalTypeId::HUGEINT:
        // for now return as string: BigInt is supported by Napi v5+
        return  Napi::String::New(env, val.ToString());
      case duckdb::LogicalTypeId::FLOAT:
        return  Napi::Number::New(env, val.GetValue<float>());
      case duckdb::LogicalTypeId::DOUBLE:
        return  Napi::Number::New(env, val.GetValue<double>());
      case duckdb::LogicalTypeId::DECIMAL:
        return  Napi::Number::New(env, val.CastAs(duckdb::LogicalType::DOUBLE).GetValue<double>());
      case duckdb::LogicalTypeId::VARCHAR:
        return  Napi::String::New(env, val.GetValue<string>());
      case duckdb::LogicalTypeId::BLOB: {
        int array_length = val.str_value.length();
        char char_array[array_length + 1];
        strcpy(char_array, val.str_value.c_str());
        return  Napi::Buffer<char>::Copy(env, char_array, array_length);
      }
      case duckdb::LogicalTypeId::TIMESTAMP: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT64) {
          throw runtime_error("expected int64 for timestamp");
        }
        int64_t tval = val.GetValue<int64_t>();
        int64_t date = Epoch(GetDate(tval)) * 1000;
        int32_t time = GetTime(tval);
        return  Napi::Number::New(env, date + time);
      }
      case duckdb::LogicalTypeId::DATE: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT32) {
          throw runtime_error("expected int32 for date");
        }
        return  Napi::Number::New(env, Epoch(val.GetValue<int32_t>()) * 1000);
      }
      case duckdb::LogicalTypeId::TIME: {
        if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT32) {
          throw runtime_error("expected int32 for time");
        }
        int64_t tval = val.GetValue<int32_t>();      
        return  Napi::Number::New(env, GetTime(tval));
      }
      case duckdb::LogicalTypeId::INTERVAL: {
        return  Napi::String::New(env, val.ToString());
      }
      default:
        // default to getting string representation
        return Napi::String::New(env, val.ToString());
      }
  }
}
