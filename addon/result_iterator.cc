#include "result_iterator.h"
#include "duckdb.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include <iostream>
#include <string.h>
using namespace std;

namespace NodeDuckDB {
Napi::FunctionReference ResultIterator::constructor;

Napi::Object ResultIterator::Init(Napi::Env env, Napi::Object exports) {
  Napi::Function func =
      DefineClass(env, "ResultIterator",
                  {InstanceMethod("fetchRow", &ResultIterator::FetchRow),
                   InstanceMethod("describe", &ResultIterator::Describe),
                   InstanceMethod("close", &ResultIterator::Close),
                   InstanceAccessor<&ResultIterator::GetType>("type"),
                   InstanceAccessor<&ResultIterator::IsClosed>("isClosed")});

  constructor = Napi::Persistent(func);
  constructor.SuppressDestruct();

  exports.Set("ResultIterator", func);
  return exports;
}

ResultIterator::ResultIterator(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<ResultIterator>(info) {
  Napi::Env env = info.Env();
}

Napi::Object ResultIterator::Create() { return constructor.New({}); }

typedef uint64_t idx_t;

int64_t GetDate(int64_t timestamp) { return timestamp; }

int64_t GetTime(int64_t timestamp) {
  return (int64_t)(timestamp & 0xFFFFFFFFFFFFFFFF);
}

#define EPOCH_DATE 719528
#define SECONDS_PER_DAY (60 * 60 * 24)

int64_t Epoch(int64_t date) {
  return ((int64_t)date - EPOCH_DATE) * SECONDS_PER_DAY;
}

Napi::Value ResultIterator::FetchRow(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  if (!result) {
    Napi::RangeError::New(env, "Result closed").ThrowAsJavaScriptException();
    return env.Undefined();
  }
  if (!current_chunk || chunk_offset >= current_chunk->size()) {
    try {
      current_chunk = result->Fetch();
    } catch (const duckdb::InvalidInputException &e) {
      if (strncmp(e.what(),
                  "Invalid Input Error: Attempting to fetch from an "
                  "unsuccessful or closed streaming query result",
                  50) == 0) {
        Napi::Error::New(
            env, "Attempting to fetch from an unsuccessful or closed streaming "
                 "query result: only "
                 "one stream can be active on one connection at a time)")
            .ThrowAsJavaScriptException();
        return env.Undefined();
      }
      throw e;
    }
    chunk_offset = 0;
  }
  if (!current_chunk || current_chunk->size() == 0) {
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

Napi::Value ResultIterator::Describe(const Napi::CallbackInfo &info) {
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
    col.Set(type_idx,
            Napi::String::New(env, result->types[col_idx].ToString()));
    row.Set(col_idx, col);
  }
  return row;
}

Napi::Value ResultIterator::GetType(const Napi::CallbackInfo &info) {
  Napi::Env env = info.Env();
  string type = this->result->type == duckdb::QueryResultType::STREAM_RESULT
                    ? "Streaming"
                    : "Materialized";
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
  auto val = current_chunk->data[col_idx].GetValue(chunk_offset);

  if (val.is_null) {
    return env.Null();
  }

  switch (result->types[col_idx].id()) {
  case duckdb::LogicalTypeId::BOOLEAN:
    return Napi::Boolean::New(env, val.GetValue<bool>());
  case duckdb::LogicalTypeId::TINYINT:
    return Napi::Number::New(env, val.GetValue<int8_t>());
  case duckdb::LogicalTypeId::SMALLINT:
    return Napi::Number::New(env, val.GetValue<int16_t>());
  case duckdb::LogicalTypeId::INTEGER:
    return Napi::Number::New(env, val.GetValue<int32_t>());
  case duckdb::LogicalTypeId::BIGINT:
    return Napi::BigInt::New(env, val.GetValue<int64_t>());
  case duckdb::LogicalTypeId::HUGEINT: {
    // hugeint_t represents a signed 128 bit integer in two's complement
    // notation napi's BigInt is basically a regular signed integer (MSB) so we
    // want to make sure we pass the absolute value of the huge int into napi
    // plus the sign bit
    auto huge_int = val.GetValue<duckdb::hugeint_t>();
    int is_negative = huge_int.upper < 0;
    duckdb::hugeint_t positive_huge_int =
        is_negative ? huge_int * duckdb::hugeint_t(-1) : huge_int;
    uint64_t arr[2]{positive_huge_int.lower, (uint64_t)positive_huge_int.upper};
    return Napi::BigInt::New(env, is_negative, 2, &arr[0]);
  }
  case duckdb::LogicalTypeId::FLOAT:
    return Napi::Number::New(env, val.GetValue<float>());
  case duckdb::LogicalTypeId::DOUBLE:
    return Napi::Number::New(env, val.GetValue<double>());
  case duckdb::LogicalTypeId::DECIMAL:
    return Napi::Number::New(
        env, val.CastAs(duckdb::LogicalType::DOUBLE).GetValue<double>());
  case duckdb::LogicalTypeId::VARCHAR:
    return Napi::String::New(env, val.GetValue<string>());
  case duckdb::LogicalTypeId::BLOB: {
    int array_length = val.str_value.length();
    char char_array[array_length + 1];
    // TODO: multiple copies, improve
    strcpy(char_array, val.str_value.c_str());
    return Napi::Buffer<char>::Copy(env, char_array, array_length);
  }
  case duckdb::LogicalTypeId::TIMESTAMP: {
    if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT64) {
      throw runtime_error("expected int64 for timestamp");
    }
    int64_t tval = val.GetValue<int64_t>();
    return Napi::Number::New(env, tval / 1000);
  }
  case duckdb::LogicalTypeId::TIME: {
    if (result->types[col_idx].InternalType() != duckdb::PhysicalType::INT64) {
      throw runtime_error("expected int64 for time");
    }
    int64_t tval = val.GetValue<int64_t>();
    return Napi::Number::New(env, GetTime(tval));
  }
  case duckdb::LogicalTypeId::INTERVAL: {
    return Napi::String::New(env, val.ToString());
  }
  default:
    // default to getting string representation
    return Napi::String::New(env, val.ToString());
  }
}
Napi::Value ResultIterator::Close(const Napi::CallbackInfo &info) {
  result.reset();
  return info.Env().Undefined();
}
void ResultIterator::close() { result.reset(); }
} // namespace NodeDuckDB
