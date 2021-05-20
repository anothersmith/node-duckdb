#include "type-converters.h"
#include "duckdb.h"
#include "duckdb.hpp"

duckdb::string
NodeDuckDB::TypeConverters::convertString(const Napi::Env &env,
                                          const Napi::Object &options,
                                          const std::string propertyName) {
  if (!options.Get(propertyName).IsString()) {
    throw Napi::TypeError::New(env, "Invalid " + propertyName +
                                        ": must be a string");
  }
  return options.Get(propertyName).ToString().Utf8Value();
}

int32_t
NodeDuckDB::TypeConverters::convertNumber(const Napi::Env &env,
                                          const Napi::Object &options,
                                          const std::string propertyName) {
  if (!options.Get(propertyName).IsNumber()) {
    throw Napi::TypeError::New(env, "Invalid " + propertyName +
                                        ": must be a number");
  }
  return options.Get(propertyName).ToNumber().Int32Value();
}

bool NodeDuckDB::TypeConverters::convertBoolean(
    const Napi::Env &env, const Napi::Object &options,
    const std::string propertyName) {
  if (!options.Get(propertyName).IsBoolean()) {
    throw Napi::TypeError::New(env, "Invalid " + propertyName +
                                        ": must be a boolean");
  }
  return options.Get(propertyName).ToBoolean().Value();
}

int32_t NodeDuckDB::TypeConverters::convertEnum(const Napi::Env &env,
                                                const Napi::Object &options,
                                                const std::string propertyName,
                                                const int min, const int max) {
  const std::string errorMessage =
      "Invalid " + propertyName + ": must be of appropriate enum type";
  if (!options.Get(propertyName).IsNumber()) {
    throw Napi::TypeError::New(env, errorMessage);
  }
  auto value = options.Get(propertyName).ToNumber().Int32Value();
  if (value < min || value > max) {
    throw Napi::TypeError::New(env, errorMessage);
  }
  return value;
}

void NodeDuckDB::TypeConverters::setDBConfig(const Napi::Env &env,
                                             const Napi::Object &config,
                                             duckdb::DBConfig &nativeConfig) {
  if (!config.Get("options").IsObject()) {
    throw Napi::TypeError::New(env, "Invalid options: must be an object");
  }
  auto optionsObject = config.Get("options").ToObject();

  if (!optionsObject.Get("accessMode").IsUndefined()) {
    nativeConfig.access_mode = static_cast<duckdb::AccessMode>(
        convertEnum(env, optionsObject, "accessMode",
                    static_cast<int>(duckdb::AccessMode::UNDEFINED),
                    static_cast<int>(duckdb::AccessMode::READ_WRITE)));
  }

  if (!optionsObject.Get("checkPointWALSize").IsUndefined()) {
    nativeConfig.checkpoint_wal_size =
        convertNumber(env, optionsObject, "checkPointWALSize");
  }

  if (!optionsObject.Get("maximumMemory").IsUndefined()) {
    nativeConfig.maximum_memory =
        convertNumber(env, optionsObject, "maximumMemory");
  }

  if (!optionsObject.Get("useTemporaryDirectory").IsUndefined()) {
    nativeConfig.use_temporary_directory =
        convertBoolean(env, optionsObject, "useTemporaryDirectory");
  }

  if (!optionsObject.Get("temporaryDirectory").IsUndefined()) {
    nativeConfig.temporary_directory =
        convertString(env, optionsObject, "temporaryDirectory");
  }

  if (!optionsObject.Get("collation").IsUndefined()) {
    nativeConfig.collation = convertString(env, optionsObject, "collation");
  }

  if (!optionsObject.Get("defaultOrderType").IsUndefined()) {
    nativeConfig.default_order_type = static_cast<duckdb::OrderType>(
        convertEnum(env, optionsObject, "defaultOrderType",
                    static_cast<int>(duckdb::OrderType::INVALID),
                    static_cast<int>(duckdb::OrderType::DESCENDING)));
  }

  if (!optionsObject.Get("defaultNullOrder").IsUndefined()) {
    nativeConfig.default_null_order = static_cast<duckdb::OrderByNullType>(
        convertEnum(env, optionsObject, "defaultNullOrder",
                    static_cast<int>(duckdb::OrderByNullType::INVALID),
                    static_cast<int>(duckdb::OrderByNullType::NULLS_LAST)));
  }

}
