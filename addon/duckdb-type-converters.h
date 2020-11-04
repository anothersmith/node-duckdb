#include "duckdb.h"
#include "duckdb.hpp"

namespace NodeDuckDB
{
    namespace TypeConverters
    {
        string convertString(const Napi::Env &env, const Napi::Object &options, const string propertyName);
        int32_t convertNumber(const Napi::Env &env, const Napi::Object &options, const string propertyName);
        bool convertBoolean(const Napi::Env &env, const Napi::Object &options, const string propertyName);
        int32_t convertEnum(const Napi::Env &env, const Napi::Object &options, const string propertyName, const int min, const int max);
        void setDBConfig(const Napi::Env &env, const Napi::Object &config, duckdb::DBConfig &nativeConfig);
    }
}
