#include <napi.h>
#include "connection_wrapper.h"
#include "result_wrapper.h"
#include <chrono>
#include <thread>
#include <iostream>
#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "parquet-extension.hpp"

using namespace Napi;
using namespace std;

std::thread nativeThread;
ThreadSafeFunction tsfn;

Value Start( const CallbackInfo& info )
{
  cout << "Streaming!" << endl;
  Napi::Env env = info.Env();

  if ( info.Length() < 2 )
  {
    throw TypeError::New( env, "Expected two arguments" );
  }
  else if ( !info[0].IsFunction() )
  {
    throw TypeError::New( env, "Expected first arg to be function" );
  }
  else if ( !info[1].IsNumber() )
  {
    throw TypeError::New( env, "Expected second arg to be number" );
  }

  int count = info[1].As<Number>().Int32Value();

  // Create a ThreadSafeFunction
  tsfn = ThreadSafeFunction::New(
      env,
      info[0].As<Function>(),  // JavaScript function called asynchronously
      "Resource Name",         // Name
      0,                       // Unlimited queue
      1,                       // Only one thread will use this initially
      []( Napi::Env ) {        // Finalizer used to clean threads up
        nativeThread.join();
      } );

  // Create a native thread
  nativeThread = std::thread( [count] {
    auto callback = []( Napi::Env env, Function jsCallback, int* value ) {
      
      bool read_only = false;
      string database_name = "";

      duckdb::DBConfig config;
      if (read_only)
        config.access_mode = duckdb::AccessMode::READ_ONLY;

      unique_ptr<duckdb::DuckDB> database;
      unique_ptr<duckdb::Connection> connection;
      database = duckdb::make_unique<duckdb::DuckDB>(database_name, &config);
      database->LoadExtension<duckdb::ParquetExtension>();
      connection = duckdb::make_unique<duckdb::Connection>(*database);

      string query = "SELECT count(*) FROM read_csv_auto('src/addon-wrapper/test-fixtures/web_page.csv')";

      auto prep = connection->Prepare(query);
      if (!prep->success) {
        Napi::Error::New(env, prep->error).ThrowAsJavaScriptException();
        return env.Undefined();
      }


      vector<duckdb::Value> args; // TODO: take arguments
      unique_ptr<duckdb::QueryResult>  result = prep->Execute(args, true);

      unique_ptr<duckdb::DataChunk> current_chunk = result->Fetch();

      typedef uint64_t idx_t;
      idx_t col_count = result->types.size();
        Napi::Array row = Napi::Array::New(env, col_count);

        for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
          auto &nullmask = duckdb::FlatVector::Nullmask(current_chunk->data[col_idx]);
          if (nullmask[0]) {
            row.Set(col_idx, env.Null());
            continue;
          }
          auto val = current_chunk->data[col_idx].GetValue(0);
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


          default:
            throw runtime_error("unsupported type: " + result->types[col_idx].ToString());
          }
        }

      // Transform native data into JS data, passing it to the provided 
      // `jsCallback` -- the TSFN's JavaScript function.
      jsCallback.Call( {row} );
      
      // We're finished with the data.
      delete value;
    };

    for ( int i = 0; i < count; i++ )
    {
      // Create new data
      int* value = new int( clock() );

      // Perform a blocking call
      napi_status status = tsfn.BlockingCall( value, callback );
      if ( status != napi_ok )
      {
        // Handle error
        break;
      }

      std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
    }

    // Release the thread-safe function
    tsfn.Release();
  } );

  return Boolean::New(env, true);
}

Napi::Object Init( Napi::Env env, Object exports )
{
  exports.Set( "start", Function::New( env, Start ) );
  return exports;
}


Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ConnectionWrapper::Init(env, exports);
  ResultWrapper::Init(env, exports);
  Init(env, exports);
  return exports;
}

NODE_API_MODULE(addon, InitAll)
