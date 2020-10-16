#include <napi.h>
#include "connection_wrapper.h"
#include "result_wrapper.h"
#include <chrono>
#include <thread>
#include <iostream>

using namespace Napi;

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
      // Transform native data into JS data, passing it to the provided 
      // `jsCallback` -- the TSFN's JavaScript function.
      jsCallback.Call( {Number::New( env, *value )} );
      
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
