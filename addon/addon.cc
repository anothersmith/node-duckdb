#include <napi.h>
#include "connection_wrapper.h"
#include "result_wrapper.h"
#include "async_worker.cc"

Napi::Value Test(const Napi::CallbackInfo& info) {
    Napi::Function cb = info[0].As<Napi::Function>();
    AsyncExecutor* wk = new AsyncExecutor(cb);
    wk->Queue();
    return info.Env().Undefined();
}

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
  ConnectionWrapper::Init(env, exports);
  ResultWrapper::Init(env, exports);
    exports.Set(Napi::String::New(env, "test"),
              Napi::Function::New(env, Test));
  return exports;
}

NODE_API_MODULE(addon, InitAll)
