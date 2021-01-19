#include "node_filesystem.h"
#include "duckdb/common/file_system.hpp"
#include <iostream>
using namespace duckdb;
using namespace std;
using namespace Napi;
namespace NodeDuckDB {

NodeFileSystem::NodeFileSystem(Napi::ThreadSafeFunction &filesystem_callback)
    : filesystem_callback{filesystem_callback} {}

unique_ptr<duckdb::FileHandle>
NodeFileSystem::OpenFile(const char *path, uint8_t flags,
                         FileLockType lock_type) {
  cout << "here" << endl;
  return duckdb::FileSystem::OpenFile(path, flags, lock_type);
}

int64_t NodeFileSystem::Read(FileHandle &handle, void *buffer,
                             int64_t nr_bytes) {
  cout << "!!!!!!" << endl;
  return duckdb::FileSystem::Read(handle, buffer, nr_bytes);
}

void NodeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                          idx_t location) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;

  auto threadsafe_fn_callback = [&](Napi::Env env, Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          std::memcpy(buffer, info[0].As<Napi::Buffer<char>>().Data(),
                      nr_bytes);
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "nodeFileSystemCallback");

    auto napi_buffer = Napi::Buffer<char>::New(env, nr_bytes);
    auto napi_path = Napi::String::New(env, handle.path);
    auto napi_length = Napi::Number::New(env, nr_bytes);
    auto napi_position = Napi::Number::New(env, location);
    js_callback.Call({napi_path, napi_buffer, napi_length, napi_position, read_finished_callback});
  };

  filesystem_callback.BlockingCall(threadsafe_fn_callback);
  while (!js_callback_fired)
    condition_variable.wait(lock);
}

} // namespace NodeDuckDB
