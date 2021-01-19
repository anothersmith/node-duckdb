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

void NodeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                          idx_t location) {
  std::condition_variable cv;
  std::mutex mtx;
  std::unique_lock<std::mutex> lk(mtx);
  bool ready = false;
  auto callback = [&](Napi::Env env, Function jsCallback, const string *path) {
    cout << "fdsafdsfdsfdsafds" << endl;
    auto fn = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) { 
          auto bla = info[0];
          auto bufferNodejs = bla.As<Napi::Buffer<char>>();

          
          std::memcpy(buffer, bufferNodejs.Data(), nr_bytes);
          ready = true;
          cv.notify_one();
          },
        "theFunction");
    auto buffer1 = Napi::Buffer<char>::New(env, nr_bytes);
    auto path1 = Napi::String::New(env, *path);
    auto length = Napi::Number::New(env, nr_bytes);
    auto position = Napi::Number::New(env, location);
    // Transform native data into JS data, passing it to the provided
    // `jsCallback` -- the TSFN's JavaScript function.
    jsCallback.Call({path1, buffer1, length, position, fn});
  };

  int *value = new int(clock());
  const string *path = &handle.path;
  filesystem_callback.BlockingCall(path, callback);
  cout << "locking" << endl;
  cout << "waiting" << endl;
  while (!ready)
    cv.wait(lk);
  cout << "unlocked" << endl;

}

} // namespace NodeDuckDB
