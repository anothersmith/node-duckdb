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
  auto callback = [&](Napi::Env env, Function jsCallback, int *value) {
    cout << "fdsafdsfdsfdsafds" << endl;
    auto fn = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) { 
          cout << "in callback" << endl; 
          // std::unique_lock<std::mutex> lk(mtx);
          ready = true;
          cv.notify_one();
          },
        "theFunction");
    // Transform native data into JS data, passing it to the provided
    // `jsCallback` -- the TSFN's JavaScript function.
    jsCallback.Call({fn});
  };

  int *value = new int(clock());
  filesystem_callback.BlockingCall(value, callback);
  cout << "locking" << endl;
  cout << "waiting" << endl;
  while (!ready)
    cv.wait(lk);
  cout << "unlocked" << endl;

  return duckdb::FileSystem::Read(handle, buffer, nr_bytes, location);
}

} // namespace NodeDuckDB
