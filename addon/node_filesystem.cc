#include "node_filesystem.h"
#include "duckdb/common/file_system.hpp"
#include <iostream>
using namespace duckdb;
using namespace std;
using namespace Napi;
namespace NodeDuckDB {

NodeFileSystem::NodeFileSystem(Napi::ThreadSafeFunction &read_with_location_callback_tsfn, Napi::ThreadSafeFunction &read_tsfn, Napi::ThreadSafeFunction &glob_tsfn)
    : read_with_location_callback_tsfn{read_with_location_callback_tsfn}, read_tsfn{read_tsfn}, glob_tsfn{glob_tsfn} {}

unique_ptr<duckdb::FileHandle>
NodeFileSystem::OpenFile(const char *path, uint8_t flags,
                         FileLockType lock_type) {
  cout << "here" << endl;
  return duckdb::FileSystem::OpenFile(path, flags, lock_type);
}

int64_t NodeFileSystem::Read(FileHandle &handle, void *buffer,
                             int64_t nr_bytes) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  auto bytes_read = 0;

  auto threadsafe_fn_callback = [&](Napi::Env env, Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          std::memcpy(buffer, info[0].As<Napi::Buffer<char>>().Data(),
                      nr_bytes);
          bytes_read = info[1].As<Napi::Number>().Int64Value();
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "nodeFileSystemCallback");

    auto napi_buffer = Napi::Buffer<char>::New(env, nr_bytes);
    auto napi_path = Napi::String::New(env, handle.path);
    auto napi_length = Napi::Number::New(env, nr_bytes);
    js_callback.Call({napi_path, napi_buffer, napi_length, read_finished_callback});
  };

  read_tsfn.BlockingCall(threadsafe_fn_callback);
  while (!js_callback_fired)
    condition_variable.wait(lock);
  return bytes_read;
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

  read_with_location_callback_tsfn.BlockingCall(threadsafe_fn_callback);
  while (!js_callback_fired)
    condition_variable.wait(lock);
}

int64_t NodeFileSystem::GetFileSize(FileHandle &handle) {
  cout << "GetFileSize" << endl;
  return duckdb::FileSystem::GetFileSize(handle);
}

bool NodeFileSystem::DirectoryExists(const string &directory) {
  cout << "DirectoryExists" << endl;
  return duckdb::FileSystem::DirectoryExists(directory);
}

bool NodeFileSystem::ListFiles(const string &directory, std::function<void(string, bool)> callback) {
  cout << "ListFiles" << endl;
  return duckdb::FileSystem::ListFiles(directory, callback);
}

bool NodeFileSystem::FileExists(const string &filename) {
  cout << "FileExists" << endl;
  return duckdb::FileSystem::FileExists(filename);
}

string NodeFileSystem::PathSeparator() {
  cout << "PathSeparator" << endl;
  return duckdb::FileSystem::PathSeparator();
}

string NodeFileSystem::JoinPath(const string &a, const string &path) {
  cout << "JoinPath" << endl;
  return duckdb::FileSystem::JoinPath(a, path);
}

void NodeFileSystem::SetWorkingDirectory(string path) {
  cout << "SetWorkingDirectory" << endl;
  return duckdb::FileSystem::SetWorkingDirectory(path);
}

string NodeFileSystem::GetWorkingDirectory() {
  cout << "GetWorkingDirectory" << endl;
  return duckdb::FileSystem::GetWorkingDirectory();
}

vector<string> NodeFileSystem::Glob(string path) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  vector<string> matches;

  auto threadsafe_fn_callback = [&](Napi::Env env, Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          auto result = info[0].As<Napi::Array>();

          matches = vector<string>(result.Length());
          for (int i = 0; i < result.Length(); i++) {
            matches[i] = result.Get(static_cast<uint32_t>(i)).As<Napi::String>().Utf8Value();
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "nodeFileSystemCallback");

    auto napi_path = Napi::String::New(env, path);
    js_callback.Call({napi_path, read_finished_callback});
  };

  glob_tsfn.BlockingCall(threadsafe_fn_callback);
  while (!js_callback_fired)
    condition_variable.wait(lock);

  return matches;
}

} // namespace NodeDuckDB
