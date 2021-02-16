#include "node_filesystem.h"
#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include <condition_variable>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>
using namespace duckdb;
using namespace std;
using namespace Napi;
namespace NodeDuckDB {
struct UnixFileHandle : public FileHandle {
public:
  UnixFileHandle(FileSystem &file_system, string path, int fd)
      : FileHandle(file_system, path), fd(fd) {}
  virtual ~UnixFileHandle() { Close(); }

protected:
  void Close() override {
    if (fd != -1) {
      close(fd);
    }
  };

public:
  int fd;
};

NodeFileSystem::NodeFileSystem(
    Napi::ThreadSafeFunction &read_with_location_callback_tsfn,
    Napi::ThreadSafeFunction &read_tsfn, Napi::ThreadSafeFunction &glob_tsfn,
    Napi::ThreadSafeFunction &get_file_size_tsfn,
    Napi::ThreadSafeFunction &open_file_tsfn,
    Napi::ThreadSafeFunction &truncate_tsfn)
    : read_with_location_callback_tsfn{read_with_location_callback_tsfn},
      read_tsfn{read_tsfn}, glob_tsfn{glob_tsfn},
      get_file_size_tsfn{get_file_size_tsfn}, open_file_tsfn{open_file_tsfn},
      truncate_tsfn{truncate_tsfn} {}

unique_ptr<duckdb::FileHandle>
NodeFileSystem::OpenFile(const char *path, uint8_t flags,
                         FileLockType lock_type) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  unique_ptr<duckdb::FileHandle> file_handle;
  string error;
  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          } else {
            auto result = info[1].As<Napi::Number>();

            file_handle = duckdb::make_unique<UnixFileHandle>(
                *this, path, result.Int64Value());
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "openFile");

    auto napi_path = Napi::String::New(env, path);
    // TODO: replicate bitwise logic for flags?
    auto napi_flags = Napi::Number::New(env, O_RDONLY);
    auto napi_lock_type =
        Napi::Number::New(env, static_cast<double>(lock_type));

    js_callback.Call(
        {napi_path, napi_flags, napi_lock_type, read_finished_callback});
  };

  open_file_tsfn.Acquire();
  auto status = open_file_tsfn.BlockingCall(threadsafe_fn_callback);
  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }
  while (!js_callback_fired)
    condition_variable.wait(lock);
  open_file_tsfn.Release();

  if (!error.empty()) {
    throw std::runtime_error(error);
  }
  return file_handle;
}

int64_t NodeFileSystem::Read(FileHandle &handle, void *buffer,
                             int64_t nr_bytes) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  auto bytes_read = 0;
  string error;

  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          } else {
            std::memcpy(buffer, info[1].As<Napi::Buffer<char>>().Data(),
                        nr_bytes);
          }
          bytes_read = info[2].As<Napi::Number>().Int64Value();
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "Read");

    auto napi_buffer = Napi::Buffer<char>::New(env, nr_bytes);
    auto napi_fd =
        Napi::Number::New(env, dynamic_cast<UnixFileHandle &>(handle).fd);
    auto napi_length = Napi::Number::New(env, nr_bytes);
    js_callback.Call(
        {napi_fd, napi_buffer, napi_length, read_finished_callback});
  };

  read_tsfn.Acquire();
  auto status = read_tsfn.BlockingCall(threadsafe_fn_callback);
  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }

  while (!js_callback_fired)
    condition_variable.wait(lock);
  read_tsfn.Release();
  if (!error.empty()) {
    throw std::runtime_error(error);
  }
  return bytes_read;
}

void NodeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                          idx_t location) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  string error;

  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          } else {
            std::memcpy(buffer, info[1].As<Napi::Buffer<char>>().Data(),
                        nr_bytes);
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "Read");

    auto napi_buffer = Napi::Buffer<char>::New(env, nr_bytes);
    auto napi_fd =
        Napi::Number::New(env, dynamic_cast<UnixFileHandle &>(handle).fd);
    auto napi_length = Napi::Number::New(env, nr_bytes);
    auto napi_position = Napi::Number::New(env, location);
    js_callback.Call({napi_fd, napi_buffer, napi_length, napi_position,
                      read_finished_callback});
  };

  read_with_location_callback_tsfn.Acquire();
  auto status =
      read_with_location_callback_tsfn.BlockingCall(threadsafe_fn_callback);
  read_with_location_callback_tsfn.Release();
  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }

  while (!js_callback_fired)
    condition_variable.wait(lock);
  if (!error.empty()) {
    throw std::runtime_error(error);
  }
}

int64_t NodeFileSystem::GetFileSize(FileHandle &handle) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  int64_t file_size;
  string error;

  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          } else {
            auto result = info[1].As<Napi::Number>();
            file_size = result.Int64Value();
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "GetFileSize");

    auto napi_path = Napi::String::New(env, handle.path);
    js_callback.Call({napi_path, read_finished_callback});
  };

  get_file_size_tsfn.Acquire();
  auto status = get_file_size_tsfn.BlockingCall(threadsafe_fn_callback);
  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }

  while (!js_callback_fired)
    condition_variable.wait(lock);
  get_file_size_tsfn.Release();

  if (!error.empty()) {
    throw std::runtime_error(error);
  }
  return file_size;
}

bool NodeFileSystem::DirectoryExists(const string &directory) {
  return duckdb::FileSystem::DirectoryExists(directory);
}

bool NodeFileSystem::ListFiles(const string &directory,
                               std::function<void(string, bool)> callback) {
  return duckdb::FileSystem::ListFiles(directory, callback);
}

bool NodeFileSystem::FileExists(const string &filename) {
  return duckdb::FileSystem::FileExists(filename);
}

string NodeFileSystem::PathSeparator() {
  return duckdb::FileSystem::PathSeparator();
}

string NodeFileSystem::JoinPath(const string &a, const string &path) {
  return duckdb::FileSystem::JoinPath(a, path);
}

void NodeFileSystem::SetWorkingDirectory(string path) {
  return duckdb::FileSystem::SetWorkingDirectory(path);
}

string NodeFileSystem::GetWorkingDirectory() {
  return duckdb::FileSystem::GetWorkingDirectory();
}

void NodeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  string error;

  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "Truncate");

    auto napi_fd =
        Napi::Number::New(env, dynamic_cast<UnixFileHandle &>(handle).fd);
    auto napi_new_size = Napi::Number::New(env, new_size);
    js_callback.Call({napi_fd, napi_new_size, read_finished_callback});
  };

  truncate_tsfn.Acquire();
  auto status = truncate_tsfn.BlockingCall(threadsafe_fn_callback);
  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }

  while (!js_callback_fired)
    condition_variable.wait(lock);
  truncate_tsfn.Release();
  if (!error.empty()) {
    throw std::runtime_error(error);
  }
}

vector<string> NodeFileSystem::Glob(string path) {
  std::condition_variable condition_variable;
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  bool js_callback_fired = false;
  vector<string> matches;
  string error;

  auto threadsafe_fn_callback = [&](Napi::Env env, Napi::Function js_callback) {
    auto read_finished_callback = Napi::Function::New(
        env,
        [&](const Napi::CallbackInfo &info) {
          if (!info[0].IsNull()) {
            error = info[0].As<Napi::Error>().Message();
          } else {
            auto result = info[1].As<Napi::Array>();

            matches = vector<string>(result.Length());
            for (int i = 0; i < result.Length(); i++) {
              matches[i] = result.Get(static_cast<uint32_t>(i))
                               .As<Napi::String>()
                               .Utf8Value();
            }
          }
          js_callback_fired = true;
          condition_variable.notify_one();
        },
        "Glob");

    auto napi_path = Napi::String::New(env, path);
    js_callback.Call({napi_path, read_finished_callback});
  };

  glob_tsfn.Acquire();
  auto status = glob_tsfn.BlockingCall(threadsafe_fn_callback);

  if (status != napi_status::napi_ok) {
    throw std::runtime_error("Napi status:" +
                             to_string(static_cast<int>(status)));
  }

  while (!js_callback_fired)
    condition_variable.wait(lock);
  glob_tsfn.Release();
  if (!error.empty()) {
    throw std::runtime_error(error);
  }

  return matches;
}

} // namespace NodeDuckDB
