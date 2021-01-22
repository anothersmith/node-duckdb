#include "duckdb/common/file_system.hpp"
#include <napi.h>

using namespace duckdb;
namespace NodeDuckDB {

class NodeFileSystem : public duckdb::FileSystem {
public:
  NodeFileSystem(Napi::ThreadSafeFunction &read_with_location_callback_tsfn, Napi::ThreadSafeFunction &read_tsfn, Napi::ThreadSafeFunction &glob_tsfn, Napi::ThreadSafeFunction &get_file_size_tsfn, Napi::ThreadSafeFunction &open_file_tsfn);
  unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags,
                                  FileLockType lock_type) override;
  int64_t GetFileSize(FileHandle &handle) override;
  bool DirectoryExists(const string &directory) override;
  bool ListFiles(const string &directory, std::function<void(string, bool)> callback) override;
  bool FileExists(const string &filename) override;
  string PathSeparator() override;
  string JoinPath(const string &a, const string &path) override;
  void SetWorkingDirectory(string path) override;
  string GetWorkingDirectory() override;
  vector<string> Glob(string path) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;
  

private:
  Napi::ThreadSafeFunction &read_with_location_callback_tsfn;
  Napi::ThreadSafeFunction &read_tsfn;
  Napi::ThreadSafeFunction &glob_tsfn;
  Napi::ThreadSafeFunction &get_file_size_tsfn;
  Napi::ThreadSafeFunction &open_file_tsfn;
};
} // namespace NodeDuckDB
