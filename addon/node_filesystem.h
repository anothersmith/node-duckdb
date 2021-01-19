#include "duckdb/common/file_system.hpp"
#include <napi.h>

using namespace duckdb;
namespace NodeDuckDB {

class NodeFileSystem : public duckdb::FileSystem {
public:
  NodeFileSystem(Napi::ThreadSafeFunction &filesystem_callback);
  unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags,
                                  FileLockType lock_type) override;
  int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
  void Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
            idx_t location) override;

private:
  Napi::ThreadSafeFunction &filesystem_callback;
};
} // namespace NodeDuckDB
