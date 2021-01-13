#include "node_filesystem.h"
#include "duckdb/common/file_system.hpp"
#include <iostream>
using namespace duckdb;
using namespace std;
namespace NodeDuckDB {

NodeFileSystem::NodeFileSystem(const Napi::Function &filesystem_callback)
    : filesystem_callback{filesystem_callback} {}

unique_ptr<duckdb::FileHandle> NodeFileSystem::OpenFile(const char *path, uint8_t flags,
                         FileLockType lock_type) {
  cout << "here" << endl;
  return duckdb::FileSystem::OpenFile(path, flags, lock_type);
}

void NodeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes,
                          idx_t location) {
  cout << "here1111" << endl;

  return duckdb::FileSystem::Read(handle, buffer, nr_bytes, location);
}

} // namespace NodeDuckDB
