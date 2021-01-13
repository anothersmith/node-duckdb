#include "duckdb/common/file_system.hpp"

using namespace duckdb;
namespace NodeDuckDB {

class NodeFileSystem : public duckdb::FileSystem {
	unique_ptr<FileHandle> OpenFile(const char *path, uint8_t flags, FileLockType lock_type) override;
     void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
};
}
