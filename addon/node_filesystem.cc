#include "duckdb/common/file_system.hpp"
#include "node_filesystem.h"
#include <iostream>
using namespace duckdb;
using namespace std;
namespace NodeDuckDB {

duckdb::unique_ptr<duckdb::FileHandle> NodeFileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock_type) {
	cout << "here" << endl;
	return duckdb::FileSystem::OpenFile(path, flags, lock_type);
}

void NodeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
		cout << "here1111" << endl;

	return duckdb::FileSystem::Read(handle, buffer, nr_bytes, location);

}

}
