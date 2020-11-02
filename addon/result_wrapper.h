#ifndef RESULT_WRAPPER_H
#define RESULT_WRAPPER_H

#include <napi.h>
#include "duckdb.hpp"

namespace NodeDuckDB {
	class ResultWrapper : public Napi::ObjectWrap<ResultWrapper> {
		public:
			static Napi::Object Init(Napi::Env env, Napi::Object exports);
			ResultWrapper(const Napi::CallbackInfo& info);
			static Napi::Object Create();
			unique_ptr<duckdb::QueryResult> result;

		private:
			static Napi::FunctionReference constructor;
			Napi::Value FetchRow(const Napi::CallbackInfo& info);
			Napi::Value Describe(const Napi::CallbackInfo& info);
			Napi::Value GetType(const Napi::CallbackInfo &info);
			Napi::Value Close(const Napi::CallbackInfo &info) {
				result = nullptr;
				return info.Env().Undefined();
			}
			Napi::Value IsClosed(const Napi::CallbackInfo &info);
			unique_ptr<duckdb::DataChunk> current_chunk;
			uint64_t chunk_offset = 0;
	};
}

#endif
