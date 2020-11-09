#include <napi.h>
#include <iostream>
#include "duckdb.hpp"
#include "result_iterator.h"
#include "async_executor.h"
#include <string>

using namespace duckdb;

namespace NodeDuckDB {
    AsyncExecutor::AsyncExecutor(Napi::Env &env, std::string &query, std::shared_ptr<duckdb::Connection> &connection, Napi::Promise::Deferred &deferred, bool forceMaterialized, ResultFormat &rowResultFormat) : Napi::AsyncWorker(env), query(query), connection(connection), deferred(deferred), forceMaterialized(forceMaterialized), rowResultFormat(rowResultFormat) {}

    AsyncExecutor::~AsyncExecutor() {}

    void AsyncExecutor::Execute() 
    {
        try
        {
            if (forceMaterialized) {
                result = connection->Query(query);
            } else {
                result = connection->SendQuery(query);
            }
            if (!result.get()->success)
            {
                SetError(result.get()->error);
            }
        }
        catch (...)
        {
            SetError("Unknown Error: Something happened during execution of the query");
        }
    }

    void AsyncExecutor::OnOK() 
    {
        Napi::HandleScope scope(Env());
        Napi::Object result_iterator = ResultIterator::Create();
        ResultIterator *result_unwrapped = ResultIterator::Unwrap(result_iterator);
        result_unwrapped->result = std::move(result);
        result_unwrapped->rowResultFormat = rowResultFormat;
        deferred.Resolve(result_iterator);
    }

    void AsyncExecutor::OnError(const Napi::Error &e) 
    {
        deferred.Reject(e.Value());
    }
}

            
