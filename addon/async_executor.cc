#include <napi.h>
#include <iostream>
#include "duckdb.hpp"
#include "result_iterator.h"
#include "async_executor.h"

using namespace duckdb;

namespace NodeDuckDB {
    AsyncExecutor::AsyncExecutor(Napi::Env &env, std::string &query, std::shared_ptr<duckdb::Connection> &connection, Napi::Promise::Deferred &deferred, bool forceMaterialized) : Napi::AsyncWorker(env), query(query), connection(connection), deferred(deferred), forceMaterialized(forceMaterialized) {}

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
        deferred.Resolve(result_iterator);
    }

    void AsyncExecutor::OnError(const Napi::Error &e) 
    {
        deferred.Reject(e.Value());
    }
}

            
