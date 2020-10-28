#include <napi.h>
#include <iostream>
#include "duckdb.hpp"
#include "result_wrapper.h"
#include "async_worker.h"

using namespace duckdb;

bool bigger_than_four(int value) {
    return value > 4;
}

template<typename TYPE>
static void udf_vectorized(DataChunk &args, ExpressionState &state, Vector &result) {
	// set the result vector type
	result.vector_type = VectorType::FLAT_VECTOR;
	// get a raw array from the result
	auto result_data = FlatVector::GetData<TYPE>(result);

	// get the solely input vector
	auto &input = args.data[0];
	// now get an orrified vector
	VectorData vdata;
	input.Orrify(args.size(), vdata);

	// get a raw array from the orrified input
	auto input_data = (TYPE *)vdata.data;

	// handling the data
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if ((*vdata.nullmask)[idx]) {
			continue;
		}
		result_data[i] = input_data[idx];
	}
}

AsyncExecutor::AsyncExecutor(Napi::Env &env, std::string &query, std::shared_ptr<duckdb::Connection> &connection, Napi::Promise::Deferred &deferred) : Napi::AsyncWorker(env), query(query), connection(connection), deferred(deferred) {}

AsyncExecutor::~AsyncExecutor() {}

void AsyncExecutor::Execute() 
{
    try
    {
        connection->CreateVectorizedFunction<int, int>("udf_vectorized_int", &udf_vectorized<int>);

        // auto prep = connection->Prepare(query);
        // if (!prep->success)
        // {
        //     SetError(prep->error);
        //     return;
        // }
        // vector<duckdb::Value> args; // TODO: take arguments
        // result = prep->Execute(args, true);
        result = connection->SendQuery(query);
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
    Napi::Object result_wrapper = ResultWrapper::Create();
    ResultWrapper *result_unwrapped = ResultWrapper::Unwrap(result_wrapper);
    result_unwrapped->result = std::move(result);
    deferred.Resolve(result_wrapper);
}

void AsyncExecutor::OnError(const Napi::Error &e) 
{
    deferred.Reject(e.Value());
}

            