#include<napi.h>
#include<iostream>
#include "duckdb.hpp"
#include "result_wrapper.h"

class AsyncExecutor : public Napi::AsyncWorker {

    public:
        AsyncExecutor(Napi::Env &env, std::string& query, std::shared_ptr<duckdb::Connection>& connection,  Napi::Promise::Deferred& deferred) : Napi::AsyncWorker(env), query(query), connection(connection), deferred(deferred) {}

        ~AsyncExecutor() {}
    
        void Execute() override {

            auto prep = connection->Prepare(query);
            // TODO: error handling 
            vector<duckdb::Value> args; // TODO: take arguments
            result = prep->Execute(args, true);
        }

        void OnOK() override {
            Napi::HandleScope scope(Env());

            Napi::Object result_wrapper = ResultWrapper::Create();
            ResultWrapper* result_unwrapped = ResultWrapper::Unwrap(result_wrapper);
            result_unwrapped->result = std::move(result);
            deferred.Resolve(result_wrapper);
        }
    
    private:
        std::string query;
        std::shared_ptr<duckdb::Connection> connection;
        std::unique_ptr<duckdb::QueryResult> result;
        Napi::Promise::Deferred deferred;
};
