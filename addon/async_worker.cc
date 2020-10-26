#include<napi.h>
#include<iostream>
#include "duckdb.hpp"
#include "result_wrapper.h"

class AsyncExecutor : public Napi::AsyncWorker {

    public:
        AsyncExecutor(Napi::Function& callback, std::string& query, std::shared_ptr<duckdb::Connection>& connection) : Napi::AsyncWorker(callback), query(query), connection(connection) {}

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
            Callback().Call({result_wrapper});
        }
    
    private:
        std::string query;
        std::shared_ptr<duckdb::Connection> connection;
        std::unique_ptr<duckdb::QueryResult> result;
};
