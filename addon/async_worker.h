#include <napi.h>
#include "duckdb.hpp"
#include "result_wrapper.h"

class AsyncExecutor : public Napi::AsyncWorker
{
public:
    AsyncExecutor(Napi::Env &env, std::string &query, std::shared_ptr<duckdb::Connection> &connection, Napi::Promise::Deferred &deferred, bool forceMaterialized);

    ~AsyncExecutor();

    void Execute() override;

    void OnOK() override;

    void OnError(const Napi::Error &e) override;

                

private:
    std::string query;
    std::shared_ptr<duckdb::Connection> connection;
    std::unique_ptr<duckdb::QueryResult> result;
    Napi::Promise::Deferred deferred;
    bool forceMaterialized;
};