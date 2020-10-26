#include<napi.h>
#include<iostream>

class AsyncExecutor : public Napi::AsyncWorker {

    public:
        AsyncExecutor(Napi::Function& callback) : Napi::AsyncWorker(callback) {}

        ~AsyncExecutor() {}
    
        void Execute() override {
            std::cout << "Executing" << std::endl;
        }

        void OnOK() override {
            Napi::HandleScope scope(Env());
            Callback().Call({Env().Null(), Napi::String::New(Env(), "test")});
        }
};
