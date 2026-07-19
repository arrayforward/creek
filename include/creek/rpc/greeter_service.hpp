#pragma once

#include "creek.grpc.pb.h"
#include "creek/leaf/leaf_runtime.hpp"

namespace creek {

class GreeterService final : public creek::v1::Greeter::Service {
public:
    explicit GreeterService(LeafRuntime::Impl* impl) : m_impl(impl) {}
    ::grpc::Status SayHello(::grpc::ServerContext* context,
                            const ::creek::v1::HelloRequest* request,
                            ::creek::v1::HelloReply* response) override;
private:
    LeafRuntime::Impl* m_impl;
};

}
