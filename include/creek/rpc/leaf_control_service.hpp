#pragma once

#include "creek.grpc.pb.h"
#include "creek/leaf/leaf_runtime.hpp"

namespace creek {

class LeafControlService final : public creek::v1::LeafControl::Service {
public:
    explicit LeafControlService(LeafRuntime::Impl* impl) : m_impl(impl) {}
    ::grpc::Status Register(::grpc::ServerContext* context,
                            const ::creek::v1::RegisterRequest* request,
                            ::creek::v1::RegisterReply* response) override;
    ::grpc::Status Heartbeat(::grpc::ServerContext* context,
                             const ::creek::v1::HeartbeatRequest* request,
                             ::creek::v1::HeartbeatReply* response) override;
    ::grpc::Status Deregister(::grpc::ServerContext* context,
                              const ::creek::v1::DeregisterRequest* request,
                              ::creek::v1::DeregisterReply* response) override;
private:
    LeafRuntime::Impl* m_impl;
};

}
