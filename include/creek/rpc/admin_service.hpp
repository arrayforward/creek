#pragma once

#include "creek.grpc.pb.h"
#include "creek/leaf/leaf_runtime.hpp"

namespace creek {

class AdminService final : public creek::v1::Admin::Service {
public:
    explicit AdminService(LeafRuntime::Impl* impl) : m_impl(impl) {}
    ::grpc::Status Metrics(::grpc::ServerContext* context,
                           const ::creek::v1::MetricRequest* request,
                           ::creek::v1::MetricReply* response) override;
    ::grpc::Status Directory(::grpc::ServerContext* context,
                             const ::creek::v1::DirectoryRequest* request,
                             ::creek::v1::DirectoryReply* response) override;
    ::grpc::Status SetStickyStrategy(::grpc::ServerContext* context,
                                     const ::creek::v1::StickyStrategyRequest* request,
                                     ::creek::v1::StickyStrategyReply* response) override;
    ::grpc::Status SetBreakerConfig(::grpc::ServerContext* context,
                                    const ::creek::v1::BreakerConfigRequest* request,
                                    ::creek::v1::BreakerConfigReply* response) override;
    ::grpc::Status PushWasmModule(::grpc::ServerContext* context,
                                  const ::creek::v1::PushWasmRequest* request,
                                  ::creek::v1::PushWasmReply* response) override;
    ::grpc::Status ListWasmModules(::grpc::ServerContext* context,
                                   const ::creek::v1::ListWasmRequest* request,
                                   ::creek::v1::ListWasmReply* response) override;
    ::grpc::Status UnloadWasmModule(::grpc::ServerContext* context,
                                    const ::creek::v1::UnloadWasmRequest* request,
                                    ::creek::v1::UnloadWasmReply* response) override;
private:
    LeafRuntime::Impl* m_impl;
};

}
