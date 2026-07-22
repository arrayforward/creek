#include "creek/rpc/admin_service.hpp"

#include "../leaf/leaf_runtime_impl.hpp"

namespace creek {

::grpc::Status AdminService::Metrics(::grpc::ServerContext* context,
                                     const ::creek::v1::MetricRequest* request,
                                     ::creek::v1::MetricReply* response) {
    return m_impl->handle_metrics(context, request, response);
}

::grpc::Status AdminService::Directory(::grpc::ServerContext* context,
                                       const ::creek::v1::DirectoryRequest* request,
                                       ::creek::v1::DirectoryReply* response) {
    return m_impl->handle_directory_query(context, request, response);
}

::grpc::Status AdminService::SetStickyStrategy(::grpc::ServerContext* context,
                                               const ::creek::v1::StickyStrategyRequest* request,
                                               ::creek::v1::StickyStrategyReply* response) {
    return m_impl->handle_set_sticky(context, request, response);
}

::grpc::Status AdminService::SetBreakerConfig(::grpc::ServerContext* context,
                                              const ::creek::v1::BreakerConfigRequest* request,
                                              ::creek::v1::BreakerConfigReply* response) {
    return m_impl->handle_set_breaker(context, request, response);
}

::grpc::Status AdminService::PushWasmModule(::grpc::ServerContext* context,
                                            const ::creek::v1::PushWasmRequest* request,
                                            ::creek::v1::PushWasmReply* response) {
    return m_impl->handle_push_wasm(context, request, response);
}

::grpc::Status AdminService::ListWasmModules(::grpc::ServerContext* context,
                                             const ::creek::v1::ListWasmRequest* request,
                                             ::creek::v1::ListWasmReply* response) {
    return m_impl->handle_list_wasm(context, request, response);
}

::grpc::Status AdminService::UnloadWasmModule(::grpc::ServerContext* context,
                                              const ::creek::v1::UnloadWasmRequest* request,
                                              ::creek::v1::UnloadWasmReply* response) {
    return m_impl->handle_unload_wasm(context, request, response);
}

}
