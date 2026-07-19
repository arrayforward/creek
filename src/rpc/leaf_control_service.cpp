#include "creek/rpc/leaf_control_service.hpp"

#include "../leaf/leaf_runtime_impl.hpp"

namespace creek {

::grpc::Status LeafControlService::Register(::grpc::ServerContext* context,
                                            const ::creek::v1::RegisterRequest* request,
                                            ::creek::v1::RegisterReply* response) {
    return m_impl->handle_register(context, request, response);
}

::grpc::Status LeafControlService::Heartbeat(::grpc::ServerContext* context,
                                             const ::creek::v1::HeartbeatRequest* request,
                                             ::creek::v1::HeartbeatReply* response) {
    return m_impl->handle_heartbeat(context, request, response);
}

::grpc::Status LeafControlService::Deregister(::grpc::ServerContext* context,
                                              const ::creek::v1::DeregisterRequest* request,
                                              ::creek::v1::DeregisterReply* response) {
    return m_impl->handle_deregister(context, request, response);
}

}
