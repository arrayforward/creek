#include "creek/rpc/greeter_service.hpp"

#include "../leaf/leaf_runtime_impl.hpp"

namespace creek {

::grpc::Status GreeterService::SayHello(::grpc::ServerContext* context,
                                        const ::creek::v1::HelloRequest* request,
                                        ::creek::v1::HelloReply* response) {
    return m_impl->handle_say_hello(context, request, response);
}

}
