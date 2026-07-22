// creek_echo_backend: test backend for the generic proxy e2e test. Serves
// creek.test.Echo (unary Echo + bidi Chat). Registration with the leaf is
// done externally via creek_registrar.

#include "echo.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <csignal>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

volatile std::sig_atomic_t stopped = 0;

void handle_signal(int) {
    stopped = 1;
}

class EchoService final : public creek::test::Echo::Service {
public:
    explicit EchoService(std::string backend_id) : backend_id_(std::move(backend_id)) {}

    grpc::Status Echo(grpc::ServerContext*, const creek::test::EchoRequest* request,
                      creek::test::EchoReply* reply) override {
        reply->set_text("echo:" + request->text());
        reply->set_backend_id(backend_id_);
        reply->set_seq(request->seq());
        return grpc::Status::OK;
    }

    grpc::Status Chat(grpc::ServerContext*,
                      grpc::ServerReaderWriter<creek::test::EchoReply,
                                               creek::test::EchoRequest>* stream) override {
        creek::test::EchoRequest request;
        while (stream->Read(&request)) {
            creek::test::EchoReply reply;
            reply.set_text("echo:" + request.text());
            reply.set_backend_id(backend_id_);
            reply.set_seq(request.seq());
            stream->Write(reply);
        }
        return grpc::Status::OK;
    }

    grpc::Status Count(grpc::ServerContext*, const creek::test::EchoRequest* request,
                       grpc::ServerWriter<creek::test::EchoReply>* writer) override {
        for (uint32_t i = 0; i < request->seq(); ++i) {
            creek::test::EchoReply reply;
            reply.set_text("echo:" + request->text());
            reply.set_backend_id(backend_id_);
            reply.set_seq(i);
            writer->Write(reply);
        }
        return grpc::Status::OK;
    }

private:
    std::string backend_id_;
};

}  // namespace

int main(int argc, char** argv) {
    std::string id;
    std::string listen;
    for (int i = 1; i < argc; ++i) {
        const std::string opt = argv[i];
        if (opt == "--id" && i + 1 < argc) {
            id = argv[++i];
        } else if (opt == "--listen" && i + 1 < argc) {
            listen = argv[++i];
        } else {
            std::cerr << "unknown/incomplete option: " << opt << '\n';
            return 2;
        }
    }
    if (id.empty() || listen.empty()) {
        std::cerr << "usage: creek_echo_backend --id ID --listen HOST:PORT\n";
        return 2;
    }
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    EchoService service(id);
    grpc::ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &selected_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server || selected_port == 0) {
        std::cerr << "failed to listen on " << listen << '\n';
        return 1;
    }
    std::cerr << "echo backend " << id << " listening on " << selected_port << '\n';
    while (!stopped) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    server->Shutdown();
    return 0;
}
