#include "creek.grpc.pb.h"
#include "creek/types.hpp"
#include "creek/crash_handler.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <csignal>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

using namespace std::chrono_literals;

volatile std::sig_atomic_t stopped = 0;

void handle_signal(int) {
    stopped = 1;
}

struct Options {
    std::string id;
    std::string listen;
    std::string leaf;
};

void usage() {
    std::cerr << "usage: creek_hello_server --id ID --listen HOST:PORT --leaf HOST:PORT\n";
}

std::string value_after(int& index, int argc, char** argv, std::string_view option) {
    if (++index >= argc) {
        throw std::runtime_error("missing value for " + std::string(option));
    }
    return argv[index];
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--id") {
            options.id = value_after(index, argc, argv, option);
        } else if (option == "--listen" || option == "--grpc") {
            options.listen = value_after(index, argc, argv, option);
        } else if (option == "--leaf") {
            options.leaf = value_after(index, argc, argv, option);
        } else {
            throw std::runtime_error("unknown option: " + option);
        }
    }
    if (options.id.empty() || options.listen.empty() || options.leaf.empty()) {
        throw std::runtime_error("--id, --listen, and --leaf are required");
    }
    return options;
}

class GreeterService final : public creek::v1::Greeter::Service {
public:
    explicit GreeterService(std::string backend_id) : backend_id_(std::move(backend_id)) {}

    grpc::Status SayHello(grpc::ServerContext*, const creek::v1::HelloRequest* request,
                          creek::v1::HelloReply* reply) override {
        reply->set_backend_id(backend_id_);
        reply->set_message("Hello, " + request->name() + " from " + backend_id_);
        return grpc::Status::OK;
    }

private:
    std::string backend_id_;
};

grpc::Status register_backend(creek::v1::LeafControl::Stub& control, const Options& options) {
    creek::v1::RegisterRequest request;
    auto* endpoint = request.mutable_endpoint();
    endpoint->set_endpoint_id(options.id);
    endpoint->set_service(creek::v1::Greeter::service_full_name());
    endpoint->set_target(options.listen);
    endpoint->set_version(creek::unix_millis());
    endpoint->set_updated_ms(creek::unix_millis());
    endpoint->set_alive(true);
    creek::v1::RegisterReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    auto status = control.Register(&context, request, &reply);
    if (status.ok() && !reply.accepted()) {
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, reply.error());
    }
    return status;
}

void heartbeat_loop(creek::v1::LeafControl::Stub& control, const std::string& id) {
    while (!stopped) {
        std::this_thread::sleep_for(500ms);
        if (stopped) {
            break;
        }
        creek::v1::HeartbeatRequest request;
        request.set_endpoint_id(id);
        creek::v1::HeartbeatReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + 1s);
        control.Heartbeat(&context, request, &reply);
    }
}

void deregister_backend(creek::v1::LeafControl::Stub& control, const std::string& id) {
    creek::v1::DeregisterRequest request;
    request.set_endpoint_id(id);
    creek::v1::DeregisterReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + 1s);
    control.Deregister(&context, request, &reply);
}

int execute(const Options& options) {
    GreeterService service(options.id);
    grpc::ServerBuilder builder;
    int selected_port = 0;
    builder.AddListeningPort(options.listen, grpc::InsecureServerCredentials(), &selected_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server || selected_port == 0) {
        std::cerr << "failed to listen on " << options.listen << '\n';
        return 1;
    }

    auto channel = grpc::CreateChannel(options.leaf, grpc::InsecureChannelCredentials());
    auto control = creek::v1::LeafControl::NewStub(channel);
    grpc::Status registration;
    const auto registration_deadline = std::chrono::steady_clock::now() + 5s;
    do {
        registration = register_backend(*control, options);
        if (registration.ok()) {
            break;
        }
        std::this_thread::sleep_for(100ms);
    } while (!stopped && std::chrono::steady_clock::now() < registration_deadline);
    if (!registration.ok() || stopped) {
        std::cerr << "register failed: " << registration.error_message() << '\n';
        server->Shutdown();
        return 1;
    }

    std::thread heartbeat(heartbeat_loop, std::ref(*control), std::cref(options.id));
    while (!stopped) {
        std::this_thread::sleep_for(100ms);
    }
    heartbeat.join();
    deregister_backend(*control, options.id);
    server->Shutdown();
    return 0;
}

}

int main(int argc, char** argv) {
    if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
        usage();
        return 0;
    }
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    creek::install_crash_handler("logs");
    try {
        return execute(parse_options(argc, argv));
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        usage();
        return 2;
    }
}
