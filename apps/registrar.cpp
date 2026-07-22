// creek_registrar: registers one backend target under one or more service
// full names with a leaf's LeafControl service, then keeps the registration
// alive with periodic heartbeats. On SIGINT/SIGTERM it deregisters cleanly.
//
//   creek_registrar --leaf HOST:PORT --target HOST:PORT --id BACKEND_ID \
//       --service pkg.Service [--service pkg.Other ...] [--heartbeat-ms N]

#include "creek.grpc.pb.h"
#include "creek/types.hpp"
#include "creek/crash_handler.hpp"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <csignal>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using namespace std::chrono_literals;

volatile std::sig_atomic_t stopped = 0;

void handle_signal(int) {
    stopped = 1;
}

struct Options {
    std::string leaf;
    std::string target;
    std::string id;
    std::vector<std::string> services;
    int heartbeat_ms = 500;
};

void usage() {
    std::cerr << "usage: creek_registrar --leaf HOST:PORT --target HOST:PORT --id ID "
                 "--service SERVICE [--service ...] [--heartbeat-ms N]\n";
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
        if (option == "--leaf") {
            options.leaf = value_after(index, argc, argv, option);
        } else if (option == "--target") {
            options.target = value_after(index, argc, argv, option);
        } else if (option == "--id") {
            options.id = value_after(index, argc, argv, option);
        } else if (option == "--service") {
            options.services.push_back(value_after(index, argc, argv, option));
        } else if (option == "--heartbeat-ms") {
            options.heartbeat_ms = std::stoi(value_after(index, argc, argv, option));
        } else {
            throw std::runtime_error("unknown option: " + option);
        }
    }
    if (options.leaf.empty() || options.target.empty() || options.id.empty() ||
        options.services.empty()) {
        throw std::runtime_error("--leaf, --target, --id and at least one --service are required");
    }
    if (options.heartbeat_ms <= 0) {
        throw std::runtime_error("--heartbeat-ms must be positive");
    }
    return options;
}

// Endpoint ids must be unique per (backend, service) pair: the directory is
// keyed by endpoint_id. A single-service registration keeps the plain id.
std::string endpoint_id_for(const Options& options, const std::string& service) {
    if (options.services.size() == 1) return options.id;
    return options.id + "#" + service;
}

grpc::Status register_service(creek::v1::LeafControl::Stub& control,
                              const Options& options, const std::string& service,
                              const std::string& endpoint_id) {
    creek::v1::RegisterRequest request;
    auto* endpoint = request.mutable_endpoint();
    endpoint->set_endpoint_id(endpoint_id);
    endpoint->set_service(service);
    endpoint->set_target(options.target);
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

void heartbeat_loop(creek::v1::LeafControl::Stub& control, const Options& options,
                    const std::vector<std::pair<std::string, std::string>>& registrations,
                    int interval_ms) {
    while (!stopped) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        if (stopped) break;
        for (const auto& [service, endpoint_id] : registrations) {
            creek::v1::HeartbeatRequest request;
            request.set_endpoint_id(endpoint_id);
            creek::v1::HeartbeatReply reply;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + 1s);
            control.Heartbeat(&context, request, &reply);
            // The leaf forgot the endpoint (e.g. it restarted): re-register.
            if (!reply.accepted()) {
                auto status = register_service(control, options, service, endpoint_id);
                if (status.ok()) {
                    std::cerr << "re-registered service=" << service
                              << " endpoint_id=" << endpoint_id << '\n';
                }
            }
        }
    }
}

void deregister_all(creek::v1::LeafControl::Stub& control,
                    const std::vector<std::string>& endpoint_ids) {
    for (const auto& id : endpoint_ids) {
        creek::v1::DeregisterRequest request;
        request.set_endpoint_id(id);
        creek::v1::DeregisterReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + 1s);
        control.Deregister(&context, request, &reply);
    }
}

int execute(const Options& options) {
    auto channel = grpc::CreateChannel(options.leaf, grpc::InsecureChannelCredentials());
    auto control = creek::v1::LeafControl::NewStub(channel);

    std::vector<std::string> endpoint_ids;
    endpoint_ids.reserve(options.services.size());
    std::vector<std::pair<std::string, std::string>> registrations;
    const auto registration_deadline = std::chrono::steady_clock::now() + 10s;
    for (const auto& service : options.services) {
        const std::string endpoint_id = endpoint_id_for(options, service);
        grpc::Status registration;
        do {
            registration = register_service(*control, options, service, endpoint_id);
            if (registration.ok()) break;
            std::this_thread::sleep_for(100ms);
        } while (!stopped && std::chrono::steady_clock::now() < registration_deadline);
        if (!registration.ok() || stopped) {
            std::cerr << "register " << service << " failed: "
                      << registration.error_message() << '\n';
            deregister_all(*control, endpoint_ids);
            return 1;
        }
        std::cerr << "registered service=" << service << " endpoint_id=" << endpoint_id
                  << " target=" << options.target << '\n';
        endpoint_ids.push_back(endpoint_id);
        registrations.emplace_back(service, endpoint_id);
    }

    std::thread heartbeat(heartbeat_loop, std::ref(*control), std::cref(options),
                          std::cref(registrations), options.heartbeat_ms);
    while (!stopped) {
        std::this_thread::sleep_for(100ms);
    }
    heartbeat.join();
    deregister_all(*control, endpoint_ids);
    return 0;
}

}  // namespace

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
