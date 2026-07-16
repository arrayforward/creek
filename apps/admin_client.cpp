#include "creek.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

int set_sticky(const std::string& target, const std::string& service, int strategy, int ttl_ms) {
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::StickyStrategyRequest request;
    request.set_service(service);
    request.set_strategy(strategy);
    request.set_ttl_ms(ttl_ms);
    creek::v1::StickyStrategyReply reply;
    grpc::ClientContext ctx;
    auto status = stub->SetStickyStrategy(&ctx, request, &reply);
    if (!status.ok() || !reply.accepted()) {
        std::cerr << "set_sticky failed: " << (reply.accepted() ? status.error_message() : reply.error()) << std::endl;
        return 1;
    }
    std::cout << "sticky strategy updated" << std::endl;
    return 0;
}

int set_breaker(const std::string& target, const std::string& endpoint_id) {
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::BreakerConfigRequest request;
    request.set_endpoint_id(endpoint_id);
    creek::v1::BreakerConfigReply reply;
    grpc::ClientContext ctx;
    auto status = stub->SetBreakerConfig(&ctx, request, &reply);
    if (!status.ok() || !reply.accepted()) {
        std::cerr << "set_breaker failed: " << (reply.accepted() ? status.error_message() : reply.error()) << std::endl;
        return 1;
    }
    std::cout << "breaker " << (endpoint_id.empty() ? "all" : endpoint_id) << " reset" << std::endl;
    return 0;
}

int push_wasm(const std::string& target, const std::string& module_name, const std::string& file_path) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
        std::cerr << "cannot open " << file_path << std::endl;
        return 1;
    }
    std::ostringstream ss;
    ss << file.rdbuf();
    std::string content = ss.str();
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::PushWasmRequest request;
    request.set_module_name(module_name);
    request.set_wasm_bytes(content);
    creek::v1::PushWasmReply reply;
    grpc::ClientContext ctx;
    auto status = stub->PushWasmModule(&ctx, request, &reply);
    if (!status.ok() || !reply.accepted()) {
        std::cerr << "push_wasm failed: " << (reply.accepted() ? status.error_message() : reply.error()) << std::endl;
        return 1;
    }
    std::cout << "wasm module pushed: id=" << reply.module_id() << std::endl;
    return 0;
}

int list_wasm(const std::string& target) {
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::ListWasmRequest request;
    creek::v1::ListWasmReply reply;
    grpc::ClientContext ctx;
    auto status = stub->ListWasmModules(&ctx, request, &reply);
    if (!status.ok()) {
        std::cerr << "list_wasm failed: " << status.error_message() << std::endl;
        return 1;
    }
    for (const auto& m : reply.modules()) {
        std::cout << "  id=" << m.module_id() << " name=" << m.name() << " size=" << m.size() << std::endl;
    }
    if (reply.modules_size() == 0) std::cout << "  (no modules loaded)" << std::endl;
    return 0;
}

int unload_wasm(const std::string& target, uint32_t module_id) {
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::UnloadWasmRequest request;
    request.set_module_id(module_id);
    creek::v1::UnloadWasmReply reply;
    grpc::ClientContext ctx;
    auto status = stub->UnloadWasmModule(&ctx, request, &reply);
    if (!status.ok() || !reply.accepted()) {
        std::cerr << "unload_wasm failed: " << (reply.accepted() ? status.error_message() : reply.error()) << std::endl;
        return 1;
    }
    std::cout << "wasm module " << module_id << " unloaded" << std::endl;
    return 0;
}

int metrics(const std::string& target) {
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::MetricRequest request;
    request.set_take(true);
    creek::v1::MetricReply reply;
    grpc::ClientContext ctx;
    auto status = stub->Metrics(&ctx, request, &reply);
    if (!status.ok()) {
        std::cerr << "metrics failed: " << status.error_message() << std::endl;
        return 1;
    }
    for (const auto& p : reply.points()) {
        std::cout << p.direction() << "/" << p.rpc_name() << ": calls=" << p.calls()
                  << " errors=" << p.errors() << " bytes=" << p.bytes() << " latency=" << p.latency_us() << std::endl;
    }
    return 0;
}

void usage() {
    std::cerr << "usage: creek_admin --target HOST:PORT <command> [args]\n"
              << "  sticky SERVICE STRATEGY TTL_MS\n"
              << "  breaker [ENDPOINT_ID]\n"
              << "  push-wasm NAME WASM_FILE\n"
              << "  list-wasm\n"
              << "  unload-wasm MODULE_ID\n"
              << "  metrics\n";
}

int main(int argc, char** argv) {
    if (argc < 4) { usage(); return 1; }
    std::string target;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--target" && i + 1 < argc) {
            target = argv[++i];
        } else if (std::string(argv[i]) == "sticky" && i + 3 < argc) {
            return set_sticky(target, argv[i+1], std::stoi(argv[i+2]), std::stoi(argv[i+3]));
        } else if (std::string(argv[i]) == "breaker") {
            return set_breaker(target, i + 1 < argc ? argv[i+1] : "");
        } else if (std::string(argv[i]) == "push-wasm" && i + 2 < argc) {
            return push_wasm(target, argv[i+1], argv[i+2]);
        } else if (std::string(argv[i]) == "list-wasm") {
            return list_wasm(target);
        } else if (std::string(argv[i]) == "unload-wasm" && i + 1 < argc) {
            return unload_wasm(target, std::stoi(argv[++i]));
        } else if (std::string(argv[i]) == "metrics") {
            return metrics(target);
        }
    }
    usage();
    return 1;
}
