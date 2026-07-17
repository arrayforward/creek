#include "creek/runtime.hpp"
#include "creek/types.hpp"
#include "creek/logger.hpp"

#include <chrono>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

volatile std::sig_atomic_t stopped = 0;

void handle_signal(int) {
    stopped = 1;
}

struct Options {
    std::string mode;
    std::string id;
    std::string udp;
    std::vector<std::string> peers;
    std::vector<std::string> parents;
    std::string grpc;
    std::string json;
    std::string metrics;
    std::string token;
    std::string redis_host;
    std::uint16_t redis_port{0};
    std::string redis_user;
    std::string redis_password;
    std::string redis_key;
    std::chrono::milliseconds heartbeat{100};
    std::chrono::milliseconds dead_timeout{3000};
    std::chrono::milliseconds sync{15000};
    std::chrono::milliseconds metric_period{60000};
    std::chrono::milliseconds backend_timeout{3000};
    std::chrono::milliseconds rpc_timeout{15000};
};

void usage() {
    std::cerr
        << "usage: creek_sidecar node --id ID --udp HOST:PORT [--peer ID@HOST:PORT]... [--metrics HOST:PORT] [--token TOKEN] [--heartbeat-ms MS] [--dead-timeout-ms MS] [--sync-ms MS] [--metric-period-ms MS] [--redis-host HOST --redis-port PORT [--redis-user USER] [--redis-password PASS] [--redis-key KEY]]\n"
         << "       creek_sidecar leaf --id ID --udp HOST:PORT --grpc HOST:PORT --parent ID@HOST:PORT... [--json HOST:PORT] [--metrics HOST:PORT] [--token TOKEN] [--heartbeat-ms MS] [--dead-timeout-ms MS] [--sync-ms MS] [--metric-period-ms MS] [--backend-timeout-ms MS] [--rpc-timeout-ms MS] [--redis-host HOST --redis-port PORT [--redis-user USER] [--redis-password PASS] [--redis-key KEY]]\n";
}

std::string value_after(int& index, int argc, char** argv, std::string_view option) {
    if (++index >= argc) {
        throw std::runtime_error("missing value for " + std::string(option));
    }
    return argv[index];
}

std::chrono::milliseconds duration_value(const std::string& value, std::string_view option) {
    std::size_t consumed = 0;
    long long parsed = 0;
    try {
        parsed = std::stoll(value, &consumed);
    } catch (const std::exception&) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    if (consumed != value.size() || parsed <= 0) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    return std::chrono::milliseconds(parsed);
}

std::uint16_t port_value(const std::string& value, std::string_view option) {
    std::size_t consumed = 0;
    long long parsed = 0;
    try {
        parsed = std::stoll(value, &consumed);
    } catch (const std::exception&) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    if (consumed != value.size() || parsed <= 0 || parsed > 65535) {
        throw std::runtime_error("invalid value for " + std::string(option));
    }
    return static_cast<std::uint16_t>(parsed);
}

Options parse_options(int argc, char** argv) {
    if (argc < 2) {
        throw std::runtime_error("missing mode");
    }
    Options options;
    options.mode = argv[1];
    for (int index = 2; index < argc; ++index) {
        const std::string option = argv[index];
        if (option == "--id") {
            options.id = value_after(index, argc, argv, option);
        } else if (option == "--udp") {
            options.udp = value_after(index, argc, argv, option);
        } else if (option == "--peer") {
            options.peers.push_back(value_after(index, argc, argv, option));
        } else if (option == "--parent") {
            options.parents.push_back(value_after(index, argc, argv, option));
        } else if (option == "--grpc") {
            options.grpc = value_after(index, argc, argv, option);
        } else if (option == "--json") {
            options.json = value_after(index, argc, argv, option);
        } else if (option == "--metrics") {
            options.metrics = value_after(index, argc, argv, option);
        } else if (option == "--token") {
            options.token = value_after(index, argc, argv, option);
        } else if (option == "--redis-host") {
            options.redis_host = value_after(index, argc, argv, option);
        } else if (option == "--redis-port") {
            options.redis_port = port_value(value_after(index, argc, argv, option), option);
        } else if (option == "--redis-user") {
            options.redis_user = value_after(index, argc, argv, option);
        } else if (option == "--redis-password") {
            options.redis_password = value_after(index, argc, argv, option);
        } else if (option == "--redis-key") {
            options.redis_key = value_after(index, argc, argv, option);
        } else if (option == "--heartbeat-ms") {
            options.heartbeat = duration_value(value_after(index, argc, argv, option), option);
        } else if (option == "--dead-timeout-ms") {
            options.dead_timeout = duration_value(value_after(index, argc, argv, option), option);
        } else if (option == "--sync-ms") {
            options.sync = duration_value(value_after(index, argc, argv, option), option);
        } else if (option == "--metric-period-ms") {
            options.metric_period = duration_value(value_after(index, argc, argv, option), option);
        } else if (option == "--backend-timeout-ms") {
            options.backend_timeout = duration_value(value_after(index, argc, argv, option), option);
        } else if (option == "--rpc-timeout-ms") {
            options.rpc_timeout = duration_value(value_after(index, argc, argv, option), option);
        } else {
            throw std::runtime_error("unknown option: " + option);
        }
    }
    if (options.mode != "node" && options.mode != "leaf") {
        throw std::runtime_error("mode must be node or leaf");
    }
    if (options.id.empty() || options.udp.empty()) {
        throw std::runtime_error("--id and --udp are required");
    }
    if (options.mode == "node" && (!options.parents.empty() || !options.grpc.empty())) {
        throw std::runtime_error("--parent and --grpc are only valid for leaf mode");
    }
    if (options.mode == "leaf" && (options.parents.empty() || options.grpc.empty())) {
        throw std::runtime_error("leaf mode requires --parent and --grpc");
    }
    if (options.mode == "leaf" && !options.peers.empty()) {
        throw std::runtime_error("--peer is only valid for node mode");
    }
    return options;
}

creek::Address address_value(const std::string& value, std::string_view option) {
    auto address = creek::parse_address(value);
    if (!address) {
        throw std::runtime_error("invalid address for " + std::string(option) + ": " + value);
    }
    return *address;
}

creek::RemotePeer remote_value(const std::string& value, std::string_view option) {
    auto remote = creek::parse_remote(value);
    if (!remote) {
        throw std::runtime_error("invalid remote for " + std::string(option) + ": " + value);
    }
    return *remote;
}

bool has_redis_options(const Options& options) {
    return !options.redis_host.empty() && options.redis_port != 0 && !options.redis_key.empty();
}

template <typename Runtime>
int run(Runtime& runtime) {
    if (!runtime.start()) {
        std::cerr << "failed to start sidecar\n";
        return 1;
    }
    while (!stopped) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    runtime.stop();
    return 0;
}

int execute(const Options& options) {
    const auto udp = address_value(options.udp, "--udp");
    const auto metrics = options.metrics.empty()
                             ? creek::Address{"127.0.0.1", 0}
                             : address_value(options.metrics, "--metrics");
    if (options.mode == "node") {
        creek::NodeConfig config;
        config.id = options.id;
        config.udp_bind = udp;
        config.metrics_bind = metrics;
        config.token = options.token;
        config.sync_interval = options.sync;
        config.metric_period = options.metric_period;
        for (const auto& peer : options.peers) {
            config.peers.push_back(remote_value(peer, "--peer"));
        }
        if (has_redis_options(options)) {
            config.redis = creek::RedisOptions{options.redis_host, options.redis_port,
                                               options.redis_user, options.redis_password,
                                               options.redis_key};
        }
        creek::NodeRuntime runtime(std::move(config));
        return run(runtime);
    }
    creek::LeafConfig config;
    config.id = options.id;
    config.udp_bind = udp;
    config.grpc_bind = address_value(options.grpc, "--grpc");
    config.json_bind = options.json.empty()
                           ? creek::Address{"127.0.0.1", 0}
                           : address_value(options.json, "--json");
    config.metrics_bind = metrics;
    config.token = options.token;
    for (const auto& parent : options.parents) {
        config.parents.push_back(remote_value(parent, "--parent"));
    }
    config.sync_interval = options.sync;
    config.metric_period = options.metric_period;
    config.backend_timeout = options.backend_timeout;
    config.rpc_timeout = options.rpc_timeout;
    if (has_redis_options(options)) {
        config.redis = creek::RedisOptions{options.redis_host, options.redis_port,
                                           options.redis_user, options.redis_password,
                                           options.redis_key};
    }
    creek::LeafRuntime runtime(std::move(config));
    return run(runtime);
}

}

int main(int argc, char** argv) {
    if (argc == 2 && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
        usage();
        return 0;
    }
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    creek::Logger::init();
    CREEK_LOG_INFO(std::string("sidecar starting argc=") + std::to_string(argc));

    try {
        int rc = execute(parse_options(argc, argv));
        creek::Logger::shutdown();
        return rc;
    } catch (const std::exception& error) {
        CREEK_LOG_ERROR(std::string("sidecar fatal: ") + error.what());
        std::cerr << error.what() << '\n';
        creek::Logger::shutdown();
        usage();
        return 2;
    }
}
