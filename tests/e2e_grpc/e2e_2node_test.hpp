#pragma once

#include "creek/runtime.hpp"
#include "creek/redis.hpp"
#include "creek/types.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace creek::e2e {

struct ChildProcess {
    std::string tag;
    std::string exe;
    std::string args;
    int pid{0};
#ifdef _WIN32
    PROCESS_INFORMATION proc{};
    HANDLE job{nullptr};
#endif
    std::string stdout_path;
    std::string stderr_path;
};

class TestEnv {
public:
    TestEnv(std::string sidecar, std::string hello_server, std::string hello_client,
            std::string redis_host, int redis_port,
            std::string redis_password, std::string redis_key,
            std::string token, std::string log_dir);
    ~TestEnv();

    bool start_all();
    bool wait_redis(const std::string& field, const std::string& expected, int timeout_ms);
    bool wait_tcp(int port, int timeout_ms);
    std::optional<std::string> hello_call(int sid, bool sticky, int timeout_ms, std::string& err);
    bool kill(const std::string& tag);
    void kill_all();

    const std::string& redis_host() const { return redis_host_; }
    int redis_port() const { return redis_port_; }
    const std::string& redis_key() const { return redis_key_; }
    const std::string& log_dir_path() const { return log_dir_; }

    struct Ports {
        int node1_udp;
        int node1_metrics;
        int node2_udp;
        int node2_metrics;
        int client_leaf_udp;
        int client_leaf_grpc;
        int client_leaf_metrics;
        int svc1_leaf_udp;
        int svc1_leaf_grpc;
        int svc1_leaf_metrics;
        int svc2_leaf_udp;
        int svc2_leaf_grpc;
        int svc2_leaf_metrics;
        int backend1;
        int backend2;
    };
    const Ports& ports() const { return ports_; }

private:
    std::shared_ptr<struct ChildHandle> spawn(const std::string& tag, const std::string& exe,
                                              std::initializer_list<std::string> args);

    std::string sidecar_;
    std::string hello_server_;
    std::string hello_client_;
    std::string redis_host_;
    int redis_port_;
    std::string redis_password_;
    std::string redis_key_;
    std::string token_;
    std::string log_dir_;

    std::unique_ptr<RedisClient> redis_;
    std::vector<std::shared_ptr<struct ChildHandle>> children_;
    Ports ports_{};
};

struct TestResult {
    bool ok;
    std::string phase;
    std::string detail;
};

TestResult run_e2e(TestEnv& env);

}
