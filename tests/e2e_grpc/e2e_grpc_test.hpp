#pragma once

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
    TestEnv(std::string sidecar, std::string hello_server,
            std::string log_dir, std::string token);
    ~TestEnv();

    // Spawn all 5 sidecar components (2 nodes, 3 leaves) and 2 hello_server
    // backends. Returns false if any spawn fails or any TCP port never opens.
    bool start_all();

    // Wait for a TCP port to accept connections. timeout_ms in milliseconds.
    bool wait_tcp(int port, int timeout_ms);

    // Returns the picked backend ID (e.g. "backend-1") or std::nullopt on failure.
    // error_out receives a human-readable error string.
    std::optional<std::string> hello_call(int sid, bool sticky, int timeout_ms,
                                         std::string& error_out);

    // Kill the named backend process ("backend1" or "backend2").
    bool kill_backend(const std::string& tag);

    // Kill all spawned processes (also called by destructor).
    void kill_all();

    const std::string& log_dir_path() const { return log_dir_; }

    struct Ports {
        int node1_udp;
        int node1_metrics;
        int node2_udp;
        int node2_metrics;
        int entry_udp;
        int entry_grpc;
        int entry_metrics;
        int entry_json;
        int svc1_udp;
        int svc1_grpc;
        int svc1_metrics;
        int svc2_udp;
        int svc2_grpc;
        int svc2_metrics;
        int backend1;
        int backend2;
    };
    const Ports& ports() const { return ports_; }

private:
    bool spawn(const std::string& tag, const std::string& exe,
               const std::vector<std::string>& args);
    std::string sidecar_;
    std::string hello_server_;
    std::string log_dir_;
    std::string token_;
    std::vector<std::unique_ptr<ChildProcess>> children_;
    Ports ports_{};
};

struct TestResult {
    bool ok;
    std::string phase;
    std::string detail;
};

TestResult run_e2e(TestEnv& env);

} // namespace creek::e2e