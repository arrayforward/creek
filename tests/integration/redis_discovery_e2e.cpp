#include "creek/redis.hpp"
#include "creek/logger.hpp"

#include <gtest/gtest.h>

#include <hiredis/hiredis.h>

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifndef CREEK_TEST_SIDECAR_EXE
#define CREEK_TEST_SIDECAR_EXE "D:\\vit\\creek\\build\\msys2-mingw64\\bin\\creek_sidecar.exe"
#endif
#ifndef CREEK_TEST_HELLO_SERVER_EXE
#define CREEK_TEST_HELLO_SERVER_EXE "D:\\vit\\creek\\build\\msys2-mingw64\\bin\\creek_hello_server.exe"
#endif
#ifndef CREEK_TEST_HELLO_CLIENT_EXE
#define CREEK_TEST_HELLO_CLIENT_EXE "D:\\vit\\creek\\build\\msys2-mingw64\\bin\\creek_hello_client.exe"
#endif

namespace {

constexpr const char* kRedisHost = "127.0.0.1";
constexpr int kRedisPort = 6379;
constexpr const char* kRedisPassword = "creekredis";
constexpr const char* kRedisNodesKey = "creek.nodes";
constexpr const char* kRedisLeavesKey = "creek.nodes:leaves";
constexpr const char* kToken = "creek-redis-e2e-token";
constexpr int kNodeCount = 2;
constexpr int kLeafCount = 2;

struct WsaInit {
    WsaInit() {
        WSADATA data{};
        WSAStartup(MAKEWORD(2, 2), &data);
    }
    ~WsaInit() { WSACleanup(); }
};

const WsaInit g_wsa_init;

std::uint16_t alloc_port(int sock_type) {
    SOCKET s = ::socket(AF_INET, sock_type, IPPROTO_IP);
    if (s == INVALID_SOCKET) return 0;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::closesocket(s);
        return 0;
    }
    sockaddr_in actual{};
    int actual_len = sizeof(actual);
    if (::getsockname(s, reinterpret_cast<sockaddr*>(&actual), &actual_len) != 0) {
        ::closesocket(s);
        return 0;
    }
    std::uint16_t port = ntohs(actual.sin_port);
    ::closesocket(s);
    return port;
}

std::uint16_t alloc_udp_port() { return alloc_port(SOCK_DGRAM); }
std::uint16_t alloc_tcp_port() { return alloc_port(SOCK_STREAM); }

std::string addr_str(std::uint16_t port) {
    return std::string("127.0.0.1:") + std::to_string(port);
}

bool wait_tcp_port(std::uint16_t port, std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
        if (s != INVALID_SOCKET) {
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);
            int rc = ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            ::closesocket(s);
            if (rc == 0) return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

std::string quote_arg(const std::string& arg) {
    bool needs_quote = arg.find_first_of(" \t\"\\") != std::string::npos;
    if (!needs_quote) return arg;
    std::string out = "\"";
    for (char c : arg) {
        if (c == '\\') out += "\\\\";
        else if (c == '"') out += "\\\"";
        else out += c;
    }
    out += "\"";
    return out;
}

std::string build_cmdline(const std::vector<std::string>& args) {
    std::string cmd;
    for (std::size_t i = 0; i < args.size(); ++i) {
        if (i > 0) cmd += ' ';
        cmd += quote_arg(args[i]);
    }
    return cmd;
}

struct ManagedProcess {
    std::string label;
    std::string log_path;
    PROCESS_INFORMATION pi{};
    bool started{false};

    ManagedProcess() { ZeroMemory(&pi, sizeof(pi)); }
    ManagedProcess(const ManagedProcess&) = delete;
    ManagedProcess& operator=(const ManagedProcess&) = delete;
    ManagedProcess(ManagedProcess&& other) noexcept { *this = std::move(other); }
    ManagedProcess& operator=(ManagedProcess&& other) noexcept {
        label = std::move(other.label);
        log_path = std::move(other.log_path);
        pi = other.pi;
        started = other.started;
        ZeroMemory(&other.pi, sizeof(other.pi));
        other.started = false;
        return *this;
    }

    ~ManagedProcess() { terminate(); }

    bool spawn(const std::vector<std::string>& args) {
        SECURITY_ATTRIBUTES sa{};
        sa.nLength = sizeof(sa);
        sa.bInheritHandle = TRUE;
        sa.lpSecurityDescriptor = nullptr;

        HANDLE h_nul = ::CreateFileA(
            "NUL", GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &sa, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);

        HANDLE h_log = ::CreateFileA(
            log_path.c_str(),
            FILE_APPEND_DATA,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &sa,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            nullptr);
        if (h_log == INVALID_HANDLE_VALUE) {
            if (h_nul != INVALID_HANDLE_VALUE) ::CloseHandle(h_nul);
            return false;
        }

        std::string cmdline = build_cmdline(args);
        std::vector<char> cmdline_buf(cmdline.begin(), cmdline.end());
        cmdline_buf.push_back('\0');

        STARTUPINFOA si{};
        si.cb = sizeof(si);
        si.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
        si.wShowWindow = SW_HIDE;
        si.hStdInput = h_nul;
        si.hStdOutput = h_log;
        si.hStdError = h_log;

        DWORD flags = CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
        BOOL ok = ::CreateProcessA(
            nullptr,
            cmdline_buf.data(),
            nullptr,
            nullptr,
            TRUE,
            flags,
            nullptr,
            nullptr,
            &si,
            &pi);

        if (h_nul != INVALID_HANDLE_VALUE) ::CloseHandle(h_nul);
        if (h_log != INVALID_HANDLE_VALUE) ::CloseHandle(h_log);

        if (!ok) {
            ZeroMemory(&pi, sizeof(pi));
            return false;
        }
        started = true;
        return true;
    }

    DWORD pid() const { return pi.dwProcessId; }

    bool running() const {
        if (!started || pi.hProcess == nullptr) return false;
        DWORD code = 0;
        if (!::GetExitCodeProcess(pi.hProcess, &code)) return false;
        return code == STILL_ACTIVE;
    }

    void terminate() {
        if (!started) return;
        if (pi.hProcess) {
            HANDLE h = ::OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, FALSE, pi.dwProcessId);
            if (h) {
                ::TerminateProcess(h, 0);
                ::WaitForSingleObject(h, 3000);
                ::CloseHandle(h);
            } else {
                ::TerminateProcess(pi.hProcess, 0);
                ::WaitForSingleObject(pi.hProcess, 3000);
            }
            ::CloseHandle(pi.hProcess);
            pi.hProcess = nullptr;
        }
        if (pi.hThread) {
            ::CloseHandle(pi.hThread);
            pi.hThread = nullptr;
        }
        started = false;
    }
};

struct RedisGuard {
    redisContext* ctx{nullptr};

    RedisGuard() = default;
    ~RedisGuard() { disconnect(); }
    RedisGuard(const RedisGuard&) = delete;
    RedisGuard& operator=(const RedisGuard&) = delete;

    bool connect() {
        struct timeval tv{2, 0};
        ctx = redisConnectWithTimeout(kRedisHost, kRedisPort, tv);
        if (!ctx || ctx->err) {
            disconnect();
            return false;
        }
        if (std::string(kRedisPassword).size() > 0) {
            redisReply* r = static_cast<redisReply*>(
                redisCommand(ctx, "AUTH %s", kRedisPassword));
            if (!r) { disconnect(); return false; }
            bool ok = r->type != REDIS_REPLY_ERROR;
            std::string err;
            if (!ok && r->str) err.assign(r->str, r->len);
            freeReplyObject(r);
            if (!ok) {
                CREEK_LOG_ERROR(std::string("Redis AUTH failed: ") + err);
                disconnect();
                return false;
            }
        }
        return true;
    }

    void disconnect() {
        if (ctx) { redisFree(ctx); ctx = nullptr; }
    }

    bool del_key(const std::string& key) {
        if (!ctx) return false;
        redisReply* r = static_cast<redisReply*>(redisCommand(ctx, "DEL %s", key.c_str()));
        if (!r) return false;
        freeReplyObject(r);
        return true;
    }

    std::size_t hlen(const std::string& key) {
        if (!ctx) return 0;
        redisReply* r = static_cast<redisReply*>(redisCommand(ctx, "HLEN %s", key.c_str()));
        std::size_t n = 0;
        if (r && r->type == REDIS_REPLY_INTEGER) n = static_cast<std::size_t>(r->integer);
        if (r) freeReplyObject(r);
        return n;
    }

    bool flush_db() {
        if (!ctx) return false;
        redisReply* r = static_cast<redisReply*>(redisCommand(ctx, "FLUSHDB"));
        if (!r) return false;
        bool ok = r->type != REDIS_REPLY_ERROR;
        freeReplyObject(r);
        return ok;
    }
};

std::string read_pipe_to_string(FILE* pipe) {
    std::string out;
    char buffer[512];
    while (char* line = ::fgets(buffer, sizeof(buffer), pipe)) {
        out.append(line);
    }
    return out;
}

}

class RedisDiscoveryE2ETest : public ::testing::Test {
protected:
    RedisGuard redis_;
    std::vector<ManagedProcess> processes_;
    std::filesystem::path log_dir_;

    void SetUp() override {
        if (!redis_.connect()) {
            GTEST_SKIP() << "Redis not reachable at " << kRedisHost << ":" << kRedisPort
                         << "; skipping creek_redis_discovery_e2e";
        }
        log_dir_ = std::filesystem::temp_directory_path() /
                   ("creek-redis-e2e-" +
                    std::to_string(::GetCurrentProcessId()) + "-" +
                    std::to_string(static_cast<long long>(
                        std::chrono::system_clock::now().time_since_epoch().count())));
        std::error_code ec;
        std::filesystem::remove_all(log_dir_, ec);
        std::filesystem::create_directories(log_dir_, ec);
        ASSERT_FALSE(ec) << "failed to create log dir " << log_dir_;
        redis_.flush_db();
    }

    void TearDown() override {
        for (auto& proc : processes_) proc.terminate();
        processes_.clear();
        if (redis_.ctx) {
            redis_.del_key(kRedisNodesKey);
            redis_.del_key(kRedisLeavesKey);
        }
        std::error_code ec;
        std::filesystem::remove_all(log_dir_, ec);
    }

    ManagedProcess& spawn(const std::string& label, const std::vector<std::string>& args) {
        ManagedProcess proc;
        proc.label = label;
        proc.log_path = (log_dir_ / (label + ".log")).string();
        EXPECT_TRUE(proc.spawn(args)) << "failed to spawn " << label;
        processes_.push_back(std::move(proc));
        return processes_.back();
    }

    bool wait_for_hlen(const std::string& key, std::size_t expected,
                       std::chrono::seconds timeout) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (redis_.hlen(key) >= expected) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        return false;
    }

    bool run_hello_client(const std::string& target, std::string* backend_id) {
        std::string cmd = std::string("\"") + CREEK_TEST_HELLO_CLIENT_EXE + "\""
                          + " --target " + target
                          + " --name e2e --sid 1 --sticky true --count 1 --timeout-ms 1000";
        FILE* pipe = ::_popen(cmd.c_str(), "r");
        if (!pipe) return false;
        std::string output = read_pipe_to_string(pipe);
        int rc = ::_pclose(pipe);
        if (rc != 0) {
            CREEK_LOG_ERROR(std::string("hello_client failed (rc=") + std::to_string(rc) + "): " + output);
            return false;
        }
        std::istringstream iss(output);
        std::string line;
        if (!std::getline(iss, line)) return false;
        auto tab = line.find('\t');
        if (tab == std::string::npos) return false;
        if (backend_id) *backend_id = line.substr(0, tab);
        return !backend_id->empty();
    }
};

TEST_F(RedisDiscoveryE2ETest, NodesAndLeavesDiscoverAndFailover) {
    std::uint16_t node1_udp = alloc_udp_port();
    std::uint16_t node1_metrics = alloc_tcp_port();
    std::uint16_t node2_udp = alloc_udp_port();
    std::uint16_t node2_metrics = alloc_tcp_port();
    std::uint16_t leaf1_udp = alloc_udp_port();
    std::uint16_t leaf1_grpc = alloc_tcp_port();
    std::uint16_t leaf2_udp = alloc_udp_port();
    std::uint16_t leaf2_grpc = alloc_tcp_port();
    std::uint16_t backend1_port = alloc_tcp_port();
    std::uint16_t backend2_port = alloc_tcp_port();

    ASSERT_NE(node1_udp, 0); ASSERT_NE(node1_metrics, 0);
    ASSERT_NE(node2_udp, 0); ASSERT_NE(node2_metrics, 0);
    ASSERT_NE(leaf1_udp, 0); ASSERT_NE(leaf1_grpc, 0);
    ASSERT_NE(leaf2_udp, 0); ASSERT_NE(leaf2_grpc, 0);
    ASSERT_NE(backend1_port, 0); ASSERT_NE(backend2_port, 0);

    auto& node1 = spawn("node1", {
        CREEK_TEST_SIDECAR_EXE, "node",
        "--id", "node-1",
        "--udp", addr_str(node1_udp),
        "--metrics", addr_str(node1_metrics),
        "--token", kToken,
        "--redis-host", kRedisHost,
        "--redis-port", std::to_string(kRedisPort),
        "--redis-password", kRedisPassword,
        "--redis-key", kRedisNodesKey,
        "--sync-ms", "200",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto& node2 = spawn("node2", {
        CREEK_TEST_SIDECAR_EXE, "node",
        "--id", "node-2",
        "--udp", addr_str(node2_udp),
        "--peer", "node-1@" + addr_str(node1_udp),
        "--metrics", addr_str(node2_metrics),
        "--token", kToken,
        "--redis-host", kRedisHost,
        "--redis-port", std::to_string(kRedisPort),
        "--redis-password", kRedisPassword,
        "--redis-key", kRedisNodesKey,
        "--sync-ms", "200",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });

    ASSERT_TRUE(wait_for_hlen(kRedisNodesKey, static_cast<std::size_t>(kNodeCount),
                              std::chrono::seconds(5)))
        << "Redis did not see " << kNodeCount << " nodes; "
        << "node1.pid=" << node1.pid() << " alive=" << node1.running()
        << "; node2.pid=" << node2.pid() << " alive=" << node2.running();

    auto& leaf1 = spawn("leaf1", {
        CREEK_TEST_SIDECAR_EXE, "leaf",
        "--id", "leaf-1",
        "--udp", addr_str(leaf1_udp),
        "--parent", "node-1@" + addr_str(node1_udp),
        "--grpc", addr_str(leaf1_grpc),
        "--token", kToken,
        "--redis-host", kRedisHost,
        "--redis-port", std::to_string(kRedisPort),
        "--redis-password", kRedisPassword,
        "--redis-key", kRedisNodesKey,
        "--sync-ms", "200",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });

    auto& leaf2 = spawn("leaf2", {
        CREEK_TEST_SIDECAR_EXE, "leaf",
        "--id", "leaf-2",
        "--udp", addr_str(leaf2_udp),
        "--parent", "node-2@" + addr_str(node2_udp),
        "--grpc", addr_str(leaf2_grpc),
        "--token", kToken,
        "--redis-host", kRedisHost,
        "--redis-port", std::to_string(kRedisPort),
        "--redis-password", kRedisPassword,
        "--redis-key", kRedisNodesKey,
        "--sync-ms", "200",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });

    ASSERT_TRUE(wait_for_hlen(kRedisLeavesKey, static_cast<std::size_t>(kLeafCount),
                              std::chrono::seconds(5)))
        << "Redis did not see " << kLeafCount << " leaves; "
        << "leaf1.pid=" << leaf1.pid() << " alive=" << leaf1.running()
        << "; leaf2.pid=" << leaf2.pid() << " alive=" << leaf2.running();

    auto& backend1 = spawn("backend1", {
        CREEK_TEST_HELLO_SERVER_EXE,
        "--id", "backend-1",
        "--listen", addr_str(backend1_port),
        "--leaf", addr_str(leaf1_grpc),
    });

    auto& backend2 = spawn("backend2", {
        CREEK_TEST_HELLO_SERVER_EXE,
        "--id", "backend-2",
        "--listen", addr_str(backend2_port),
        "--leaf", addr_str(leaf2_grpc),
    });

    ASSERT_TRUE(wait_tcp_port(leaf1_grpc, std::chrono::seconds(10)))
        << "leaf1 grpc never became ready";
    ASSERT_TRUE(wait_tcp_port(leaf2_grpc, std::chrono::seconds(10)))
        << "leaf2 grpc never became ready";

    std::string target = addr_str(leaf1_grpc);
    std::string first_backend;
    auto discovery_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < discovery_deadline) {
        if (backend1.running() && backend2.running() &&
            run_hello_client(target, &first_backend)) {
            break;
        }
        first_backend.clear();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_FALSE(first_backend.empty())
        << "no successful client call within 20s; "
        << "backend1.pid=" << backend1.pid() << " alive=" << backend1.running()
        << "; backend2.pid=" << backend2.pid() << " alive=" << backend2.running();

    for (int i = 0; i < 10; ++i) {
        std::string b;
        ASSERT_TRUE(run_hello_client(target, &b))
            << "sticky call " << i << " failed; expected backend " << first_backend;
        ASSERT_EQ(b, first_backend)
            << "sticky sid=1 selected multiple backends at call " << i
            << " (got " << b << ", expected " << first_backend << ")";
    }

    std::string expected;
    ManagedProcess* target_proc = nullptr;
    if (first_backend == "backend-1") {
        expected = "backend-2";
        target_proc = &backend1;
    } else if (first_backend == "backend-2") {
        expected = "backend-1";
        target_proc = &backend2;
    } else {
        FAIL() << "unexpected backend id from client: " << first_backend;
    }
    ASSERT_NE(target_proc, nullptr);

    HANDLE h = ::OpenProcess(PROCESS_TERMINATE, FALSE, target_proc->pid());
    ASSERT_NE(h, nullptr) << "OpenProcess failed for pid " << target_proc->pid();
    BOOL term_ok = ::TerminateProcess(h, 0);
    ::CloseHandle(h);
    ASSERT_TRUE(term_ok);

    auto failover_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    bool switched = false;
    while (std::chrono::steady_clock::now() < failover_deadline) {
        std::string b;
        if (run_hello_client(target, &b) && b == expected) {
            switched = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    ASSERT_TRUE(switched)
        << "did not route to " << expected << " within 20s after killing "
        << first_backend;
}