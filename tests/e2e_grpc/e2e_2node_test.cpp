#include "e2e_2node_test.hpp"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "creek/redis.hpp"

namespace creek::e2e {

namespace fs = std::filesystem;

namespace {

FILE* g_log_file = nullptr;

void log_open(const std::string& path) {
    if (g_log_file) { std::fclose(g_log_file); g_log_file = nullptr; }
    std::string full = path + "/e2e-run.log";
    g_log_file = std::fopen(full.c_str(), "w");
}
void log_close() { if (g_log_file) { std::fclose(g_log_file); g_log_file = nullptr; } }
void log_emit(const std::string& tag, const std::string& msg) {
    if (g_log_file) { std::fprintf(g_log_file, "[e2e] %s: %s\n", tag.c_str(), msg.c_str()); std::fflush(g_log_file); }
    std::fprintf(stdout, "[e2e] %s: %s\n", tag.c_str(), msg.c_str()); std::fflush(stdout);
}
void log_phase(const std::string& msg) { log_emit("===", msg); }
void log_step(const std::string& tag, const std::string& msg) { log_emit(tag, msg); }

std::string quote_arg(const std::string& a) {
    if (a.empty()) return "\"\"";
    bool need = false;
    for (char c : a) {
        if (c == ' ' || c == '\t' || c == '"') { need = true; break; }
    }
    if (!need) return a;
    std::string out = "\"";
    for (char c : a) {
        if (c == '"' || c == '\\') out.push_back('\\');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::string build_cmd(const std::string& exe, const std::vector<std::string>& args) {
    std::ostringstream os;
    os << quote_arg(exe);
    for (const auto& a : args) {
        os << ' ' << quote_arg(a);
    }
    return os.str();
}

bool tcp_connect_ok(const std::string& host, int port, int timeout_ms) {
#ifdef _WIN32
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
#endif
    if (s < 0) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
#ifdef _WIN32
        ::closesocket(s);
#else
        ::close(s);
#endif
        return false;
    }
#ifdef _WIN32
    u_long nb = 1;
    ::ioctlsocket(s, FIONBIO, &nb);
#else
    int fl = fcntl(s, F_GETFL, 0);
    if (fl >= 0) fcntl(s, F_SETFL, fl | O_NONBLOCK);
#endif
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    bool ok = false;
    while (std::chrono::steady_clock::now() < deadline) {
        int r = ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (r == 0) { ok = true; break; }
#ifdef _WIN32
        int e = ::WSAGetLastError();
        if (e != WSAEWOULDBLOCK && e != WSAEINPROGRESS && e != WSAEALREADY) {
            break;
        }
#else
        if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK) {
            break;
        }
#endif
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(s, &fds);
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int sr = ::select(static_cast<int>(s) + 1, nullptr, &fds, nullptr, &tv);
        if (sr > 0) {
            int so_error = 0;
            socklen_t len = sizeof(so_error);
            ::getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &len);
            if (so_error == 0) { ok = true; break; }
        }
    }
#ifdef _WIN32
    ::closesocket(s);
#else
    ::close(s);
#endif
    return ok;
}

void log_dump(TestEnv& env) {
    for (const auto& tag : {"client_leaf","node1","node2","service1_leaf","service2_leaf","backend1","backend2"}) {
        std::string err = (fs::path(env.log_dir_path()) / (std::string(tag) + ".err")).string();
        std::ifstream in(err);
        if (!in) continue;
        log_emit("dump", std::string("---- ") + tag + ".err ----");
        std::string line;
        std::vector<std::string> lines;
        while (std::getline(in, line)) {
            lines.push_back(line);
            if (lines.size() > 40) lines.erase(lines.begin());
        }
        for (const auto& l : lines) {
            log_emit("dump", l);
        }
    }
}

}

struct ChildHandle {
    std::string tag;
    std::string args;
    std::string stdout_path;
    std::string stderr_path;
    int pid{0};
#ifdef _WIN32
    PROCESS_INFORMATION proc{};
    HANDLE job{nullptr};
#endif
};

TestEnv::TestEnv(std::string sidecar, std::string hello_server, std::string hello_client,
                 std::string redis_host, int redis_port,
                 std::string redis_password, std::string redis_key,
                 std::string token, std::string log_dir)
    : sidecar_(std::move(sidecar)), hello_server_(std::move(hello_server)),
      hello_client_(std::move(hello_client)),
      redis_host_(std::move(redis_host)), redis_port_(redis_port),
      redis_password_(std::move(redis_password)), redis_key_(std::move(redis_key)),
      token_(std::move(token)), log_dir_(std::move(log_dir)) {
    fs::create_directories(log_dir_);

    ports_.node1_udp = 30000;
    ports_.node1_metrics = 30001;
    ports_.node2_udp = 30004;
    ports_.node2_metrics = 30005;
    ports_.client_leaf_udp = 30008;
    ports_.client_leaf_grpc = 30009;
    ports_.client_leaf_metrics = 30012;
    ports_.svc1_leaf_udp = 30013;
    ports_.svc1_leaf_grpc = 30016;
    ports_.svc1_leaf_metrics = 30017;
    ports_.svc2_leaf_udp = 30020;
    ports_.svc2_leaf_grpc = 30021;
    ports_.svc2_leaf_metrics = 30024;
    ports_.backend1 = 30025;
    ports_.backend2 = 30028;
}

TestEnv::~TestEnv() {
    kill_all();
}

std::shared_ptr<ChildHandle> TestEnv::spawn(const std::string& tag, const std::string& exe,
                                             std::initializer_list<std::string> args) {
    auto handle = std::make_shared<ChildHandle>();
    handle->tag = tag;
    handle->stdout_path = (fs::path(log_dir_) / (tag + ".log")).string();
    handle->stderr_path = (fs::path(log_dir_) / (tag + ".err")).string();

    std::vector<std::string> arg_vec(args);
    handle->args = build_cmd(exe, arg_vec);

#ifdef _WIN32
    HANDLE job = ::CreateJobObjectW(nullptr, nullptr);
    if (job) {
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION jb{};
        jb.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        ::SetInformationJobObject(job, JobObjectExtendedLimitInformation, &jb, sizeof(jb));
    }
    handle->job = job;

    STARTUPINFOA si{};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    sa.lpSecurityDescriptor = nullptr;
    HANDLE out_file = ::CreateFileA(handle->stdout_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ,
                                     &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE err_file = ::CreateFileA(handle->stderr_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ,
                                     &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    si.hStdOutput = out_file;
    si.hStdError = err_file;
    si.hStdInput = INVALID_HANDLE_VALUE;

    std::string cmd_buf = handle->args;

    PROCESS_INFORMATION pi{};
    BOOL ok = ::CreateProcessA(nullptr, cmd_buf.data(), nullptr, nullptr, TRUE,
                                CREATE_DEFAULT_ERROR_MODE | CREATE_SUSPENDED,
                                nullptr, nullptr, &si, &pi);
    if (out_file != INVALID_HANDLE_VALUE) ::CloseHandle(out_file);
    if (err_file != INVALID_HANDLE_VALUE) ::CloseHandle(err_file);
    if (!ok) {
        if (job) ::CloseHandle(job);
        char msg[128];
        std::snprintf(msg, sizeof(msg), "create process failed for %s: gle=%lu", tag.c_str(), ::GetLastError());
        log_emit("spawn", msg);
        return nullptr;
    }
    if (job) {
        ::AssignProcessToJobObject(job, pi.hProcess);
    }
    ::ResumeThread(pi.hThread);
    handle->proc = pi;
    handle->pid = static_cast<int>(pi.dwProcessId);
#else
    pid_t pid = fork();
    if (pid < 0) {
        log_emit("spawn", "fork failed");
        return nullptr;
    }
    if (pid == 0) {
        int fd_out = ::open(handle->stdout_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int fd_err = ::open(handle->stderr_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out >= 0) { ::dup2(fd_out, 1); ::close(fd_out); }
        if (fd_err >= 0) { ::dup2(fd_err, 2); ::close(fd_err); }
        std::vector<std::string> argv;
        argv.reserve(arg_vec.size() + 1);
        argv.push_back(exe);
        for (const auto& a : arg_vec) argv.push_back(a);
        std::vector<char*> cargv;
        for (auto& a : argv) cargv.push_back(a.data());
        cargv.push_back(nullptr);
        ::execv(exe.c_str(), cargv.data());
        std::exit(127);
    }
    handle->pid = pid;
#endif
    log_emit("spawn", tag + " pid=" + std::to_string(handle->pid));
    return handle;
}

bool TestEnv::wait_redis(const std::string& field, const std::string& expected, int timeout_ms) {
    if (!redis_) {
        creek::RedisOptions opts{};
        opts.host = redis_host_;
        opts.port = static_cast<std::uint16_t>(redis_port_);
        opts.user = "";
        opts.password = redis_password_;
        opts.key = redis_key_;
        try {
            redis_ = std::make_unique<creek::RedisClient>(opts, "e2e-driver");
        } catch (const std::exception& e) {
            char m[256];
            std::snprintf(m, sizeof(m), "redis connect failed: %s", e.what());
            log_emit("redis", m);
            return false;
        }
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        try {
            auto value = redis_->hget(redis_key_, field);
            if (value == expected) return true;
        } catch (const std::exception& e) {
            char m[256];
            std::snprintf(m, sizeof(m), "redis hget error: %s", e.what());
            log_emit("redis", m);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    return false;
}

bool TestEnv::wait_tcp(int port, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (tcp_connect_ok("127.0.0.1", port, 200)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::optional<std::string> TestEnv::hello_call(int sid, bool sticky, int timeout_ms, std::string& err) {
    std::string stdout_path = (fs::path(log_dir_) / "client.tmp").string();
    std::string stderr_path = (fs::path(log_dir_) / "client.err").string();
    std::error_code ec;
    std::filesystem::remove(stdout_path, ec);
    std::filesystem::remove(stderr_path, ec);

    std::vector<std::string> args = {
        "--target", "127.0.0.1:" + std::to_string(ports_.client_leaf_grpc),
        "--name", "e2e",
        "--sid", std::to_string(sid),
        "--sticky", sticky ? "true" : "false",
        "--count", "1",
        "--timeout-ms", std::to_string(timeout_ms),
    };

#ifdef _WIN32
    STARTUPINFOA si{};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    sa.lpSecurityDescriptor = nullptr;
    HANDLE out_file = ::CreateFileA(stdout_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ,
                                     &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE err_file = ::CreateFileA(stderr_path.c_str(), GENERIC_WRITE, FILE_SHARE_READ,
                                     &sa, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    si.hStdOutput = out_file;
    si.hStdError = err_file;
    si.hStdInput = INVALID_HANDLE_VALUE;

    std::string cmd = build_cmd(hello_client_, args);

    PROCESS_INFORMATION pi{};
    BOOL ok = ::CreateProcessA(nullptr, cmd.data(), nullptr, nullptr, TRUE,
                                CREATE_DEFAULT_ERROR_MODE | CREATE_SUSPENDED,
                                nullptr, nullptr, &si, &pi);
    if (out_file != INVALID_HANDLE_VALUE) ::CloseHandle(out_file);
    if (err_file != INVALID_HANDLE_VALUE) ::CloseHandle(err_file);
    if (!ok) {
        err = "create process failed";
        return std::nullopt;
    }
    ::ResumeThread(pi.hThread);
    ::WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD code = 0;
    ::GetExitCodeProcess(pi.hProcess, &code);
    ::CloseHandle(pi.hThread);
    ::CloseHandle(pi.hProcess);
#else
    pid_t pid = fork();
    if (pid < 0) {
        err = "fork failed";
        return std::nullopt;
    }
    if (pid == 0) {
        int fd_out = ::open(stdout_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int fd_err = ::open(stderr_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out >= 0) { ::dup2(fd_out, 1); ::close(fd_out); }
        if (fd_err >= 0) { ::dup2(fd_err, 2); ::close(fd_err); }
        std::vector<std::string> argv;
        argv.reserve(args.size() + 1);
        argv.push_back(hello_client_);
        for (const auto& a : args) argv.push_back(a);
        std::vector<char*> cargv;
        for (auto& a : argv) cargv.push_back(a.data());
        cargv.push_back(nullptr);
        ::execv(hello_client_.c_str(), cargv.data());
        std::exit(127);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
#endif

    std::ifstream out(stdout_path);
    std::stringstream ss; ss << out.rdbuf();
    std::string line; std::getline(ss, line);
    std::ifstream err_in(stderr_path);
    std::stringstream se; se << err_in.rdbuf();
    err = se.str();
    if (line.empty()) return std::nullopt;
    if (line.rfind("backend-", 0) != 0) return std::nullopt;
    auto pos = line.find_first_of(" \t");
    if (pos == std::string::npos) return line;
    return line.substr(0, pos);
}

bool TestEnv::kill(const std::string& tag) {
    for (auto& c : children_) {
        if (c->tag == tag) {
#ifdef _WIN32
            if (c->proc.hProcess) {
                ::TerminateProcess(c->proc.hProcess, 0);
            }
#else
            if (c->pid > 0) {
                ::kill(c->pid, SIGTERM);
                int status = 0;
                ::waitpid(c->pid, &status, 0);
            }
#endif
            return true;
        }
    }
    return false;
}

void TestEnv::kill_all() {
    for (auto& c : children_) {
#ifdef _WIN32
        if (c->proc.hProcess) {
            ::TerminateProcess(c->proc.hProcess, 0);
            ::WaitForSingleObject(c->proc.hProcess, 1500);
        }
        if (c->proc.hThread) ::CloseHandle(c->proc.hThread);
        if (c->proc.hProcess) ::CloseHandle(c->proc.hProcess);
        if (c->job) {
            ::CloseHandle(c->job);
            c->job = nullptr;
        }
#else
        if (c->pid > 0) {
            ::kill(c->pid, SIGTERM);
            int status = 0;
            ::waitpid(c->pid, &status, 0);
        }
#endif
    }
    children_.clear();
}

bool TestEnv::start_all() {
    log_step("start_all", "spawning node-1");
    auto node1 = spawn("node1", sidecar_, {
        "node", "--id", "node-1",
        "--udp", "127.0.0.1:" + std::to_string(ports_.node1_udp),
        "--metrics", "127.0.0.1:" + std::to_string(ports_.node1_metrics),
        "--token", token_,
        "--peer", "node-2@127.0.0.1:" + std::to_string(ports_.node2_udp),
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });
    if (!node1) { log_step("start_all", "node1 spawn failed"); return false; }
    children_.push_back(node1);
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    log_step("start_all", "spawning node-2");
    auto node2 = spawn("node2", sidecar_, {
        "node", "--id", "node-2",
        "--udp", "127.0.0.1:" + std::to_string(ports_.node2_udp),
        "--metrics", "127.0.0.1:" + std::to_string(ports_.node2_metrics),
        "--token", token_,
        "--peer", "node-1@127.0.0.1:" + std::to_string(ports_.node1_udp),
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
    });
    if (!node2) { log_step("start_all", "node2 spawn failed"); return false; }
    children_.push_back(node2);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    log_step("start_all", "nodes running");

    log_step("start_all", "spawning client_leaf");
    auto client_leaf = spawn("client_leaf", sidecar_, {
        "leaf", "--id", "client-leaf",
        "--udp", "127.0.0.1:" + std::to_string(ports_.client_leaf_udp),
        "--parent", "node-1@127.0.0.1:" + std::to_string(ports_.node1_udp),
        "--grpc", "127.0.0.1:" + std::to_string(ports_.client_leaf_grpc),
        "--metrics", "127.0.0.1:" + std::to_string(ports_.client_leaf_metrics),
        "--token", token_,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000",
    });
    if (!client_leaf) { log_step("start_all", "client_leaf spawn failed"); return false; }
    children_.push_back(client_leaf);

    log_step("start_all", "spawning service1_leaf");
    auto svc1 = spawn("service1_leaf", sidecar_, {
        "leaf", "--id", "service1-leaf",
        "--udp", "127.0.0.1:" + std::to_string(ports_.svc1_leaf_udp),
        "--parent", "node-2@127.0.0.1:" + std::to_string(ports_.node2_udp),
        "--grpc", "127.0.0.1:" + std::to_string(ports_.svc1_leaf_grpc),
        "--metrics", "127.0.0.1:" + std::to_string(ports_.svc1_leaf_metrics),
        "--token", token_,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000",
    });
    if (!svc1) { log_step("start_all", "service1_leaf spawn failed"); return false; }
    children_.push_back(svc1);

    log_step("start_all", "spawning service2_leaf");
    auto svc2 = spawn("service2_leaf", sidecar_, {
        "leaf", "--id", "service2-leaf",
        "--udp", "127.0.0.1:" + std::to_string(ports_.svc2_leaf_udp),
        "--parent", "node-2@127.0.0.1:" + std::to_string(ports_.node2_udp),
        "--grpc", "127.0.0.1:" + std::to_string(ports_.svc2_leaf_grpc),
        "--metrics", "127.0.0.1:" + std::to_string(ports_.svc2_leaf_metrics),
        "--token", token_,
        "--sync-ms", "100",
        "--heartbeat-ms", "50",
        "--dead-timeout-ms", "2000",
        "--rpc-timeout-ms", "3000",
    });
    if (!svc2) { log_step("start_all", "service2_leaf spawn failed"); return false; }
    children_.push_back(svc2);

    log_step("start_all", "waiting for client_leaf gRPC");
    if (!wait_tcp(ports_.client_leaf_grpc, 15000)) {
        log_step("start_all", "client_leaf gRPC not listening");
        return false;
    }
    log_step("start_all", "waiting for service1_leaf gRPC");
    if (!wait_tcp(ports_.svc1_leaf_grpc, 15000)) {
        log_step("start_all", "service1_leaf gRPC not listening");
        return false;
    }
    log_step("start_all", "waiting for service2_leaf gRPC");
    if (!wait_tcp(ports_.svc2_leaf_grpc, 15000)) {
        log_step("start_all", "service2_leaf gRPC not listening");
        return false;
    }
    log_step("start_all", "all leaf gRPC servers are listening");

    log_step("start_all", "spawning backend-1");
    auto b1 = spawn("backend1", hello_server_, {
        "--id", "backend-1",
        "--listen", "127.0.0.1:" + std::to_string(ports_.backend1),
        "--leaf", "127.0.0.1:" + std::to_string(ports_.svc1_leaf_grpc),
    });
    if (!b1) { log_step("start_all", "backend1 spawn failed"); return false; }
    children_.push_back(b1);

    log_step("start_all", "spawning backend-2");
    auto b2 = spawn("backend2", hello_server_, {
        "--id", "backend-2",
        "--listen", "127.0.0.1:" + std::to_string(ports_.backend2),
        "--leaf", "127.0.0.1:" + std::to_string(ports_.svc2_leaf_grpc),
    });
    if (!b2) { log_step("start_all", "backend2 spawn failed"); return false; }
    children_.push_back(b2);

    log_step("start_all", "waiting for backend-1");
    if (!wait_tcp(ports_.backend1, 15000)) {
        log_step("start_all", "backend-1 not listening");
        return false;
    }
    log_step("start_all", "waiting for backend-2");
    if (!wait_tcp(ports_.backend2, 15000)) {
        log_step("start_all", "backend-2 not listening");
        return false;
    }
    log_step("start_all", "backends are listening");

    return true;
}

TestResult run_e2e(TestEnv& env) {
    log_open(env.log_dir_path());
    log_step("e2e", "=== STARTING creek_e2e_2node ===");
    log_step("e2e", "redis=" + env.redis_host() + ":" + std::to_string(env.redis_port()) + " key=" + env.redis_key());
    TestResult result{false, "init", ""};
    if (!env.start_all()) {
        result.detail = "start_all failed";
        log_dump(env);
        log_close();
        return result;
    }
    log_phase("PHASE 1 nodes registered");
    result.phase = "discovery";

    log_phase("PHASE 2 waiting for service discovery");
    bool discovered = false;
    for (int i = 0; i < 30; ++i) {
        std::string err;
        auto r = env.hello_call(99, false, 1500, err);
        if (r.has_value()) { discovered = true; break; }
        char m[128];
        std::snprintf(m, sizeof(m), "discovery attempt %d not ready: %s", i + 1, err.c_str());
        log_step("discovery", m);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!discovered) {
        result.detail = "service discovery never converged";
        log_dump(env);
        log_close();
        return result;
    }
    log_phase("PHASE 2 done");

    log_phase("PHASE 3 sticky sid=1 -> 10 calls hit same backend");
    std::optional<std::string> selected;
    for (int i = 1; i <= 10; ++i) {
        std::string err;
        auto r = env.hello_call(1, true, 1500, err);
        if (!r.has_value()) {
            char m[128];
            std::snprintf(m, sizeof(m), "sticky call %d failed: %s", i, err.c_str());
            result.detail = m;
            log_dump(env);
            log_close();
            return result;
        }
        char m[128];
        std::snprintf(m, sizeof(m), "[%d/10] -> %s", i, r->c_str());
        log_step("sticky", m);
        if (!selected) selected = r;
        else if (*r != *selected) {
            char m2[128];
            std::snprintf(m2, sizeof(m2), "sticky flipped from %s to %s on attempt %d", selected->c_str(), r->c_str(), i);
            result.detail = m2;
            log_dump(env);
            log_close();
            return result;
        }
    }
    if (!selected || (*selected != "backend-1" && *selected != "backend-2")) {
        result.detail = "unknown selected backend " + (selected ? *selected : std::string("null"));
        log_dump(env);
        log_close();
        return result;
    }
    log_phase(std::string("PHASE 3 done; sticky=") + *selected);

    std::string killed = *selected;
    std::string expected = killed == "backend-1" ? "backend-2" : "backend-1";
    log_phase(std::string("PHASE 4 killing ") + killed);
    env.kill(killed == "backend-1" ? "backend1" : "backend2");
    std::this_thread::sleep_for(std::chrono::seconds(2));

    bool switched = false;
    for (int i = 1; i <= 30; ++i) {
        std::string err;
        auto r = env.hello_call(1, true, 1500, err);
        if (r && *r == expected) {
            char m[128];
            std::snprintf(m, sizeof(m), "[%d/30] -> %s", i, r->c_str());
            log_step("switched", m);
            switched = true;
            break;
        }
        char m[128];
        std::snprintf(m, sizeof(m), "[%d/30] %s", i, r ? r->c_str() : err.c_str());
        log_step("switch-wait", m);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!switched) {
        result.detail = "failover did not reach " + expected;
        log_dump(env);
        log_close();
        return result;
    }
    log_phase("ALL CHECKS PASSED");
    result.ok = true;
    log_close();
    return result;
}

}
