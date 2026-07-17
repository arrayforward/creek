#include "e2e_grpc_test.hpp"

#include "creek.grpc.pb.h"
#include "creek/logger.hpp"

#include <grpcpp/grpcpp.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace creek::e2e {

namespace fs = std::filesystem;

namespace {

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
    for (const auto& a : args) os << ' ' << quote_arg(a);
    return os.str();
}

bool tcp_connect_ok(const std::string& host, int port, int timeout_ms) {
#ifdef _WIN32
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_UDP);
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
        if (e != WSAEWOULDBLOCK && e != WSAEINPROGRESS && e != WSAEALREADY) break;
#else
        if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK) break;
#endif
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(s, &fds);
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int sr = ::select(0, nullptr, &fds, nullptr, &tv);
        if (sr > 0) {
            int so_error = 0;
            socklen_t len = sizeof(so_error);
            ::getsockopt(s, SOL_SOCKET, SO_ERROR,
                         reinterpret_cast<char*>(&so_error), &len);
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

void emit_log(const std::string& tag, const std::string& msg) {
    std::fprintf(stdout, "[e2e-5node] %s: %s\n", tag.c_str(), msg.c_str());
    std::fflush(stdout);
}

} // namespace

TestEnv::TestEnv(std::string sidecar, std::string hello_server,
                 std::string log_dir, std::string token)
    : sidecar_(std::move(sidecar)), hello_server_(std::move(hello_server)),
      log_dir_(std::move(log_dir)), token_(std::move(token)) {
    std::error_code ec;
    fs::create_directories(log_dir_, ec);

    ports_.node1_udp = 30100;
    ports_.node1_metrics = 30101;
    ports_.node2_udp = 30104;
    ports_.node2_metrics = 30105;
    ports_.entry_udp = 30108;
    ports_.entry_grpc = 30109;
    ports_.entry_metrics = 30112;
    ports_.entry_json = 30113;
    ports_.svc1_udp = 30116;
    ports_.svc1_grpc = 30119;
    ports_.svc1_metrics = 30120;
    ports_.svc2_udp = 30123;
    ports_.svc2_grpc = 30126;
    ports_.svc2_metrics = 30127;
    ports_.backend1 = 30130;
    ports_.backend2 = 30133;
}

TestEnv::~TestEnv() {
    kill_all();
}

bool TestEnv::spawn(const std::string& tag, const std::string& exe,
                    const std::vector<std::string>& args) {
    auto child = std::make_unique<ChildProcess>();
    child->tag = tag;
    child->exe = exe;
    child->args = build_cmd(exe, std::vector<std::string>(args));
    child->stdout_path = (fs::path(log_dir_) / (tag + ".log")).string();
    child->stderr_path = (fs::path(log_dir_) / (tag + ".err")).string();

#ifdef _WIN32
    HANDLE job = ::CreateJobObjectW(nullptr, nullptr);
    if (job) {
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION jb{};
        jb.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        ::SetInformationJobObject(job, JobObjectExtendedLimitInformation, &jb, sizeof(jb));
    }
    child->job = job;

    STARTUPINFOA si{};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;
    SECURITY_ATTRIBUTES sa{};
    sa.nLength = sizeof(sa);
    sa.bInheritHandle = TRUE;
    sa.lpSecurityDescriptor = nullptr;
    HANDLE out_file = ::CreateFileA(child->stdout_path.c_str(),
                                     GENERIC_WRITE, FILE_SHARE_READ, &sa,
                                     CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    HANDLE err_file = ::CreateFileA(child->stderr_path.c_str(),
                                     GENERIC_WRITE, FILE_SHARE_READ, &sa,
                                     CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    si.hStdOutput = out_file;
    si.hStdError = err_file;
    si.hStdInput = INVALID_HANDLE_VALUE;

    std::string cmd = child->args;
    PROCESS_INFORMATION pi{};
    BOOL ok = ::CreateProcessA(nullptr, cmd.data(), nullptr, nullptr, TRUE,
                                CREATE_DEFAULT_ERROR_MODE | CREATE_SUSPENDED,
                                nullptr, nullptr, &si, &pi);
    if (out_file != INVALID_HANDLE_VALUE) ::CloseHandle(out_file);
    if (err_file != INVALID_HANDLE_VALUE) ::CloseHandle(err_file);
    if (!ok) {
        if (job) ::CloseHandle(job);
        char buf[128];
        std::snprintf(buf, sizeof(buf), "create process failed for %s: gle=%lu",
                      tag.c_str(), ::GetLastError());
        emit_log("spawn", buf);
        return false;
    }
    if (job) {
        ::AssignProcessToJobObject(job, pi.hProcess);
    }
    ::ResumeThread(pi.hThread);
    child->proc = pi;
    child->pid = static_cast<int>(pi.dwProcessId);
#else
    pid_t pid = fork();
    if (pid < 0) { emit_log("spawn", "fork failed"); return false; }
    if (pid == 0) {
        // Child: redirect stdout/stderr to files and exec.
        int fd_out = ::open(child->stdout_path.c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int fd_err = ::open(child->stderr_path.c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out >= 0) { ::dup2(fd_out, 1); ::close(fd_out); }
        if (fd_err >= 0) { ::dup2(fd_err, 2); ::close(fd_err); }
        std::vector<std::string> argv;
        argv.reserve(args.size() + 1);
        argv.push_back(exe);
        for (const auto& a : args) argv.push_back(a);
        std::vector<char*> cargv;
        for (auto& a : argv) cargv.push_back(a.data());
        cargv.push_back(nullptr);
        ::execv(exe.c_str(), cargv.data());
        std::exit(127);
    }
    child->pid = pid;
#endif
    emit_log("spawn", tag + " pid=" + std::to_string(child->pid));
    children_.push_back(std::move(child));
    return true;
}

bool TestEnv::wait_tcp(int port, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (tcp_connect_ok("127.0.0.1", port, 200)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::optional<std::string> TestEnv::hello_call(int sid, bool sticky,
                                              int timeout_ms,
                                              std::string& error_out) {
    auto target = "127.0.0.1:" + std::to_string(ports_.entry_grpc);
    auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
    auto greeter = creek::v1::Greeter::NewStub(channel);
    creek::v1::HelloRequest request;
    request.set_name("e2e");
    request.set_sid(std::to_string(sid));
    request.set_sticky(sticky);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(timeout_ms));
    context.AddMetadata("sticky", sticky ? "true" : "false");
    context.AddMetadata("sid", std::to_string(sid));
    creek::v1::HelloReply reply;
    auto status = greeter->SayHello(&context, request, &reply);
    if (!status.ok()) {
        error_out = "SayHello failed: code=" + std::to_string(status.error_code()) +
                    " msg=" + status.error_message();
        return std::nullopt;
    }
    return reply.backend_id();
}

bool TestEnv::kill_backend(const std::string& tag) {
    for (auto& c : children_) {
        if (c->tag != tag) continue;
#ifdef _WIN32
        if (c->proc.hProcess) {
            ::TerminateProcess(c->proc.hProcess, 0);
            ::WaitForSingleObject(c->proc.hProcess, 2000);
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
    return false;
}

void TestEnv::kill_all() {
    for (auto& c : children_) {
#ifdef _WIN32
        if (c->proc.hProcess) {
            ::TerminateProcess(c->proc.hProcess, 0);
            ::WaitForSingleObject(c->proc.hProcess, 1000);
        }
        if (c->proc.hThread) ::CloseHandle(c->proc.hThread);
        if (c->proc.hProcess) ::CloseHandle(c->proc.hProcess);
        if (c->job) ::CloseHandle(c->job);
        c->job = nullptr;
#else
        if (c->pid > 0) ::kill(c->pid, SIGTERM);
#endif
    }
    children_.clear();
}

bool TestEnv::start_all() {
    auto build_node_args = [&](const std::string& id, int udp, int metrics,
                              const std::string& peer) {
        std::vector<std::string> args = {
            "node", "--id", id,
            "--udp", "127.0.0.1:" + std::to_string(udp),
            "--metrics", "127.0.0.1:" + std::to_string(metrics),
            "--sync-ms", "100",
            "--metric-period-ms", "1000",
            "--token", token_,
        };
        if (!peer.empty()) {
            args.push_back("--peer");
            args.push_back(peer);
        }
        return args;
    };
    auto build_leaf_args = [&](const std::string& id, int udp,
                              const std::string& parent, int grpc,
                              int metrics, int json_port) {
        std::vector<std::string> args = {
            "leaf", "--id", id,
            "--udp", "127.0.0.1:" + std::to_string(udp),
            "--parent", parent,
            "--grpc", "127.0.0.1:" + std::to_string(grpc),
            "--metrics", "127.0.0.1:" + std::to_string(metrics),
            "--sync-ms", "100",
            "--metric-period-ms", "1000",
            "--token", token_,
            "--rpc-timeout-ms", "3000",
        };
        if (json_port > 0) {
            args.push_back("--json");
            args.push_back("127.0.0.1:" + std::to_string(json_port));
        }
        return args;
    };
    auto build_backend_args = [&](const std::string& id, int listen, int leaf_grpc) {
        return std::vector<std::string>{
            "--id", id,
            "--listen", "127.0.0.1:" + std::to_string(listen),
            "--leaf", "127.0.0.1:" + std::to_string(leaf_grpc),
        };
    };

    emit_log("start_all", "spawning node-1");
    auto n1_args = build_node_args("node-1", ports_.node1_udp,
                                  ports_.node1_metrics,
                                  "node-2@127.0.0.1:" +
                                  std::to_string(ports_.node2_udp));
    if (!spawn("node1", sidecar_, n1_args)) {
        emit_log("start_all", "node-1 spawn failed");
        return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    emit_log("start_all", "spawning node-2");
    auto n2_args = build_node_args("node-2", ports_.node2_udp,
                                  ports_.node2_metrics,
                                  "node-1@127.0.0.1:" +
                                  std::to_string(ports_.node1_udp));
    if (!spawn("node2", sidecar_, n2_args)) {
        emit_log("start_all", "node-2 spawn failed");
        return false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    emit_log("start_all", "spawning entry_leaf (parent=node-1)");
    auto entry_args = build_leaf_args("entry-leaf", ports_.entry_udp,
                                     "node-1@127.0.0.1:" +
                                     std::to_string(ports_.node1_udp),
                                     ports_.entry_grpc,
                                     ports_.entry_metrics,
                                     ports_.entry_json);
    if (!spawn("entry_leaf", sidecar_, entry_args)) {
        emit_log("start_all", "entry_leaf spawn failed");
        return false;
    }

    emit_log("start_all", "spawning service_leaf_1 (parent=node-2)");
    auto svc1_args = build_leaf_args("service-leaf-1", ports_.svc1_udp,
                                     "node-2@127.0.0.1:" +
                                     std::to_string(ports_.node2_udp),
                                     ports_.svc1_grpc,
                                     ports_.svc1_metrics,
                                     0);
    if (!spawn("service_leaf1", sidecar_, svc1_args)) {
        emit_log("start_all", "service_leaf_1 spawn failed");
        return false;
    }

    emit_log("start_all", "spawning service_leaf_2 (parent=node-1)");
    auto svc2_args = build_leaf_args("service-leaf-2", ports_.svc2_udp,
                                     "node-1@127.0.0.1:" +
                                     std::to_string(ports_.node1_udp),
                                     ports_.svc2_grpc,
                                     ports_.svc2_metrics,
                                     0);
    if (!spawn("service_leaf2", sidecar_, svc2_args)) {
        emit_log("start_all", "service_leaf_2 spawn failed");
        return false;
    }

    if (!wait_tcp(ports_.entry_grpc, 15000)) {
        emit_log("start_all", "entry_leaf gRPC not listening");
        return false;
    }
    if (!wait_tcp(ports_.svc1_grpc, 15000)) {
        emit_log("start_all", "service_leaf_1 gRPC not listening");
        return false;
    }
    if (!wait_tcp(ports_.svc2_grpc, 15000)) {
        emit_log("start_all", "service_leaf_2 gRPC not listening");
        return false;
    }
    emit_log("start_all", "all leaf gRPC servers are listening");

    emit_log("start_all", "spawning backend-1");
    auto b1_args = build_backend_args("backend-1", ports_.backend1, ports_.svc1_grpc);
    if (!spawn("backend1", hello_server_, b1_args)) {
        emit_log("start_all", "backend-1 spawn failed");
        return false;
    }

    emit_log("start_all", "spawning backend-2");
    auto b2_args = build_backend_args("backend-2", ports_.backend2, ports_.svc2_grpc);
    if (!spawn("backend2", hello_server_, b2_args)) {
        emit_log("start_all", "backend-2 spawn failed");
        return false;
    }

    if (!wait_tcp(ports_.backend1, 15000)) {
        emit_log("start_all", "backend-1 not listening");
        return false;
    }
    if (!wait_tcp(ports_.backend2, 15000)) {
        emit_log("start_all", "backend-2 not listening");
        return false;
    }
    emit_log("start_all", "backends are listening");

    emit_log("start_all", "waiting 5s for directory sync");
    std::this_thread::sleep_for(std::chrono::seconds(5));
    return true;
}

TestResult run_e2e(TestEnv& env) {
    TestResult result{false, "init", ""};
    if (!env.start_all()) {
        result.detail = "start_all failed";
        return result;
    }

    // Phase 1: service discovery.
    emit_log("e2e", "PHASE 1 service discovery");
    result.phase = "discovery";
    bool discovered = false;
    for (int i = 1; i <= 30; ++i) {
        std::string err;
        auto r = env.hello_call(99, false, 10000, err);
        if (r.has_value()) {
            emit_log("e2e", "discovered backend " + *r);
            discovered = true;
            break;
        }
        emit_log("e2e", "discovery attempt " + std::to_string(i) + ": " + err);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!discovered) {
        result.detail = "service discovery never converged";
        return result;
    }

    // Phase 2: sticky routing.
    emit_log("e2e", "PHASE 2 sticky sid=1 (10 calls)");
    result.phase = "sticky";
    std::optional<std::string> selected;
    for (int i = 1; i <= 10; ++i) {
        std::string err;
        auto r = env.hello_call(1, true, 10000, err);
        if (!r.has_value()) {
            result.detail = "sticky call " + std::to_string(i) + " failed: " + err;
            return result;
        }
        emit_log("e2e", "[" + std::to_string(i) + "/10] -> " + *r);
        if (!selected) {
            selected = *r;
        } else if (*r != *selected) {
            result.detail = "sticky flipped from " + *selected + " to " + *r +
                            " on attempt " + std::to_string(i);
            return result;
        }
    }
    if (!selected || (*selected != "backend-1" && *selected != "backend-2")) {
        result.detail = "unexpected backend " +
                        (selected ? *selected : std::string("<none>"));
        return result;
    }
    emit_log("e2e", "PHASE 2 sticky selected " + *selected);

    // Phase 3: failover.
    std::string killed = *selected;
    std::string expected = killed == "backend-1" ? "backend-2" : "backend-1";
    std::string killed_tag = killed == "backend-1" ? "backend1" : "backend2";
    emit_log("e2e", "PHASE 3 killing " + killed + " (tag " + killed_tag + ")");
    result.phase = "failover";
    env.kill_backend(killed_tag);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    bool switched = false;
    for (int i = 1; i <= 30; ++i) {
        std::string err;
        auto r = env.hello_call(1, true, 10000, err);
        if (r && *r == expected) {
            emit_log("e2e", "[" + std::to_string(i) + "/30] switched to " + *r);
            switched = true;
            break;
        }
        emit_log("e2e", "[" + std::to_string(i) + "/30] " +
                          (r ? *r : err));
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!switched) {
        result.detail = "failover did not reach " + expected;
        return result;
    }

    emit_log("e2e", "ALL CHECKS PASSED");
    result.ok = true;
    return result;
}

} // namespace creek::e2e