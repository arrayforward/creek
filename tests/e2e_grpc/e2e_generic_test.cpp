// End-to-end test for the generic gRPC proxy (creek_e2e_generic):
//   a) generic unary via a generic client through the leaf mesh
//   b) bidi streaming through the leaf mesh (multi-message, half-close)
//   c) x-session-id stickiness across repeated unary calls
//
// Topology (all 32xxx ports, must not collide with the 31xxx dev cluster):
//   client -> entry-leaf -> node-1 <-> node-2 -> service-leaf-{1,2}
//   echo-backend-1 registered at service-leaf-1 via creek_registrar
//   echo-backend-2 registered at service-leaf-2 via creek_registrar

#include "echo.grpc.pb.h"
#include "creek/logger.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <optional>
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
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#endif

namespace creek::e2e {

struct TestResult {
    bool ok = false;
    std::string phase = "init";
    std::string detail;
};

namespace fs = std::filesystem;

namespace {

std::string env_or(const char* key, const std::string& fallback) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : fallback;
}

void emit_log(const std::string& tag, const std::string& msg) {
    std::fprintf(stdout, "[e2e-generic] %s: %s\n", tag.c_str(), msg.c_str());
    std::fflush(stdout);
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

bool wait_tcp(int port, int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (tcp_connect_ok("127.0.0.1", port, 200)) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

struct Child {
    std::string tag;
    int pid = -1;
};

std::vector<Child> g_children;

bool spawn(const std::string& tag, const std::string& log_dir, const std::string& exe,
           const std::vector<std::string>& args) {
#ifdef _WIN32
    (void)tag; (void)log_dir; (void)exe; (void)args;
    return false;
#else
    std::string out_path = (fs::path(log_dir) / (tag + ".log")).string();
    std::string err_path = (fs::path(log_dir) / (tag + ".err")).string();
    pid_t pid = fork();
    if (pid < 0) return false;
    if (pid == 0) {
        int fd_out = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int fd_err = ::open(err_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out >= 0) { ::dup2(fd_out, 1); ::close(fd_out); }
        if (fd_err >= 0) { ::dup2(fd_err, 2); ::close(fd_err); }
        std::vector<std::string> argv;
        argv.push_back(exe);
        for (const auto& a : args) argv.push_back(a);
        std::vector<char*> cargv;
        for (auto& a : argv) cargv.push_back(a.data());
        cargv.push_back(nullptr);
        ::execv(exe.c_str(), cargv.data());
        std::exit(127);
    }
    emit_log("spawn", tag + " pid=" + std::to_string(pid));
    g_children.push_back(Child{tag, static_cast<int>(pid)});
    return true;
#endif
}

void kill_all() {
#ifndef _WIN32
    for (auto& c : g_children) {
        if (c.pid > 0) ::kill(c.pid, SIGTERM);
    }
    for (auto& c : g_children) {
        if (c.pid > 0) {
            int status = 0;
            ::waitpid(c.pid, &status, 0);
        }
    }
#endif
    g_children.clear();
}

// ---- generic client helpers ------------------------------------------------

grpc::ByteBuffer bb_from_string(const std::string& s) {
    grpc::Slice slice(s);
    return grpc::ByteBuffer(&slice, 1);
}

std::string bb_to_string(grpc::ByteBuffer& bb) {
    std::vector<grpc::Slice> slices;
    std::string out;
    if (bb.Dump(&slices).ok()) {
        for (const auto& slice : slices) {
            out.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
        }
    }
    return out;
}

grpc::Status generic_unary(const std::shared_ptr<grpc::Channel>& channel,
                           const std::string& method,
                           const std::map<std::string, std::string>& metadata,
                           const std::string& body, std::string* resp_body,
                           int timeout_ms = 10000) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
    for (const auto& kv : metadata) ctx.AddMetadata(kv.first, kv.second);
    grpc::CompletionQueue cq;
    grpc::GenericStub stub(channel);
    grpc::ByteBuffer req_bb = bb_from_string(body);
    grpc::ByteBuffer resp_bb;
    grpc::Status status;
    auto reader = stub.PrepareUnaryCall(&ctx, method, req_bb, &cq);
    if (!reader) return grpc::Status(grpc::StatusCode::INTERNAL, "call_create_failed");
    reader->StartCall();
    reader->Finish(&resp_bb, &status, reinterpret_cast<void*>(1));
    void* tag = nullptr;
    bool ok = false;
    cq.Next(&tag, &ok);
    cq.Shutdown();
    if (status.ok() && resp_body) *resp_body = bb_to_string(resp_bb);
    return status;
}

struct BidiResult {
    bool ok = false;
    std::string detail;
    std::vector<creek::test::EchoReply> replies;
    grpc::Status finish_status;
};

BidiResult generic_bidi(const std::shared_ptr<grpc::Channel>& channel,
                        const std::string& method,
                        const std::map<std::string, std::string>& metadata,
                        const std::vector<creek::test::EchoRequest>& requests) {
    BidiResult result;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    for (const auto& kv : metadata) ctx.AddMetadata(kv.first, kv.second);
    grpc::CompletionQueue cq;
    grpc::GenericStub stub(channel);
    auto rw = stub.PrepareCall(&ctx, method, &cq);
    if (!rw) {
        result.detail = "call_create_failed";
        return result;
    }
    rw->StartCall(reinterpret_cast<void*>(0));
    void* tag = nullptr;
    bool ok = false;
    if (!cq.Next(&tag, &ok) || !ok) {
        result.detail = "start_call_failed";
        return result;
    }
    for (const auto& req : requests) {
        grpc::ByteBuffer bb = bb_from_string(req.SerializeAsString());
        rw->Write(bb, reinterpret_cast<void*>(1));
        if (!cq.Next(&tag, &ok) || !ok) {
            result.detail = "write_failed";
            return result;
        }
    }
    rw->WritesDone(reinterpret_cast<void*>(2));
    if (!cq.Next(&tag, &ok) || !ok) {
        result.detail = "writes_done_failed";
        return result;
    }
    while (true) {
        grpc::ByteBuffer bb;
        rw->Read(&bb, reinterpret_cast<void*>(3));
        if (!cq.Next(&tag, &ok)) {
            result.detail = "read_cq_failed";
            return result;
        }
        if (!ok) break;
        creek::test::EchoReply reply;
        if (!reply.ParseFromString(bb_to_string(bb))) {
            result.detail = "bad_reply_body";
            return result;
        }
        result.replies.push_back(reply);
    }
    rw->Finish(&result.finish_status, reinterpret_cast<void*>(4));
    if (!cq.Next(&tag, &ok)) {
        result.detail = "finish_cq_failed";
        return result;
    }
    cq.Shutdown();
    result.ok = true;
    return result;
}

std::shared_ptr<grpc::Channel> make_channel(int port) {
    return grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                               grpc::InsecureChannelCredentials());
}

creek::test::EchoRequest make_req(const std::string& text, uint32_t seq) {
    creek::test::EchoRequest req;
    req.set_text(text);
    req.set_seq(seq);
    return req;
}

constexpr int kNode1Udp = 32100;
constexpr int kNode1Metrics = 32101;
constexpr int kNode2Udp = 32104;
constexpr int kNode2Metrics = 32105;
constexpr int kEntryUdp = 32108;
constexpr int kEntryGrpc = 32109;
constexpr int kEntryMetrics = 32112;
constexpr int kSvc1Udp = 32116;
constexpr int kSvc1Grpc = 32119;
constexpr int kSvc1Metrics = 32120;
constexpr int kSvc2Udp = 32123;
constexpr int kSvc2Grpc = 32126;
constexpr int kSvc2Metrics = 32127;
constexpr int kBackend1 = 32130;
constexpr int kBackend2 = 32133;

constexpr const char* kEchoService = "creek.test.Echo";
constexpr const char* kEchoMethod = "/creek.test.Echo/Echo";
constexpr const char* kChatMethod = "/creek.test.Echo/Chat";

TestResult run_generic_e2e(const std::string& sidecar, const std::string& echo_backend,
                           const std::string& registrar, const std::string& log_dir,
                           const std::string& token) {
#ifdef _WIN32
    (void)sidecar; (void)echo_backend; (void)registrar; (void)log_dir; (void)token;
    return {false, "platform", "windows not supported by this test"};
#else
    TestResult result;
    std::error_code ec;
    fs::create_directories(log_dir, ec);

    auto node_args = [&](const std::string& id, int udp, int metrics, const std::string& peer) {
        std::vector<std::string> args = {
            "node", "--id", id,
            "--udp", "127.0.0.1:" + std::to_string(udp),
            "--metrics", "127.0.0.1:" + std::to_string(metrics),
            "--sync-ms", "100",
            "--metric-period-ms", "1000",
            "--token", token,
        };
        if (!peer.empty()) {
            args.push_back("--peer");
            args.push_back(peer);
        }
        return args;
    };
    auto leaf_args = [&](const std::string& id, int udp, const std::string& parent,
                         int grpc_port, int metrics) {
        return std::vector<std::string>{
            "leaf", "--id", id,
            "--udp", "127.0.0.1:" + std::to_string(udp),
            "--parent", parent,
            "--grpc", "127.0.0.1:" + std::to_string(grpc_port),
            "--metrics", "127.0.0.1:" + std::to_string(metrics),
            "--sync-ms", "100",
            "--metric-period-ms", "1000",
            "--token", token,
            "--rpc-timeout-ms", "3000",
        };
    };

    if (!spawn("node1", log_dir, sidecar,
               node_args("node-1", kNode1Udp, kNode1Metrics,
                         "node-2@127.0.0.1:" + std::to_string(kNode2Udp)))) {
        return {false, "setup", "node1 spawn failed"};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    if (!spawn("node2", log_dir, sidecar,
               node_args("node-2", kNode2Udp, kNode2Metrics,
                         "node-1@127.0.0.1:" + std::to_string(kNode1Udp)))) {
        return {false, "setup", "node2 spawn failed"};
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    if (!spawn("entry_leaf", log_dir, sidecar,
               leaf_args("entry-leaf", kEntryUdp,
                         "node-1@127.0.0.1:" + std::to_string(kNode1Udp),
                         kEntryGrpc, kEntryMetrics))) {
        return {false, "setup", "entry_leaf spawn failed"};
    }
    if (!spawn("service_leaf1", log_dir, sidecar,
               leaf_args("service-leaf-1", kSvc1Udp,
                         "node-2@127.0.0.1:" + std::to_string(kNode2Udp),
                         kSvc1Grpc, kSvc1Metrics))) {
        return {false, "setup", "service_leaf1 spawn failed"};
    }
    if (!spawn("service_leaf2", log_dir, sidecar,
               leaf_args("service-leaf-2", kSvc2Udp,
                         "node-1@127.0.0.1:" + std::to_string(kNode1Udp),
                         kSvc2Grpc, kSvc2Metrics))) {
        return {false, "setup", "service_leaf2 spawn failed"};
    }
    if (!wait_tcp(kEntryGrpc, 60000) || !wait_tcp(kSvc1Grpc, 60000) ||
        !wait_tcp(kSvc2Grpc, 60000)) {
        return {false, "setup", "leaf gRPC ports not listening"};
    }
    if (!spawn("echo_backend1", log_dir, echo_backend,
               {"--id", "backend-1", "--listen", "127.0.0.1:" + std::to_string(kBackend1)})) {
        return {false, "setup", "echo_backend1 spawn failed"};
    }
    if (!spawn("echo_backend2", log_dir, echo_backend,
               {"--id", "backend-2", "--listen", "127.0.0.1:" + std::to_string(kBackend2)})) {
        return {false, "setup", "echo_backend2 spawn failed"};
    }
    if (!wait_tcp(kBackend1, 15000) || !wait_tcp(kBackend2, 15000)) {
        return {false, "setup", "echo backends not listening"};
    }
    if (!spawn("registrar1", log_dir, registrar,
               {"--leaf", "127.0.0.1:" + std::to_string(kSvc1Grpc),
                "--target", "127.0.0.1:" + std::to_string(kBackend1),
                "--id", "backend-1", "--service", kEchoService})) {
        return {false, "setup", "registrar1 spawn failed"};
    }
    if (!spawn("registrar2", log_dir, registrar,
               {"--leaf", "127.0.0.1:" + std::to_string(kSvc2Grpc),
                "--target", "127.0.0.1:" + std::to_string(kBackend2),
                "--id", "backend-2", "--service", kEchoService})) {
        return {false, "setup", "registrar2 spawn failed"};
    }
    emit_log("setup", "waiting 5s for directory sync");
    std::this_thread::sleep_for(std::chrono::seconds(5));

    auto entry = make_channel(kEntryGrpc);

    // Phase 1: discovery via generic unary through the mesh.
    result.phase = "discovery";
    bool discovered = false;
    std::string discovered_backend;
    for (int i = 1; i <= 30 && !discovered; ++i) {
        std::string resp_body;
        auto status = generic_unary(entry, kEchoMethod, {}, make_req("probe", 0).SerializeAsString(),
                                    &resp_body, 10000);
        if (status.ok()) {
            creek::test::EchoReply reply;
            if (reply.ParseFromString(resp_body) &&
                (reply.backend_id() == "backend-1" || reply.backend_id() == "backend-2")) {
                discovered = true;
                discovered_backend = reply.backend_id();
                emit_log("discovery", "generic unary reached " + discovered_backend);
                break;
            }
        }
        emit_log("discovery", "attempt " + std::to_string(i) + ": " + status.error_message());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (!discovered) return {false, result.phase, "generic unary never succeeded"};

    // Phase 2: generic unary correctness (remote endpoint via entry leaf).
    result.phase = "unary";
    {
        std::string resp_body;
        auto status = generic_unary(entry, kEchoMethod,
                                    {{"x-session-id", "unary-check"}},
                                    make_req("hello-mesh", 7).SerializeAsString(), &resp_body);
        if (!status.ok()) return {false, result.phase, "unary failed: " + status.error_message()};
        creek::test::EchoReply reply;
        if (!reply.ParseFromString(resp_body)) return {false, result.phase, "bad reply body"};
        if (reply.text() != "echo:hello-mesh" || reply.seq() != 7) {
            return {false, result.phase, "unexpected reply text=" + reply.text()};
        }
        emit_log("unary", "remote unary ok backend=" + reply.backend_id());
    }
    // Local-endpoint branch: call service-leaf-1's gRPC port directly. The
    // balancer round-robins between backend-1 (local to that leaf) and
    // backend-2 (remote), so retry until the local branch answers.
    {
        auto svc1 = make_channel(kSvc1Grpc);
        bool local_ok = false;
        for (int i = 0; i < 8 && !local_ok; ++i) {
            std::string resp_body;
            auto status = generic_unary(svc1, kEchoMethod, {},
                                        make_req("hello-local", 8).SerializeAsString(), &resp_body);
            if (!status.ok()) return {false, result.phase, "local unary failed: " + status.error_message()};
            creek::test::EchoReply reply;
            if (!reply.ParseFromString(resp_body)) return {false, result.phase, "bad local reply body"};
            if (reply.text() != "echo:hello-local") {
                return {false, result.phase, "unexpected local reply text=" + reply.text()};
            }
            if (reply.backend_id() == "backend-1") local_ok = true;
        }
        if (!local_ok) return {false, result.phase, "local endpoint never answered on service-leaf-1"};
        emit_log("unary", "local unary ok backend=backend-1");
    }

    // Phase 3: bidi streaming through the mesh (multi-message, half-close).
    result.phase = "bidi";
    {
        std::vector<creek::test::EchoRequest> requests;
        for (uint32_t i = 0; i < 5; ++i) {
            requests.push_back(make_req("chunk-" + std::to_string(i), i));
        }
        auto r = generic_bidi(entry, kChatMethod, {{"x-session-id", "bidi-1"}}, requests);
        if (!r.ok) return {false, result.phase, "bidi failed: " + r.detail};
        if (!r.finish_status.ok()) {
            return {false, result.phase, "bidi finish: " + r.finish_status.error_message()};
        }
        if (r.replies.size() != requests.size()) {
            return {false, result.phase,
                    "expected 5 replies, got " + std::to_string(r.replies.size())};
        }
        const std::string backend = r.replies.front().backend_id();
        for (uint32_t i = 0; i < requests.size(); ++i) {
            const auto& reply = r.replies[i];
            if (reply.seq() != i || reply.text() != "echo:chunk-" + std::to_string(i)) {
                return {false, result.phase,
                        "reply " + std::to_string(i) + " out of order/corrupt: " + reply.text()};
            }
            if (reply.backend_id() != backend) {
                return {false, result.phase, "backend flipped mid-stream"};
            }
        }
        if (backend != "backend-1" && backend != "backend-2") {
            return {false, result.phase, "unexpected backend " + backend};
        }
        emit_log("bidi", "bidi stream ok, 5/5 echoes in order from " + backend);
    }

    // Phase 4: stickiness — same x-session-id must land on one backend.
    result.phase = "sticky";
    for (const char* session : {"sess-alpha", "sess-beta"}) {
        std::optional<std::string> selected;
        for (int i = 0; i < 8; ++i) {
            std::string resp_body;
            auto status = generic_unary(entry, kEchoMethod, {{"x-session-id", session}},
                                        make_req("sticky", i).SerializeAsString(), &resp_body);
            if (!status.ok()) {
                return {false, result.phase,
                        std::string("sticky call failed: ") + status.error_message()};
            }
            creek::test::EchoReply reply;
            if (!reply.ParseFromString(resp_body)) return {false, result.phase, "bad sticky reply"};
            if (!selected) {
                selected = reply.backend_id();
            } else if (*selected != reply.backend_id()) {
                return {false, result.phase,
                        std::string("sticky flipped ") + *selected + " -> " + reply.backend_id()};
            }
        }
        emit_log("sticky", std::string("session ") + session + " pinned to " + *selected);
    }

    emit_log("done", "ALL CHECKS PASSED");
    result.ok = true;
    return result;
#endif
}

}  // namespace

TestResult run_e2e_generic_entry() {
    std::string sidecar = env_or("CREEK_SIDECAR", "");
    std::string echo_backend = env_or("CREEK_ECHO_BACKEND", "");
    std::string registrar = env_or("CREEK_REGISTRAR", "");
    std::string log_dir = env_or("CREEK_E2E_LOG_DIR", "./e2e-logs");
    std::string token = env_or("CREEK_E2E_TOKEN", "test-e2e-token");
    if (sidecar.empty() || echo_backend.empty() || registrar.empty()) {
        return {false, "env", "CREEK_SIDECAR/CREEK_ECHO_BACKEND/CREEK_REGISTRAR not set"};
    }
    auto result = run_generic_e2e(sidecar, echo_backend, registrar, log_dir, token);
    kill_all();
    return result;
}

}  // namespace creek::e2e
