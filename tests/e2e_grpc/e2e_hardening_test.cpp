// End-to-end hardening test (creek_e2e_hardening), all on 33xxx ports:
//   a) dead endpoint removal: SIGKILL the registrar; the endpoint is marked
//      dead, then dropped from every leaf's directory view (cluster-wide
//      convergence via removal tombstones).
//   b) mount retry: leaves start BEFORE the node; once the node comes up
//      the tight handshake retries (500ms backoff, 5s cap) and the leaves
//      mount automatically — proven by a unary call crossing the mesh.
//   c) admin directory: Admin::Directory RPC + `creek_admin_client
//      directory` table output contain the registered endpoint.
//   d) server-streaming through the mesh: Echo/Count (one request +
//      half-close, N streaming replies) delivers every frame.
//   plus flap tolerance: a registrar restarted inside the grace period
//   revives its endpoint without it ever disappearing.
//
// Topology: client -> leaf-a -> node-1 -> leaf-b -> echo-backend
// (leaf-a and leaf-b are started before node-1.)

#include "echo.grpc.pb.h"
#include "creek.grpc.pb.h"
#include "creek/logger.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#ifndef _WIN32
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
    std::fprintf(stdout, "[e2e-hardening] %s: %s\n", tag.c_str(), msg.c_str());
    std::fflush(stdout);
}

#ifndef _WIN32

bool tcp_connect_ok(const std::string& host, int port, int timeout_ms) {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return false;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        ::close(s);
        return false;
    }
    int fl = fcntl(s, F_GETFL, 0);
    if (fl >= 0) fcntl(s, F_SETFL, fl | O_NONBLOCK);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    bool ok = false;
    while (std::chrono::steady_clock::now() < deadline) {
        int r = ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (r == 0) { ok = true; break; }
        if (errno != EINPROGRESS && errno != EALREADY && errno != EWOULDBLOCK) break;
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(s, &fds);
        timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int sr = ::select(s + 1, nullptr, &fds, nullptr, &tv);
        if (sr > 0) {
            int so_error = 0;
            socklen_t len = sizeof(so_error);
            ::getsockopt(s, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &len);
            if (so_error == 0) { ok = true; break; }
        }
    }
    ::close(s);
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

int spawn(const std::string& tag, const std::string& log_dir, const std::string& exe,
          const std::vector<std::string>& args) {
    std::string out_path = (fs::path(log_dir) / (tag + ".log")).string();
    std::string err_path = (fs::path(log_dir) / (tag + ".err")).string();
    pid_t pid = fork();
    if (pid < 0) return -1;
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
    return static_cast<int>(pid);
}

// Run a short-lived command synchronously, capturing stdout to a file.
bool run_capture(const std::string& tag, const std::string& log_dir, const std::string& exe,
                 const std::vector<std::string>& args, std::string* out) {
    std::string out_path = (fs::path(log_dir) / (tag + ".out")).string();
    pid_t pid = fork();
    if (pid < 0) return false;
    if (pid == 0) {
        int fd_out = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_out >= 0) { ::dup2(fd_out, 1); ::close(fd_out); }
        std::vector<std::string> argv;
        argv.push_back(exe);
        for (const auto& a : args) argv.push_back(a);
        std::vector<char*> cargv;
        for (auto& a : argv) cargv.push_back(a.data());
        cargv.push_back(nullptr);
        ::execv(exe.c_str(), cargv.data());
        std::exit(127);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
    std::ifstream in(out_path);
    std::ostringstream ss;
    ss << in.rdbuf();
    *out = ss.str();
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

void kill_pid(int pid, int sig) {
    if (pid <= 0) return;
    ::kill(pid, sig);
    int status = 0;
    ::waitpid(pid, &status, 0);
    for (auto it = g_children.begin(); it != g_children.end(); ++it) {
        if (it->pid == pid) { g_children.erase(it); break; }
    }
}

void kill_all() {
    for (auto& c : g_children) {
        if (c.pid > 0) ::kill(c.pid, SIGTERM);
    }
    for (auto& c : g_children) {
        if (c.pid > 0) {
            int status = 0;
            ::waitpid(c.pid, &status, 0);
        }
    }
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

struct ServerStreamResult {
    bool ok = false;
    std::string detail;
    std::vector<creek::test::EchoReply> replies;
    grpc::Status finish_status;
};

// One request + WritesDone, then read until the stream ends: the client
// shape of a server-streaming call.
ServerStreamResult generic_server_stream(const std::shared_ptr<grpc::Channel>& channel,
                                         const std::string& method,
                                         const creek::test::EchoRequest& request) {
    ServerStreamResult result;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
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
    grpc::ByteBuffer req_bb = bb_from_string(request.SerializeAsString());
    rw->Write(req_bb, reinterpret_cast<void*>(1));
    if (!cq.Next(&tag, &ok) || !ok) {
        result.detail = "write_failed";
        return result;
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

// ---- admin directory helper -------------------------------------------------

std::shared_ptr<grpc::Channel> make_channel(int port) {
    return grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                               grpc::InsecureChannelCredentials());
}

bool fetch_directory(const std::shared_ptr<grpc::Channel>& channel,
                     creek::v1::DirectoryReply* reply) {
    auto stub = creek::v1::Admin::NewStub(channel);
    creek::v1::DirectoryRequest request;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    return stub->Directory(&ctx, request, reply).ok();
}

// endpoints for kEchoService; first element = count, second = alive count
std::pair<int, int> echo_endpoint_stats(const creek::v1::DirectoryReply& reply,
                                        const std::string& service) {
    int total = 0, alive = 0;
    for (const auto& ep : reply.endpoints()) {
        if (ep.service() != service) continue;
        ++total;
        if (ep.alive()) ++alive;
    }
    return {total, alive};
}

// ---- ports (33xxx: must not collide with the 31xxx/32xxx ranges) -----------

constexpr int kNodeUdp = 33100;
constexpr int kNodeMetrics = 33101;
constexpr int kLeafAUdp = 33110;
constexpr int kLeafAGrpc = 33111;
constexpr int kLeafAMetrics = 33112;
constexpr int kLeafBUdp = 33120;
constexpr int kLeafBGrpc = 33121;
constexpr int kLeafBMetrics = 33122;
constexpr int kBackend = 33130;

constexpr const char* kEchoService = "creek.test.Echo";
constexpr const char* kEchoMethod = "/creek.test.Echo/Echo";
constexpr const char* kCountMethod = "/creek.test.Echo/Count";

creek::test::EchoRequest make_req(const std::string& text, uint32_t seq) {
    creek::test::EchoRequest req;
    req.set_text(text);
    req.set_seq(seq);
    return req;
}

TestResult run_hardening_e2e(const std::string& sidecar, const std::string& echo_backend,
                             const std::string& registrar, const std::string& admin_client,
                             const std::string& log_dir, const std::string& token) {
    TestResult result;
    std::error_code ec;
    fs::create_directories(log_dir, ec);

    auto leaf_args = [&](const std::string& id, int udp, int grpc_port, int metrics) {
        return std::vector<std::string>{
            "leaf", "--id", id,
            "--udp", "127.0.0.1:" + std::to_string(udp),
            "--parent", "node-1@127.0.0.1:" + std::to_string(kNodeUdp),
            "--grpc", "127.0.0.1:" + std::to_string(grpc_port),
            "--metrics", "127.0.0.1:" + std::to_string(metrics),
            "--sync-ms", "200",
            "--metric-period-ms", "1000",
            "--token", token,
            "--rpc-timeout-ms", "3000",
        };
    };

    // Phase 0 (task b setup): leaves start BEFORE the node exists.
    result.phase = "mount-retry";
    if (spawn("leaf_a", log_dir, sidecar,
              leaf_args("leaf-a", kLeafAUdp, kLeafAGrpc, kLeafAMetrics)) < 0) {
        return {false, "setup", "leaf_a spawn failed"};
    }
    if (spawn("leaf_b", log_dir, sidecar,
              leaf_args("leaf-b", kLeafBUdp, kLeafBGrpc, kLeafBMetrics)) < 0) {
        return {false, "setup", "leaf_b spawn failed"};
    }
    emit_log("mount-retry", "leaves up, node down; waiting 3s");
    std::this_thread::sleep_for(std::chrono::seconds(3));
    if (spawn("node1", log_dir, sidecar,
              {"node", "--id", "node-1",
               "--udp", "127.0.0.1:" + std::to_string(kNodeUdp),
               "--metrics", "127.0.0.1:" + std::to_string(kNodeMetrics),
               "--sync-ms", "200",
               "--metric-period-ms", "1000",
               "--token", token}) < 0) {
        return {false, "setup", "node1 spawn failed"};
    }
    if (!wait_tcp(kLeafAGrpc, 30000) || !wait_tcp(kLeafBGrpc, 30000)) {
        return {false, "setup", "leaf gRPC ports not listening"};
    }
    if (spawn("echo_backend", log_dir, echo_backend,
              {"--id", "backend-1", "--listen", "127.0.0.1:" + std::to_string(kBackend)}) < 0) {
        return {false, "setup", "echo_backend spawn failed"};
    }
    if (!wait_tcp(kBackend, 15000)) {
        return {false, "setup", "echo backend not listening"};
    }
    if (spawn("registrar", log_dir, registrar,
              {"--leaf", "127.0.0.1:" + std::to_string(kLeafBGrpc),
               "--target", "127.0.0.1:" + std::to_string(kBackend),
               "--id", "backend-1", "--service", kEchoService}) < 0) {
        return {false, "setup", "registrar spawn failed"};
    }

    // The late-started node must accept the retried handshakes; both leaves
    // mount and the endpoint becomes callable through the mesh.
    auto entry = make_channel(kLeafAGrpc);
    bool mounted = false;
    for (int i = 1; i <= 40 && !mounted; ++i) {
        std::string resp_body;
        auto status = generic_unary(entry, kEchoMethod, {},
                                    make_req("probe", 0).SerializeAsString(), &resp_body, 3000);
        if (status.ok()) {
            creek::test::EchoReply reply;
            if (reply.ParseFromString(resp_body) && reply.backend_id() == "backend-1") {
                mounted = true;
                emit_log("mount-retry", "mesh callable after node late start, attempt " +
                         std::to_string(i));
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    if (!mounted) {
        return {false, result.phase, "leaf->node mount never completed after late node start"};
    }

    // Unary regression: one request + half-close through the (merged) stream
    // path still answers exactly one reply, also on the local branch.
    result.phase = "unary";
    {
        std::string resp_body;
        auto status = generic_unary(entry, kEchoMethod, {},
                                    make_req("hello-mesh", 7).SerializeAsString(), &resp_body);
        if (!status.ok()) return {false, result.phase, "unary failed: " + status.error_message()};
        creek::test::EchoReply reply;
        if (!reply.ParseFromString(resp_body) || reply.text() != "echo:hello-mesh" ||
            reply.seq() != 7) {
            return {false, result.phase, "unexpected unary reply"};
        }
        auto leaf_b = make_channel(kLeafBGrpc);
        resp_body.clear();
        status = generic_unary(leaf_b, kEchoMethod, {},
                               make_req("hello-local", 8).SerializeAsString(), &resp_body);
        if (!status.ok()) {
            return {false, result.phase, "local unary failed: " + status.error_message()};
        }
        if (!reply.ParseFromString(resp_body) || reply.text() != "echo:hello-local") {
            return {false, result.phase, "unexpected local unary reply"};
        }
        emit_log("unary", "remote + local unary ok");
    }

    // Phase (d): server-streaming through the mesh — all N frames arrive.
    result.phase = "server-streaming";
    {
        constexpr uint32_t kCount = 5;
        auto r = generic_server_stream(entry, kCountMethod, make_req("tick", kCount));
        if (!r.ok) return {false, result.phase, "count failed: " + r.detail};
        if (!r.finish_status.ok()) {
            return {false, result.phase, "count finish: " + r.finish_status.error_message()};
        }
        if (r.replies.size() != kCount) {
            return {false, result.phase,
                    "expected 5 streaming replies, got " + std::to_string(r.replies.size())};
        }
        for (uint32_t i = 0; i < kCount; ++i) {
            if (r.replies[i].seq() != i || r.replies[i].text() != "echo:tick" ||
                r.replies[i].backend_id() != "backend-1") {
                return {false, result.phase,
                        "stream reply " + std::to_string(i) + " corrupt: " + r.replies[i].text()};
            }
        }
        emit_log("server-streaming", "count ok, 5/5 frames in order");
    }

    // Phase (c): admin directory — RPC and CLI table both show the endpoint.
    result.phase = "admin-directory";
    {
        creek::v1::DirectoryReply reply;
        if (!fetch_directory(entry, &reply)) {
            return {false, result.phase, "Directory RPC failed"};
        }
        auto [total, alive] = echo_endpoint_stats(reply, kEchoService);
        if (total != 1 || alive != 1) {
            return {false, result.phase,
                    "directory RPC total=" + std::to_string(total) +
                    " alive=" + std::to_string(alive)};
        }
        bool fields_ok = false;
        for (const auto& ep : reply.endpoints()) {
            if (ep.service() == kEchoService && ep.endpoint_id() == "backend-1" &&
                ep.target() == "127.0.0.1:" + std::to_string(kBackend) &&
                ep.owner_leaf() == "leaf-b" && ep.owner_node() == "node-1" && ep.alive()) {
                fields_ok = true;
            }
        }
        if (!fields_ok) return {false, result.phase, "directory endpoint fields wrong"};

        std::string out;
        if (!run_capture("admin_directory", log_dir, admin_client,
                         {"--target", "127.0.0.1:" + std::to_string(kLeafAGrpc), "directory"},
                         &out)) {
            return {false, result.phase, "creek_admin_client directory failed"};
        }
        for (const char* needle : {"creek.test.Echo", "backend-1", "127.0.0.1:33130",
                                   "leaf-b", "node-1", "true", "ENDPOINT_ID"}) {
            if (out.find(needle) == std::string::npos) {
                return {false, result.phase,
                        std::string("admin directory output missing '") + needle + "': " + out};
            }
        }
        emit_log("admin-directory", "Directory RPC + CLI table ok");
    }

    // Flap tolerance: kill the registrar; inside the grace period the
    // endpoint turns alive=false but must NOT disappear; a restarted
    // registrar revives it.
    result.phase = "flap";
    int registrar_pid = -1;
    for (const auto& c : g_children) {
        if (c.tag == "registrar") registrar_pid = c.pid;
    }
    if (registrar_pid < 0) return {false, result.phase, "registrar pid not found"};
    {
        kill_pid(registrar_pid, SIGKILL);
        emit_log("flap", "registrar SIGKILLed; waiting 5s (dead but inside grace)");
        std::this_thread::sleep_for(std::chrono::seconds(5));
        creek::v1::DirectoryReply reply;
        if (!fetch_directory(entry, &reply)) {
            return {false, result.phase, "Directory RPC failed after kill"};
        }
        auto [total, alive] = echo_endpoint_stats(reply, kEchoService);
        if (total != 1) {
            return {false, result.phase,
                    "endpoint disappeared inside grace period (total=" + std::to_string(total) + ")"};
        }
        if (alive != 0) {
            return {false, result.phase, "endpoint not marked dead after 5s without heartbeat"};
        }
        emit_log("flap", "endpoint dead-but-present; restarting registrar");
        if (spawn("registrar2", log_dir, registrar,
                  {"--leaf", "127.0.0.1:" + std::to_string(kLeafBGrpc),
                   "--target", "127.0.0.1:" + std::to_string(kBackend),
                   "--id", "backend-1", "--service", kEchoService}) < 0) {
            return {false, result.phase, "registrar restart failed"};
        }
        bool revived = false;
        for (int i = 0; i < 20 && !revived; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            creek::v1::DirectoryReply r2;
            if (!fetch_directory(entry, &r2)) continue;
            auto [t2, a2] = echo_endpoint_stats(r2, kEchoService);
            if (t2 == 1 && a2 == 1) revived = true;
        }
        if (!revived) {
            return {false, result.phase, "endpoint did not revive after registrar restart"};
        }
        emit_log("flap", "endpoint revived without disappearing");
    }

    // Phase (a): SIGKILL the registrar for good; the endpoint must be
    // removed everywhere (entry leaf view and owner leaf view).
    result.phase = "dead-removal";
    {
        registrar_pid = -1;
        for (const auto& c : g_children) {
            if (c.tag == "registrar2") registrar_pid = c.pid;
        }
        if (registrar_pid < 0) return {false, result.phase, "registrar2 pid not found"};
        kill_pid(registrar_pid, SIGKILL);
        emit_log("dead-removal", "registrar SIGKILLed; waiting for directory convergence");
        auto leaf_b = make_channel(kLeafBGrpc);
        bool converged_entry = false, converged_owner = false;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline &&
               (!converged_entry || !converged_owner)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            creek::v1::DirectoryReply r_entry, r_owner;
            if (!converged_entry && fetch_directory(entry, &r_entry)) {
                converged_entry = echo_endpoint_stats(r_entry, kEchoService).first == 0;
            }
            if (!converged_owner && fetch_directory(leaf_b, &r_owner)) {
                converged_owner = echo_endpoint_stats(r_owner, kEchoService).first == 0;
            }
        }
        if (!converged_entry) {
            return {false, result.phase, "entry leaf still sees the dead endpoint"};
        }
        if (!converged_owner) {
            return {false, result.phase, "owner leaf still sees the dead endpoint"};
        }
        emit_log("dead-removal", "directory converged: dead endpoint removed everywhere");
    }

    emit_log("done", "ALL CHECKS PASSED");
    result.ok = true;
    return result;
}

#endif  // !_WIN32

}  // namespace

TestResult run_e2e_hardening_entry() {
#ifdef _WIN32
    return {false, "platform", "windows not supported by this test"};
#else
    std::string sidecar = env_or("CREEK_SIDECAR", "");
    std::string echo_backend = env_or("CREEK_ECHO_BACKEND", "");
    std::string registrar = env_or("CREEK_REGISTRAR", "");
    std::string admin_client = env_or("CREEK_ADMIN_CLIENT", "");
    std::string log_dir = env_or("CREEK_E2E_LOG_DIR", "./e2e-logs");
    std::string token = env_or("CREEK_E2E_TOKEN", "test-e2e-token");
    if (sidecar.empty() || echo_backend.empty() || registrar.empty() || admin_client.empty()) {
        return {false, "env", "CREEK_SIDECAR/CREEK_ECHO_BACKEND/CREEK_REGISTRAR/CREEK_ADMIN_CLIENT not set"};
    }
    auto result = run_hardening_e2e(sidecar, echo_backend, registrar, admin_client, log_dir, token);
    kill_all();
    return result;
#endif
}

}  // namespace creek::e2e
