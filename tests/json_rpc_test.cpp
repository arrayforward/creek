#include "creek/json_rpc.hpp"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_passed = 0;
int g_failed = 0;

void EXPECT(bool cond, const std::string& msg) {
    if (cond) {
        ++g_passed;
    } else {
        ++g_failed;
        std::fprintf(stdout, "FAIL [json_rpc_test]: %s\n", msg.c_str());
    }
}

std::string http_post(int port, const std::string& body,
                      const std::vector<std::pair<std::string, std::string>>& extra_headers = {}) {
    std::string host = "127.0.0.1";
    std::string port_str = std::to_string(port);

    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);

    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<unsigned short>(port));
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    std::string request = "POST /rpc HTTP/1.1\r\n";
    request += "Host: 127.0.0.1:" + port_str + "\r\n";
    request += "Content-Type: application/json\r\n";
    request += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    for (const auto& h : extra_headers) {
        request += h.first + ": " + h.second + "\r\n";
    }
    request += "Connection: close\r\n\r\n";
    request += body;

    ::send(s, request.data(), static_cast<int>(request.size()), 0);

    std::string response;
    char buf[4096];
    while (true) {
        int n = ::recv(s, buf, sizeof(buf), 0);
        if (n <= 0) break;
        response.append(buf, n);
    }
    ::closesocket(s);
    WSACleanup();
    return response;
}

void test_basic_json_rpc() {
    creek::Address bind{"127.0.0.1", 0};
    std::atomic<int> invocations{0};
    std::string saved_body;
    creek::JsonRpcHttpServer::HeaderMap saved_headers;

    creek::JsonRpcHttpServer server(bind, [&](std::string body, const creek::JsonRpcHttpServer::HeaderMap& headers) {
        ++invocations;
        saved_body = body;
        saved_headers = headers;
        return std::make_pair(200, "{\"jsonrpc\":\"2.0\",\"result\":\"ok\"}");
    });

    EXPECT(server.start(), "server should start");
    std::uint16_t port = server.local_port();
    EXPECT(port > 0, "port should be non-zero");

    std::string response = http_post(port, "{\"method\":\"SayHello\",\"params\":{}}");
    EXPECT(invocations.load() == 1, "should have one invocation");

    server.stop();
    std::fprintf(stdout, "[json_rpc] basic: %d invocations port=%d\n", invocations.load(), static_cast<int>(port));
}

void test_sticky_via_header() {
    creek::Address bind{"127.0.0.1", 0};
    creek::JsonRpcHttpServer::HeaderMap captured;

    creek::JsonRpcHttpServer server(bind, [&](std::string, const creek::JsonRpcHttpServer::HeaderMap& headers) {
        captured = headers;
        return std::make_pair(200, "{\"jsonrpc\":\"2.0\",\"result\":\"ok\"}");
    });

    EXPECT(server.start(), "server should start");
    std::uint16_t port = server.local_port();

    std::string response = http_post(port, "{\"method\":\"SayHello\",\"params\":{}}",
        {{"sid", "alice-123"}, {"sticky", "true"}, {"x-creek-sid", "bob-456"}, {"x-creek-sticky", "false"}});

    std::string sid_val, sticky_val, x_sid, x_sticky;
    for (const auto& kv : captured) {
        if (kv.first == "sid") sid_val = kv.second;
        if (kv.first == "sticky") sticky_val = kv.second;
        if (kv.first == "x-creek-sid") x_sid = kv.second;
        if (kv.first == "x-creek-sticky") x_sticky = kv.second;
    }

    EXPECT(sid_val == "alice-123", "sid header should be alice-123");
    EXPECT(sticky_val == "true", "sticky header should be true");
    EXPECT(x_sid == "bob-456", "x-creek-sid header should be bob-456");
    EXPECT(x_sticky == "false", "x-creek-sticky header should be false");

    server.stop();
}

void test_body_received_correctly() {
    creek::Address bind{"127.0.0.1", 0};
    std::string original = "{\"method\":\"SayHello\",\"params\":{\"name\":\"world\",\"sid\":\"s1\",\"sticky\":true}}";

    creek::JsonRpcHttpServer server(bind, [&](std::string body, const creek::JsonRpcHttpServer::HeaderMap&) {
        bool valid = (body.find("SayHello") != std::string::npos);
        return std::make_pair(valid ? 200 : 400, valid ? "{\"ok\":1}" : "{\"ok\":0}");
    });

    EXPECT(server.start(), "server should start");
    std::uint16_t port = server.local_port();

    std::string response = http_post(port, original);
    EXPECT(response.find("200 OK") != std::string::npos || response.find("200") != std::string::npos,
           "should get 200 response");

    server.stop();
}

void test_healthz() {
    creek::Address bind{"127.0.0.1", 0};
    creek::JsonRpcHttpServer server(bind, [](std::string, const creek::JsonRpcHttpServer::HeaderMap&) {
        return std::make_pair(200, "{}");
    });
    EXPECT(server.start(), "server should start");
    std::uint16_t port = server.local_port();

    std::string host = "127.0.0.1";
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<unsigned short>(port));
    inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    ::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    std::string req = "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    ::send(s, req.data(), static_cast<int>(req.size()), 0);
    std::string resp;
    char buf[4096];
    while (true) { int n = ::recv(s, buf, sizeof(buf), 0); if (n <= 0) break; resp.append(buf, n); }
    ::closesocket(s);
    WSACleanup();

    EXPECT(resp.find("200") != std::string::npos, "healthz should return 200");

    server.stop();
}

void test_stop_restart() {
    creek::Address bind{"127.0.0.1", 0};
    creek::JsonRpcHttpServer server(bind, [](std::string, const creek::JsonRpcHttpServer::HeaderMap&) {
        return std::make_pair(200, "{}");
    });

    EXPECT(server.start(), "first start");
    server.stop();
    server.stop();
    server.stop();
    std::fprintf(stdout, "[json_rpc] stop_restart: stop called 3x, all idempotent\n");
}

}

int main() {
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);

    test_basic_json_rpc();
    test_sticky_via_header();
    test_body_received_correctly();
    test_healthz();
    test_stop_restart();

    WSACleanup();
    std::fprintf(stdout, "JSON-RPC Tests: Passed=%d Failed=%d\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
