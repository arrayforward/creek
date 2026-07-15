#include "creek/json_rpc.hpp"

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace creek {

namespace {

constexpr std::size_t kMaxBodyBytes = static_cast<std::size_t>(4) * 1024 * 1024;
constexpr std::size_t kRecvChunk = 4096;
constexpr std::size_t kHeaderBufReserve = 16u * 1024u;
constexpr char kHeaderTerminator[] = "\r\n\r\n";

#if defined(_WIN32) && !defined(_BIG_ENDIAN) && !defined(__BIG_ENDIAN__)
constexpr SOCKET kInvalid = INVALID_SOCKET;
#else
constexpr int kInvalid = -1;
#endif

std::string to_lower(std::string s) {
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}

void trim(std::string& s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.erase(0, 1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r')) s.pop_back();
}

std::string status_reason(int code) {
    if (code == 200) return "OK";
    if (code == 400) return "Bad Request";
    if (code == 404) return "Not Found";
    if (code == 405) return "Method Not Allowed";
    if (code == 411) return "Length Required";
    if (code == 413) return "Payload Too Large";
    if (code == 500) return "Internal Server Error";
    return "Unknown";
}

void send_response(SOCKET sock, int status, const std::string& reason, const std::string& body) {
    std::ostringstream resp;
    resp << "HTTP/1.1 " << status << ' ' << reason << "\r\n";
    resp << "Content-Type: application/json\r\n";
    resp << "Content-Length: " << body.size() << "\r\n";
    resp << "Connection: close\r\n";
    resp << "\r\n";
    resp << body;
    std::string s = resp.str();
    ::send(sock, s.data(), static_cast<int>(s.size()), 0);
}

}

class JsonRpcHttpServer::Impl {
public:
    Impl(Address bind, Handler handler)
        : bind_(std::move(bind)), handler_(std::move(handler)) {}

    ~Impl() { stop(); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    bool start() {
        if (running_.load()) return true;
#if defined(_WIN32)
        WSADATA wsa;
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return false;
#endif
        socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_ == kInvalid) return false;

        int opt = 1;
        ::setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(bind_.port);
#if defined(_WIN32)
        inet_pton(AF_INET, bind_.host.c_str(), &addr.sin_addr);
#else
        inet_pton(AF_INET, bind_.host.c_str(), &addr.sin_addr);
#endif
        if (::bind(socket_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            close_socket();
            return false;
        }
        if (::listen(socket_, 16) != 0) {
            close_socket();
            return false;
        }

        sockaddr_in actual{};
        socklen_t alen = sizeof(actual);
        if (::getsockname(socket_, reinterpret_cast<sockaddr*>(&actual), &alen) == 0) {
            actual_port_ = ntohs(actual.sin_port);
        }

        running_.store(true);
        thread_ = std::thread([this] { run_loop(); });
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        close_socket();
        if (thread_.joinable()) thread_.join();
    }

    std::uint16_t local_port() const { return actual_port_; }

private:
    void close_socket() {
        if (socket_ == kInvalid) return;
#ifdef _WIN32
        ::closesocket(socket_);
        WSACleanup();
#else
        ::close(socket_);
#endif
        socket_ = kInvalid;
    }

    void run_loop() {
        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t clen = sizeof(client_addr);
            SOCKET client = ::accept(socket_, reinterpret_cast<sockaddr*>(&client_addr), &clen);
            if (client == kInvalid) {
                if (!running_.load()) break;
                continue;
            }
            handle_client(client);
#ifdef _WIN32
            ::closesocket(client);
#else
            ::close(client);
#endif
        }
    }

    static bool recv_until(SOCKET client, std::string& buf, std::size_t limit) {
        buf.clear();
        buf.reserve(limit);
        while (buf.size() < limit) {
            char tmp[4096];
            int n = ::recv(client, tmp, sizeof(tmp), 0);
            if (n <= 0) return false;
            buf.append(tmp, static_cast<std::size_t>(n));
            if (buf.find(kHeaderTerminator) != std::string::npos) return true;
        }
        return buf.find(kHeaderTerminator) != std::string::npos;
    }

    bool parse_request_line(const std::string& header_block, std::string& method, std::string& path) {
        auto line_end = header_block.find("\r\n");
        if (line_end == std::string::npos) return false;
        std::string start = header_block.substr(0, line_end);
        auto first = start.find(' ');
        if (first == std::string::npos) return false;
        method = start.substr(0, first);
        auto second = start.find(' ', first + 1);
        path = start.substr(first + 1, second - first - 1);
        return !method.empty() && !path.empty();
    }

    bool parse_content_length(const std::string& header_block,
                              std::size_t& content_length, bool& has_value) {
        content_length = 0;
        has_value = false;
        std::size_t cursor = 0;
        while (cursor < header_block.size()) {
            auto next = header_block.find("\r\n", cursor);
            if (next == std::string::npos) next = header_block.size();
            if (next == cursor) break;
            std::string line = header_block.substr(cursor, next - cursor);
            cursor = next + 2;
            auto colon = line.find(':');
            if (colon == std::string::npos) continue;
            if (to_lower(line.substr(0, colon)) != "content-length") continue;
            std::string value = line.substr(colon + 1);
            trim(value);
            if (value.empty()) return false;
            char* endp = nullptr;
            unsigned long long v = std::strtoull(value.c_str(), &endp, 10);
            if (endp == nullptr || *endp != '\0') return false;
            content_length = static_cast<std::size_t>(v);
            has_value = true;
            return true;
        }
        return true;
    }

    HeaderMap parse_headers(const std::string& header_block) {
        HeaderMap result;
        std::size_t cursor = 0;
        while (cursor < header_block.size()) {
            auto next = header_block.find("\r\n", cursor);
            if (next == std::string::npos) next = header_block.size();
            if (next == cursor) break;
            std::string line = header_block.substr(cursor, next - cursor);
            cursor = next + 2;
            auto colon = line.find(':');
            if (colon == std::string::npos) continue;
            std::string key = to_lower(line.substr(0, colon));
            std::string val = line.substr(colon + 1);
            trim(val);
            result[key] = val;
        }
        return result;
    }

    void handle_client(SOCKET client) {
        std::string head;
        if (!recv_until(client, head, kHeaderBufReserve)) {
            send_response(client, 400, "Bad Request", "{\"error\":\"bad request\"}");
            return;
        }
        auto header_end = head.find(kHeaderTerminator);
        if (header_end == std::string::npos) {
            send_response(client, 400, "Bad Request", "{\"error\":\"bad request\"}");
            return;
        }
        std::size_t after_headers = header_end + 4;
        std::string header_block = head.substr(0, after_headers);
        std::string prefix_body = head.substr(after_headers);

        std::string method, path;
        if (!parse_request_line(header_block, method, path)) {
            send_response(client, 400, "Bad Request", "{\"error\":\"bad request\"}");
            return;
        }
        std::size_t content_length = 0;
        bool has_cl = false;
        if (!parse_content_length(header_block, content_length, has_cl)) {
            send_response(client, 400, "Bad Request", "{\"error\":\"bad header\"}");
            return;
        }
        if (method == "GET" && path == "/healthz") {
            send_response(client, 200, "OK", "{\"status\":\"ok\"}");
            return;
        }
        if (path != "/rpc") {
            send_response(client, 404, "Not Found", "{\"error\":\"not found\"}");
            return;
        }
        if (method != "POST") {
            send_response(client, 405, "Method Not Allowed", "{\"error\":\"method not allowed\"}");
            return;
        }
        if (!has_cl) {
            send_response(client, 411, "Length Required", "{\"error\":\"content-length required\"}");
            return;
        }
        if (content_length > kMaxBodyBytes) {
            send_response(client, 413, "Payload Too Large", "{\"error\":\"payload too large\"}");
            return;
        }

        std::string body;
        body.reserve(content_length);
        if (prefix_body.size() >= content_length) {
            body.assign(prefix_body, 0, content_length);
        } else {
            body.append(prefix_body);
            while (body.size() < content_length) {
                char chunk[kRecvChunk];
                std::size_t want = std::min(sizeof(chunk), content_length - body.size());
                int n = ::recv(client, chunk, static_cast<int>(want), 0);
                if (n <= 0) {
                    send_response(client, 400, "Bad Request", "{\"error\":\"incomplete body\"}");
                    return;
                }
                body.append(chunk, static_cast<std::size_t>(n));
            }
        }

        HeaderMap headers = parse_headers(header_block);

        std::pair<int, std::string> result;
        try {
            result = handler_(body, headers);
        } catch (...) {
            send_response(client, 500, "Internal Server Error", "{\"error\":\"internal error\"}");
            return;
        }
        send_response(client, result.first, status_reason(result.first), result.second);
    }

    Address bind_;
    Handler handler_;
    SOCKET socket_{kInvalid};
    std::uint16_t actual_port_{};
    std::atomic<bool> running_{false};
    std::thread thread_;
};

JsonRpcHttpServer::JsonRpcHttpServer(Address bind, Handler handler)
    : impl_(std::make_unique<Impl>(std::move(bind), std::move(handler))) {}
JsonRpcHttpServer::~JsonRpcHttpServer() = default;
bool JsonRpcHttpServer::start() { return impl_->start(); }
void JsonRpcHttpServer::stop() { impl_->stop(); }
std::uint16_t JsonRpcHttpServer::local_port() const { return impl_->local_port(); }

}
