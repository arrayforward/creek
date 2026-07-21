#include "creek/metrics.hpp"
#include "creek/types.hpp"
#include "creek.pb.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "socket_compat.hpp"

namespace creek {

namespace {

constexpr char kKeySeparator = '\x1f';

std::string escape_openmetrics_label(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            case '\n': out += "\\n"; break;
            default:   out += c;
        }
    }
    return out;
}

std::string escape_json_string(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

void split_key(const std::string& key, std::string& direction,
               std::string& rpc_name, std::string& metadata) {
    auto sep1 = key.find(kKeySeparator);
    if (sep1 == std::string::npos) {
        direction = key;
        return;
    }
    direction.assign(key, 0, sep1);
    auto sep2 = key.find(kKeySeparator, sep1 + 1);
    if (sep2 == std::string::npos) {
        rpc_name.assign(key, sep1 + 1, std::string::npos);
        return;
    }
    rpc_name.assign(key, sep1 + 1, sep2 - sep1 - 1);
    metadata.assign(key, sep2 + 1, std::string::npos);
}

void append_metric_lines(std::ostringstream& os, const std::string& name,
                         const std::string& help, const std::string& type,
                         const std::string& field, const MetricMap& source) {
    os << "# HELP " << name << ' ' << help << "\n";
    os << "# TYPE " << name << ' ' << type << "\n";
    for (const auto& kv : source) {
        std::string direction, rpc_name, metadata;
        split_key(kv.first, direction, rpc_name, metadata);
        std::uint64_t value = 0;
        if (field == "calls") value = kv.second.calls;
        else if (field == "errors") value = kv.second.errors;
        else if (field == "bytes") value = kv.second.bytes;
        else if (field == "latency_us") value = kv.second.latency_us;
        os << name
           << "{direction=\"" << escape_openmetrics_label(direction) << "\""
           << ",rpc=\"" << escape_openmetrics_label(rpc_name) << "\""
           << ",metadata=\"" << escape_openmetrics_label(metadata) << "\"} "
           << value << '\n';
    }
}

}

MetricsStore::MetricsStore(std::chrono::milliseconds period)
    : period_(period), period_start_(SteadyClock::now()) {}

void MetricsStore::rotate_locked(SteadyClock::time_point now) {
    if (period_.count() <= 0) return;
    if (now - period_start_ < period_) return;
    previous_ = std::move(current_);
    current_.clear();
    period_start_ = now;
}

std::string MetricsStore::key(const MetricEvent& event) {
    std::string k;
    k.reserve(event.direction.size() + event.rpc_name.size() + 32);
    k.append(event.direction);
    k.push_back(kKeySeparator);
    k.append(event.rpc_name);
    k.push_back(kKeySeparator);
    k.append(metadata_string(event.metadata));
    return k;
}

void MetricsStore::record(const MetricEvent& event) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto now = SteadyClock::now();
    rotate_locked(now);

    const std::string k = key(event);
    auto& v = current_[k];
    ++v.calls;
    if (!event.success) ++v.errors;
    v.bytes += event.bytes;
    v.latency_us += event.latency_us;

    auto& cv = cumulative_[k];
    ++cv.calls;
    if (!event.success) ++cv.errors;
    cv.bytes += event.bytes;
    cv.latency_us += event.latency_us;
}

MetricMap MetricsStore::previous() {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());
    return previous_;
}

MetricMap MetricsStore::current() {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());
    return current_;
}

MetricMap MetricsStore::take() {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());
    MetricMap result;
    result.swap(cumulative_);
    return result;
}

creek::v1::MetricReply MetricsStore::protobuf_snapshot(bool previous_minute, bool take_values) {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());

    MetricMap data;
    if (previous_minute) {
        data = previous_;
    } else if (take_values) {
        data.swap(cumulative_);
    } else {
        data = current_;
    }

    creek::v1::MetricReply reply;
    for (const auto& kv : data) {
        std::string direction, rpc_name, metadata;
        split_key(kv.first, direction, rpc_name, metadata);
        auto* point = reply.add_points();
        point->set_direction(std::move(direction));
        point->set_rpc_name(std::move(rpc_name));
        point->set_metadata(std::move(metadata));
        point->set_calls(kv.second.calls);
        point->set_errors(kv.second.errors);
        point->set_bytes(kv.second.bytes);
        point->set_latency_us(kv.second.latency_us);
    }
    return reply;
}

std::string MetricsStore::openmetrics() {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());

    std::ostringstream os;
    append_metric_lines(os, "creek_rpc_calls_total",
                        "Total number of RPC calls handled.",
                        "counter", "calls", current_);
    append_metric_lines(os, "creek_rpc_errors_total",
                        "Total number of RPC errors observed.",
                        "counter", "errors", current_);
    append_metric_lines(os, "creek_rpc_bytes_total",
                        "Total bytes transferred by RPCs.",
                        "counter", "bytes", current_);
    append_metric_lines(os, "creek_rpc_latency_microseconds_total",
                        "Total RPC latency in microseconds.",
                        "counter", "latency_us", current_);
    os << "# EOF\n";
    return os.str();
}

std::string MetricsStore::json(bool previous_minute, bool take_values) {
    std::lock_guard<std::mutex> lock(mutex_);
    rotate_locked(SteadyClock::now());

    MetricMap data;
    if (previous_minute) {
        data = previous_;
    } else if (take_values) {
        data.swap(cumulative_);
    } else {
        data = current_;
    }

    std::ostringstream os;
    os << "{\"points\":[";
    bool first = true;
    for (const auto& kv : data) {
        std::string direction, rpc_name, metadata;
        split_key(kv.first, direction, rpc_name, metadata);
        if (!first) os << ',';
        first = false;
        os << "{\"direction\":\"" << escape_json_string(direction) << "\","
           << "\"rpc_name\":\"" << escape_json_string(rpc_name) << "\","
           << "\"metadata\":\"" << escape_json_string(metadata) << "\","
           << "\"calls\":" << kv.second.calls << ","
           << "\"errors\":" << kv.second.errors << ","
           << "\"bytes\":" << kv.second.bytes << ","
           << "\"latency_us\":" << kv.second.latency_us << '}';
    }
    os << "]}";
    return os.str();
}

class MetricsHttpServer::Impl {
public:
    Impl(std::shared_ptr<MetricsStore> store, Address bind)
        : store_(std::move(store)), bind_(std::move(bind)) {}

    ~Impl() { stop(); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    bool start() {
        if (running_.load()) return true;
        if (!wsa_initialized_.load()) {
            if (!socket_startup()) return false;
            wsa_initialized_.store(true);
        }

        socket_ = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_ == INVALID_SOCKET) {
            cleanup_socket();
            return false;
        }

        char opt = 1;
        setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(bind_.port);
        if (inet_pton(AF_INET, bind_.host.c_str(), &addr.sin_addr) != 1) {
            cleanup_socket();
            return false;
        }

        if (::bind(socket_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
            cleanup_socket();
            return false;
        }

        if (::listen(socket_, 16) == SOCKET_ERROR) {
            cleanup_socket();
            return false;
        }

        if (bind_.port == 0) {
            sockaddr_in actual{};
            socklen_t len = sizeof(actual);
            if (getsockname(socket_, reinterpret_cast<sockaddr*>(&actual), &len) == 0) {
                actual_port_ = ntohs(actual.sin_port);
            } else {
                actual_port_ = 0;
            }
        } else {
            actual_port_ = bind_.port;
        }

        running_.store(true);
        thread_ = std::thread([this] { run_loop(); });
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (socket_ != INVALID_SOCKET) {
            // shutdown() makes a blocked accept() return immediately on
            // Linux (plain close() from another thread does not).
            ::shutdown(socket_, SHUT_RDWR);
        }
        if (thread_.joinable()) thread_.join();
        cleanup_socket();
    }

    std::uint16_t local_port() const { return actual_port_; }

private:
    void cleanup_socket() {
        if (socket_ != INVALID_SOCKET) {
            ::closesocket(socket_);
            socket_ = INVALID_SOCKET;
        }
        if (wsa_initialized_.exchange(false)) {
            socket_cleanup();
        }
    }

    void run_loop() {
        while (running_.load()) {
            sockaddr_in client_addr{};
            socklen_t client_len = sizeof(client_addr);
            SOCKET client = ::accept(socket_,
                                    reinterpret_cast<sockaddr*>(&client_addr),
                                    &client_len);
            if (client == INVALID_SOCKET) {
                if (!running_.load()) break;
                continue;
            }
            handle_client(client);
            ::closesocket(client);
        }
    }

    static bool read_request(SOCKET client, std::string& out) {
        char buffer[4096];
        std::size_t total = 0;
        while (total < sizeof(buffer) - 1) {
            int n = ::recv(client, buffer + total,
                           static_cast<int>(sizeof(buffer) - 1 - total), 0);
            if (n <= 0) break;
            total += static_cast<std::size_t>(n);
            out.assign(buffer, total);
            if (out.find("\r\n\r\n") != std::string::npos) break;
        }
        buffer[total] = '\0';
        out.assign(buffer, total);
        return total > 0;
    }

    static std::string url_decode(const std::string& s) {
        std::string out;
        out.reserve(s.size());
        for (std::size_t i = 0; i < s.size(); ++i) {
            char c = s[i];
            if (c == '+') {
                out.push_back(' ');
            } else if (c == '%' && i + 2 < s.size()) {
                auto hex = s.substr(i + 1, 2);
                char* end = nullptr;
                long v = std::strtol(hex.c_str(), &end, 16);
                if (end == hex.c_str() + 2) {
                    out.push_back(static_cast<char>(v));
                    i += 2;
                } else {
                    out.push_back(c);
                }
            } else {
                out.push_back(c);
            }
        }
        return out;
    }

    static bool query_has(const std::string& query, const std::string& key_value) {
        std::size_t pos = 0;
        while (pos <= query.size()) {
            std::size_t amp = query.find('&', pos);
            std::size_t end = (amp == std::string::npos) ? query.size() : amp;
            std::string part = query.substr(pos, end - pos);
            if (part == key_value) return true;
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
        return false;
    }

    void handle_client(SOCKET client) {
        std::string raw;
        if (!read_request(client, raw)) return;

        std::string line;
        auto eol = raw.find("\r\n");
        if (eol != std::string::npos) {
            line = raw.substr(0, eol);
        } else {
            line = raw;
        }

        std::string method;
        std::string target;
        {
            auto sp1 = line.find(' ');
            if (sp1 == std::string::npos) return;
            method = line.substr(0, sp1);
            auto sp2 = line.find(' ', sp1 + 1);
            if (sp2 == std::string::npos) {
                target = line.substr(sp1 + 1);
            } else {
                target = line.substr(sp1 + 1, sp2 - sp1 - 1);
            }
        }

        std::string path = target;
        std::string query;
        auto qm = target.find('?');
        if (qm != std::string::npos) {
            path = target.substr(0, qm);
            query = url_decode(target.substr(qm + 1));
        }

        std::string body;
        std::string content_type = "text/plain; charset=utf-8";
        int status = 200;
        std::string status_text = "OK";

        if (method == "GET" && path == "/metrics") {
            body = store_->openmetrics();
        } else if (method == "GET" && path == "/stats") {
            bool prev = query_has(query, "previous=1");
            bool take = query_has(query, "take=1");
            body = store_->json(prev, take);
            content_type = "application/json";
        } else if (method == "GET" && path == "/healthz") {
            body = "ok\n";
        } else {
            status = 404;
            status_text = "Not Found";
            body = "Not Found\n";
        }

        std::ostringstream resp;
        resp << "HTTP/1.1 " << status << ' ' << status_text << "\r\n";
        resp << "Content-Type: " << content_type << "\r\n";
        resp << "Content-Length: " << body.size() << "\r\n";
        resp << "Cache-Control: no-store\r\n";
        resp << "Connection: close\r\n";
        resp << "\r\n";
        resp << body;

        auto out = resp.str();
        std::size_t sent = 0;
        while (sent < out.size()) {
            int n = ::send(client, out.data() + sent,
                           static_cast<int>(out.size() - sent), 0);
            if (n <= 0) break;
            sent += static_cast<std::size_t>(n);
        }
    }

    std::shared_ptr<MetricsStore> store_;
    Address bind_;
    SOCKET socket_{INVALID_SOCKET};
    std::uint16_t actual_port_{};
    std::atomic<bool> wsa_initialized_{false};
    std::atomic<bool> running_{false};
    std::thread thread_;
};

MetricsHttpServer::MetricsHttpServer(std::shared_ptr<MetricsStore> store, Address bind)
    : impl_(std::make_unique<Impl>(std::move(store), std::move(bind))) {}

MetricsHttpServer::~MetricsHttpServer() = default;

bool MetricsHttpServer::start() { return impl_->start(); }

void MetricsHttpServer::stop() { impl_->stop(); }

std::uint16_t MetricsHttpServer::local_port() const { return impl_->local_port(); }

}
