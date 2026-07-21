#include "creek/otlp.hpp"
#include "creek/types.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <sstream>
#include <string>

#include "socket_compat.hpp"

namespace creek {

namespace {

constexpr char kKeySeparator = '\x1f';

std::string escape_json_string_otlp(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
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

void split_key_otlp(const std::string& key, std::string& direction,
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

std::string build_otlp_json(const MetricMap& data, const std::string& service_name) {
    auto now = std::chrono::system_clock::now();
    auto time_nanos = std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());

    static const char* metric_names[] = {
        "creek_rpc_calls_total",
        "creek_rpc_errors_total",
        "creek_rpc_bytes_total",
        "creek_rpc_latency_microseconds_total"
    };

    std::ostringstream os;
    os << "{\"resourceMetrics\":[{\"resource\":{\"attributes\":"
       << "[{\"key\":\"service.name\",\"value\":{\"stringValue\":\""
       << escape_json_string_otlp(service_name)
       << "\"}}]},\"scopeMetrics\":[{\"scope\":{\"name\":\"creek\"},\"metrics\":[";

    bool first_metric = true;
    for (int m = 0; m < 4; ++m) {
        if (!first_metric) os << ',';
        first_metric = false;

        os << "{\"name\":\"" << metric_names[m] << "\",\"gauge\":{\"dataPoints\":[";

        bool first_point = true;
        for (const auto& kv : data) {
            std::string direction, rpc_name, metadata;
            split_key_otlp(kv.first, direction, rpc_name, metadata);

            if (!first_point) os << ',';
            first_point = false;

            std::uint64_t value = 0;
            switch (m) {
                case 0: value = kv.second.calls; break;
                case 1: value = kv.second.errors; break;
                case 2: value = kv.second.bytes; break;
                case 3: value = kv.second.latency_us; break;
            }

            os << "{\"attributes\":["
               << "{\"key\":\"direction\",\"value\":{\"stringValue\":\""
               << escape_json_string_otlp(direction) << "\"}},"
               << "{\"key\":\"rpc_name\",\"value\":{\"stringValue\":\""
               << escape_json_string_otlp(rpc_name) << "\"}},"
               << "{\"key\":\"metadata\",\"value\":{\"stringValue\":\""
               << escape_json_string_otlp(metadata) << "\"}}"
               << "],\"timeUnixNano\":\"" << time_nanos
               << "\",\"asInt\":\"" << value << "\"}";
        }

        os << "]}}";
    }

    os << "]}]}]}";
    return os.str();
}

bool http_post(const std::string& host, std::uint16_t port,
               const std::string& path, const std::string& body) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    std::string port_str = std::to_string(port);
    addrinfo* result = nullptr;
    int rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
    if (rc != 0 || !result) return false;

    SOCKET sock = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (sock == INVALID_SOCKET) {
        freeaddrinfo(result);
        return false;
    }

    if (::connect(sock, result->ai_addr, static_cast<int>(result->ai_addrlen)) == SOCKET_ERROR) {
        freeaddrinfo(result);
        ::closesocket(sock);
        return false;
    }
    freeaddrinfo(result);

    std::ostringstream req;
    req << "POST " << path << " HTTP/1.1\r\n"
        << "Host: " << host;
    if (port != 80) req << ':' << port;
    req << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << body.size() << "\r\n"
        << "Connection: close\r\n"
        << "\r\n"
        << body;

    std::string req_str = req.str();
    std::size_t sent = 0;
    while (sent < req_str.size()) {
        int n = ::send(sock, req_str.data() + sent,
                       static_cast<int>(req_str.size() - sent), 0);
        if (n <= 0) {
            ::closesocket(sock);
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }

    char buf[4096];
    int n = ::recv(sock, buf, sizeof(buf) - 1, 0);
    ::closesocket(sock);

    if (n <= 0) return false;
    buf[n] = '\0';
    std::string resp(buf, n);

    return resp.find("HTTP/1.1 2") == 0 || resp.find("HTTP/1.0 2") == 0;
}

}

class OtlpExporter::Impl {
public:
    Impl(std::string collector_addr, std::string service_name, bool use_tls)
        : collector_addr_(std::move(collector_addr))
        , service_name_(std::move(service_name))
        , use_tls_(use_tls) {}

    ~Impl() { stop(); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    bool export_metrics(MetricsStore& store) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) return false;

        if (!wsa_initialized_) {
            if (!socket_startup()) return false;
            wsa_initialized_ = true;
        }

        auto data = store.take();
        if (data.empty()) return true;

        auto body = build_otlp_json(data, service_name_);

        auto addr = parse_address(collector_addr_);
        if (!addr) return false;

        return http_post(addr->host, addr->port, "/v1/metrics", body);
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_) return;
        stopped_ = true;
        if (wsa_initialized_) {
            socket_cleanup();
            wsa_initialized_ = false;
        }
    }

private:
    std::string collector_addr_;
    std::string service_name_;
    bool use_tls_;
    bool stopped_{false};
    bool wsa_initialized_{false};
    std::mutex mutex_;
};

OtlpExporter::OtlpExporter(std::string collector_addr, std::string service_name, bool use_tls)
    : impl_(std::make_unique<Impl>(std::move(collector_addr), std::move(service_name), use_tls)) {}

OtlpExporter::~OtlpExporter() = default;

bool OtlpExporter::export_metrics(MetricsStore& store) {
    return impl_->export_metrics(store);
}

void OtlpExporter::stop() {
    impl_->stop();
}

}
