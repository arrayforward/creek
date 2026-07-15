#include "creek/types.hpp"

#include <chrono>
#include <random>
#include <sstream>
#include <stdexcept>

namespace creek {

std::uint64_t unix_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

namespace {
std::mt19937_64& thread_rng() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    return rng;
}
}

std::string random_id() {
    static const char hex[] = "0123456789abcdef";
    std::uint64_t a = thread_rng()();
    std::uint64_t b = thread_rng()();
    std::string out(32, '0');
    for (int i = 0; i < 16; ++i) {
        out[i] = hex[(b >> ((15 - i) * 4)) & 0xF];
        out[16 + i] = hex[(a >> ((15 - i) * 4)) & 0xF];
    }
    return out;
}

std::optional<Address> parse_address(std::string_view value) {
    auto colon = value.rfind(':');
    if (colon == std::string_view::npos) return std::nullopt;
    std::string host{value.substr(0, colon)};
    std::string port_str{value.substr(colon + 1)};
    if (host.empty() || port_str.empty()) return std::nullopt;
    if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    try {
        std::size_t pos = 0;
        unsigned long port = std::stoul(port_str, &pos);
        if (pos != port_str.size() || port == 0 || port > 65535UL) return std::nullopt;
        return Address{std::move(host), static_cast<std::uint16_t>(port)};
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<RemotePeer> parse_remote(std::string_view value) {
    auto at = value.find('@');
    if (at == std::string_view::npos) return std::nullopt;
    std::string id{value.substr(0, at)};
    auto addr = parse_address(value.substr(at + 1));
    if (!addr) return std::nullopt;
    return RemotePeer{std::move(id), *addr};
}

std::string metadata_string(const Metadata& metadata) {
    std::ostringstream os;
    bool first = true;
    for (const auto& kv : metadata) {
        if (!first) os << ';';
        os << kv.first << '=' << kv.second;
        first = false;
    }
    return os.str();
}

}