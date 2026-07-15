#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace creek {

using Bytes = std::vector<std::uint8_t>;
using Metadata = std::map<std::string, std::string>;
using SteadyClock = std::chrono::steady_clock;
using SystemClock = std::chrono::system_clock;

struct Address {
    std::string host{"127.0.0.1"};
    std::uint16_t port{};
};

struct RemotePeer {
    std::string id;
    Address address;
};

std::uint64_t unix_millis();
std::string random_id();
std::optional<Address> parse_address(std::string_view value);
std::optional<RemotePeer> parse_remote(std::string_view value);
std::string metadata_string(const Metadata& metadata);

}
