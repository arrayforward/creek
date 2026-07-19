#pragma once

// Internal wire-format helpers shared by the node and leaf runtime
// implementation translation units. Not part of the public API.

#include "creek/types.hpp"
#include "creek.pb.h"

#include <cstdint>
#include <string>

namespace creek {

inline std::string format_address(const Address& addr) {
    return addr.host + ":" + std::to_string(addr.port);
}

inline Bytes serialize_wire(const creek::v1::WireMessage& msg) {
    Bytes out(static_cast<std::size_t>(msg.ByteSizeLong()));
    if (!out.empty()) {
        msg.SerializeToArray(out.data(), static_cast<int>(out.size()));
    }
    return out;
}

inline bool parse_wire(const Bytes& data, creek::v1::WireMessage& msg) {
    if (data.empty()) return false;
    return msg.ParseFromArray(data.data(), static_cast<int>(data.size()));
}

inline Metadata request_metadata(const creek::v1::RoutedRequest& request) {
    Metadata metadata;
    for (const auto& entry : request.metadata()) {
        metadata[entry.first] = entry.second;
    }
    return metadata;
}

inline constexpr std::uint32_t kDefaultHopLimit = 16;
inline constexpr std::uint32_t kDefaultBackendHopLimit = 16;

}
