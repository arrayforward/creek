#pragma once

#include "creek/types.hpp"

#include <cstddef>
#include <cstdint>

namespace creek {

enum class PacketType : std::uint8_t {
    Handshake = 1,
    HandshakeAck = 2,
    Online = 3,
    Heartbeat = 4,
    Bye = 5,
    Data = 6,
    Ack = 7,
    Parity = 8,
    Report = 9
};

struct PacketHeader {
    std::uint32_t magic{0x54474854U};
    std::uint8_t version{1};
    PacketType type{PacketType::Data};
    std::uint16_t flags{};
    std::uint32_t client_id{};
    std::uint64_t session_id{};
    std::uint32_t sequence{};
    std::uint32_t acknowledgment{};
    std::uint32_t message_id{};
    std::uint16_t fragment_index{};
    std::uint16_t fragment_count{};
    std::uint16_t payload_size{};
    std::uint16_t reserved{};
    std::uint32_t tick{};
    std::uint32_t checksum{};
};

class PacketCodec {
public:
    static constexpr std::size_t header_size = 48;
    static Bytes encode(const PacketHeader& header, const Bytes& payload);
    static bool decode(const Bytes& datagram, PacketHeader& header, Bytes& payload);
    static std::uint32_t crc32(const std::uint8_t* data, std::size_t size);
};

}
