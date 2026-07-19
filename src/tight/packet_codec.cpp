#include "creek/tight/packet_codec.hpp"

#include "crc32.hpp"
#include "wire_format.hpp"

#include <cstring>

namespace creek {

using namespace tight_detail;

Bytes PacketCodec::encode(const PacketHeader& header, const Bytes& payload) {
    Bytes buf(kHeaderSize + payload.size());
    std::uint8_t* p = buf.data();

    auto put32 = [&](std::size_t off, std::uint32_t v) {
        std::uint32_t be = to_be32(v);
        std::memcpy(p + off, &be, 4);
    };
    auto put16 = [&](std::size_t off, std::uint16_t v) {
        std::uint16_t be = to_be16(v);
        std::memcpy(p + off, &be, 2);
    };
    auto put64 = [&](std::size_t off, std::uint64_t v) {
        std::uint64_t be = to_be64(v);
        std::memcpy(p + off, &be, 8);
    };

    put32(0, header.magic);
    p[4] = header.version;
    p[5] = static_cast<std::uint8_t>(header.type);
    put16(6, header.flags);
    put32(8, header.client_id);
    put64(12, header.session_id);
    put32(20, header.sequence);
    put32(24, header.acknowledgment);
    put32(28, header.message_id);
    put16(32, header.fragment_index);
    put16(34, header.fragment_count);
    put16(36, static_cast<std::uint16_t>(payload.size()));
    put16(38, header.reserved);
    put32(40, header.tick);
    put32(44, 0);

    if (!payload.empty()) {
        std::memcpy(p + kHeaderSize, payload.data(), payload.size());
    }

    std::uint32_t crc = crc32_compute(buf.data(), buf.size());
    put32(44, crc);
    return buf;
}

bool PacketCodec::decode(const Bytes& datagram, PacketHeader& header, Bytes& payload) {
    if (datagram.size() < kHeaderSize) return false;
    const std::uint8_t* p = datagram.data();

    auto get32 = [&](std::size_t off) {
        std::uint32_t v;
        std::memcpy(&v, p + off, 4);
        return to_be32(v);
    };
    auto get16 = [&](std::size_t off) {
        std::uint16_t v;
        std::memcpy(&v, p + off, 2);
        return to_be16(v);
    };
    auto get64 = [&](std::size_t off) {
        std::uint32_t lo = get32(off);
        std::uint32_t hi = get32(off + 4);
        return (static_cast<std::uint64_t>(hi) << 32) | lo;
    };

    std::uint32_t magic = get32(0);
    if (magic != kMagic) return false;
    std::uint8_t version = p[4];
    if (version != kVersion) return false;

    header.magic = magic;
    header.version = version;
    header.type = static_cast<PacketType>(p[5]);
    header.flags = get16(6);
    header.client_id = get32(8);
    header.session_id = get64(12);
    header.sequence = get32(20);
    header.acknowledgment = get32(24);
    header.message_id = get32(28);
    header.fragment_index = get16(32);
    header.fragment_count = get16(34);
    std::uint16_t payload_size = get16(36);
    header.payload_size = payload_size;
    header.reserved = get16(38);
    header.tick = get32(40);
    header.checksum = get32(44);

    if (datagram.size() < kHeaderSize + payload_size) return false;

    Bytes tmp(kHeaderSize + payload_size);
    std::memcpy(tmp.data(), p, kHeaderSize);
    if (payload_size > 0) {
        std::memcpy(tmp.data() + kHeaderSize, p + kHeaderSize, payload_size);
    }
    std::uint32_t zero = 0;
    std::memcpy(tmp.data() + 44, &zero, 4);
    std::uint32_t calc = crc32_compute(tmp.data(), tmp.size());
    if (calc != header.checksum) return false;

    payload.assign(p + kHeaderSize, p + kHeaderSize + payload_size);
    return true;
}

std::uint32_t PacketCodec::crc32(const std::uint8_t* data, std::size_t size) {
    return crc32_compute(data, size);
}

}
