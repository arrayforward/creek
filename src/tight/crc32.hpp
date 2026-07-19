#pragma once

// Internal CRC32 (IEEE 802.3, table-driven) declaration. The implementation
// lives in crc32.cpp. Not part of the public API; the public entry point is
// PacketCodec::crc32.

#include <cstddef>
#include <cstdint>

namespace creek::tight_detail {

std::uint32_t crc32_compute(const std::uint8_t* data, std::size_t size);

}
