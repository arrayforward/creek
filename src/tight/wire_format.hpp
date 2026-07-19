#pragma once

// Internal wire-format constants and big-endian conversion helpers shared by
// the tight transport translation units. Not part of the public API.

#include <cstddef>
#include <cstdint>

namespace creek::tight_detail {

inline constexpr std::uint32_t kMagic = 0x54474854U;
inline constexpr std::uint8_t kVersion = 1;
inline constexpr std::size_t kHeaderSize = 48;

inline std::uint16_t to_be16(std::uint16_t v) {
    return static_cast<std::uint16_t>(((v & 0x00FFU) << 8) | ((v & 0xFF00U) >> 8));
}
inline std::uint32_t to_be32(std::uint32_t v) {
    return ((v & 0x000000FFU) << 24) | ((v & 0x0000FF00U) << 8)
         | ((v & 0x00FF0000U) >> 8)  | ((v & 0xFF000000U) >> 24);
}
inline std::uint64_t to_be64(std::uint64_t v) {
    return (static_cast<std::uint64_t>(to_be32(static_cast<std::uint32_t>((v >> 32) & 0xFFFFFFFFULL))) << 32)
         | to_be32(static_cast<std::uint32_t>(v & 0xFFFFFFFFULL));
}

}
