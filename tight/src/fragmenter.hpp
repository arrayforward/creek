#pragma once

// Internal outbound data path: message fragmentation and rotating-parity FEC
// generation. Not part of the public API.

#include "tight/types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>

namespace tight::tight_detail {

struct Peer;

class Fragmenter {
public:
    // Called once per emitted fragment (all data fragments first, then all
    // parity fragments). frag_data/frag_len 为分片区间（调用方持有，
    // 回调内同步消费）；width 为线上补齐宽度（不足补零）。
    using SendFragmentCallback = std::function<void(Peer* peer, std::uint32_t msg_id,
        std::uint16_t idx, std::uint16_t cnt, std::uint16_t data_cnt,
        std::uint16_t real_size, const std::uint8_t* frag_data,
        std::size_t frag_len, std::size_t width, bool ackable)>;

    // Splits payload into mtu-sized fragments, appends FEC parity fragments,
    // and emits every fragment through the callback.
    static void fragment_and_send(Peer& peer, Bytes payload, std::size_t mtu,
                                  const SendFragmentCallback& send_fragment);

    // Number of XOR parity fragments for a message. Information-theoretic
    // sizing: the redundancy rate is the binary entropy of the peer-reported
    // late-packet ratio p, H = -p*log2(p) - (1-p)*log2(1-p), scaled by a
    // safety coefficient (1.2, within the 1.1~1.3 engineering range), then
    // clamped to [1, 3]. Each parity fragment recovers exactly one fragment.
    static std::uint16_t compute_parity_count_for(double late_ratio,
                                                  std::size_t data_count);

private:
    static constexpr double kFecSafetyCoefficient = 1.2;
};

}
