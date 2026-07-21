#pragma once

// Internal report packet handling: builds periodic loss/late reports and
// processes incoming reports (ACK pruning + loss retransmission). Not part
// of the public API.
//
// Wire payload layout:
//   offset 0  (4) ack cursor (highest contiguous received sequence)
//   offset 4  (2) late-packet ratio * 10000 (slow-packet rate, NOT loss rate)
//   offset 6  (2) lost sequence count N
//   offset 8  (4) reserved (legacy heartbeat-tick echo, deprecated, always 0)
//   offset 12 (4N) lost sequence numbers
//   offset 12+4N (4) optional: receiver-measured inbound bandwidth (bytes/s)
//                    from the latest speed-test probe train

#include "tight/types.hpp"

#include <chrono>
#include <cstdint>
#include <functional>

namespace tight::tight_detail {

struct Peer;

class Report {
public:
    // Retransmits one still-pending packet.
    using ResendCallback = std::function<void(Peer* peer, const PacketHeader& header,
                                              const Bytes& payload)>;

    // Builds the report payload (ack cursor, late ratio, lost sequences,
    // probed bandwidth) and resets the peer's per-interval counters.
    // report_interval 用于推导 NACK 放弃时限（kMaxRetries + 2 个周期）。
    static Bytes build_payload(Peer& peer, std::chrono::milliseconds report_interval);

    // Handles an incoming report payload: updates the peer's late ratio,
    // prunes acknowledged pendings, and retransmits lost ones via the
    // callback. Returns the peer-measured bandwidth (bytes/s), or 0 when the
    // report carries none.
    static std::uint64_t handle(Peer& peer, const Bytes& payload,
                                const ResendCallback& resend);

private:
    static constexpr std::uint32_t kMaxRetries = 10;
};

}
