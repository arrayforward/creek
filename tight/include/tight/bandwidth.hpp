#pragma once

// Public BBR-style bandwidth/RTT estimator. The implementation lives in
// tight/bandwidth.cpp.
//
// Model (simplified BBR):
//  - BtlBw: windowed max of delivery-rate samples (bytes acked / ack RTT).
//  - RTprop: minimum RTT ever observed.
//  - Pacing gain from two signals:
//      PRIMARY   RTT trend: smoothed RTT well above RTprop (queue building)
//                drains (0.75); RTT hugging RTprop and not rising probes
//                (1.25); otherwise cruises (1.0).
//      SECONDARY late-packet ratio reported by the peer: it can only make
//                the estimate more conservative (veto probe / force drain),
//                never more aggressive.

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>

namespace tight {

class BandwidthEstimator {
public:
    explicit BandwidthEstimator(std::uint64_t initial_bytes_per_second);

    // Feeds an ACK sample: bytes acknowledged within rtt. bytes == 0 means a
    // pure RTT sample (e.g. derived from heartbeat/report one-way transit).
    void on_ack(std::size_t bytes, std::chrono::microseconds rtt);

    // Feeds the peer-reported late-packet ratio [0,1] (secondary gain signal).
    void on_late_ratio(double late_ratio);

    // Adopts an externally measured bandwidth (the peer's speed-test train
    // measurement) as the new BtlBw baseline and floor.
    void seed_bandwidth(std::uint64_t bytes_per_second);

    std::uint64_t bytes_per_second() const;
    std::chrono::microseconds rtt() const;

private:
    static constexpr std::size_t kWindowSize = 10;   // BtlBw max-filter window
    static constexpr double kGainUp = 1.25;          // probe
    static constexpr double kGainDown = 0.75;        // drain
    // RTT-trend thresholds: smoothed RTT relative to RTprop.
    static constexpr double kQueueHigh = 1.5;        // definite queueing
    static constexpr double kQueueLow = 1.2;         // queueing when rising
    static constexpr double kQueueTight = 1.1;       // RTT at the floor
    // Late-ratio thresholds (secondary).
    static constexpr double kLateHigh = 0.10;        // congested above this
    static constexpr double kLateLow = 0.02;         // clean below this

    void recompute_gain();

    mutable std::mutex m_mu;
    std::array<std::uint64_t, kWindowSize> m_window{};
    std::size_t m_window_pos{0};
    std::size_t m_window_count{0};
    std::uint64_t m_btl_bw;
    std::uint64_t m_floor;
    double m_gain{1.0};
    double m_last_late_ratio{0.0};
    bool m_have_late{false};
    bool m_late_rising{false};
    std::chrono::microseconds m_rtt{0};
    std::chrono::microseconds m_rt_prop{0};
    std::chrono::microseconds m_prev_rtt{0};
    bool m_have_prev_rtt{false};
};

} // namespace tight
