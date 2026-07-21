#include "tight/bandwidth.hpp"

#include <algorithm>

namespace tight {

BandwidthEstimator::BandwidthEstimator(std::uint64_t initial_bytes_per_second)
    : m_btl_bw(initial_bytes_per_second == 0 ? 1 : initial_bytes_per_second),
      m_floor(initial_bytes_per_second == 0 ? 1 : initial_bytes_per_second) {
}

void BandwidthEstimator::on_ack(std::size_t bytes, std::chrono::microseconds rtt) {
    std::lock_guard<std::mutex> lock(m_mu);
    auto rtt_count = rtt.count();
    if (rtt_count <= 0) return;

    // RTprop (min filter) + smoothed RTT; keep the previous smoothed value
    // for trend detection.
    if (m_rt_prop.count() == 0 || rtt < m_rt_prop) m_rt_prop = rtt;
    if (m_rtt.count() == 0) {
        m_rtt = rtt;
    } else {
        m_prev_rtt = m_rtt;
        m_have_prev_rtt = true;
        m_rtt = std::chrono::microseconds((m_rtt.count() * 7 + rtt_count) / 8);
    }

    recompute_gain();  // RTT trend is the primary gain signal

    if (bytes == 0) return;  // pure RTT sample (heartbeat/report transit)

    // Delivery-rate sample (bytes/s); BtlBw = windowed max (BBR max filter).
    std::uint64_t sample = (static_cast<std::uint64_t>(bytes) * 1000000ULL)
                         / static_cast<std::uint64_t>(rtt_count);
    if (sample == 0) sample = 1;
    m_window[m_window_pos] = sample;
    m_window_pos = (m_window_pos + 1) % m_window.size();
    if (m_window_count < m_window.size()) ++m_window_count;
    std::uint64_t best = 0;
    for (std::size_t i = 0; i < m_window_count; ++i) best = std::max(best, m_window[i]);
    if (best > 0) m_btl_bw = best;
}

void BandwidthEstimator::on_late_ratio(double late_ratio) {
    std::lock_guard<std::mutex> lock(m_mu);
    if (late_ratio < 0.0) late_ratio = 0.0;
    if (late_ratio > 1.0) late_ratio = 1.0;
    m_late_rising = m_have_late && late_ratio > m_last_late_ratio + 0.001;
    m_last_late_ratio = late_ratio;
    m_have_late = true;
    recompute_gain();  // late ratio is the secondary gain signal
}

// PRIMARY: RTT trend. Smoothed RTT far above RTprop means a queue is
// building (drain); RTT near RTprop and not rising means headroom (probe).
// SECONDARY: the peer-reported late ratio can only pull the gain down
// (veto probe / force drain), never push it up.
void BandwidthEstimator::recompute_gain() {
    double primary = 1.0;
    if (m_rt_prop.count() > 0 && m_rtt.count() > 0) {
        double queue_ratio = static_cast<double>(m_rtt.count()) /
                             static_cast<double>(m_rt_prop.count());
        bool rising = m_have_prev_rtt &&
                      m_rtt.count() * 100LL > m_prev_rtt.count() * 105LL;
        if (queue_ratio >= kQueueHigh || (rising && queue_ratio >= kQueueLow)) {
            primary = kGainDown;
        } else if (queue_ratio <= kQueueTight && !rising) {
            primary = kGainUp;
        }
    }

    double gain = primary;
    if (m_have_late) {
        if (m_last_late_ratio >= kLateHigh ||
            (m_late_rising && m_last_late_ratio >= kLateLow)) {
            gain = std::min(gain, kGainDown);      // late burst: force drain
        } else if (m_last_late_ratio > kLateLow) {
            gain = std::min(gain, 1.0);            // mild lateness vetoes probe
        }
    }
    m_gain = gain;
}

void BandwidthEstimator::seed_bandwidth(std::uint64_t bytes_per_second) {
    if (bytes_per_second == 0) return;
    std::lock_guard<std::mutex> lock(m_mu);
    m_btl_bw = bytes_per_second;
    m_floor = bytes_per_second;
}

std::uint64_t BandwidthEstimator::bytes_per_second() const {
    std::lock_guard<std::mutex> lock(m_mu);
    double paced = static_cast<double>(m_btl_bw) * m_gain;
    return std::max<std::uint64_t>(m_floor, static_cast<std::uint64_t>(paced));
}

std::chrono::microseconds BandwidthEstimator::rtt() const {
    std::lock_guard<std::mutex> lock(m_mu);
    return m_rtt;
}

}
