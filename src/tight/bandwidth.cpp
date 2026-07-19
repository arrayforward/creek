#include "creek/tight/bandwidth.hpp"

#include <algorithm>

namespace creek {

BandwidthEstimator::BandwidthEstimator(std::uint64_t initial_bytes_per_second)
    : m_bandwidth(initial_bytes_per_second == 0 ? 1 : initial_bytes_per_second),
      m_floor(initial_bytes_per_second == 0 ? 1 : initial_bytes_per_second) {
}

void BandwidthEstimator::on_ack(std::size_t bytes, std::chrono::microseconds rtt) {
    auto rtt_count = rtt.count();
    if (rtt_count <= 0) return;
    if (bytes == 0) {
        if (m_rtt.count() == 0) m_rtt = rtt;
        else m_rtt = std::chrono::microseconds((m_rtt.count() * 7 + rtt_count) / 8);
        return;
    }
    std::uint64_t sample = (static_cast<std::uint64_t>(bytes) * 1000000ULL)
                         / static_cast<std::uint64_t>(rtt_count);
    if (sample == 0) sample = 1;
    if (m_bandwidth == 0) m_bandwidth = sample;
    else m_bandwidth = std::max<std::uint64_t>(m_floor, (m_bandwidth * 7ULL + sample) / 8ULL);

    if (m_rtt.count() == 0) m_rtt = rtt;
    else m_rtt = std::chrono::microseconds((m_rtt.count() * 7 + rtt_count) / 8);
}

std::uint64_t BandwidthEstimator::bytes_per_second() const {
    return m_bandwidth;
}

std::chrono::microseconds BandwidthEstimator::rtt() const {
    return m_rtt;
}

}
