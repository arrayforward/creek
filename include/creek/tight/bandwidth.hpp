#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace creek {

class BandwidthEstimator {
public:
    explicit BandwidthEstimator(std::uint64_t initial_bytes_per_second);
    void on_ack(std::size_t bytes, std::chrono::microseconds rtt);
    std::uint64_t bytes_per_second() const;
    std::chrono::microseconds rtt() const;

private:
    std::uint64_t m_bandwidth;
    // Lower bound for the estimate: never throttle the sender below the
    // configured initial bandwidth. Prevents a bogus low sample (e.g. an
    // RTT inflated by report batching) from starving the token bucket.
    std::uint64_t m_floor;
    std::chrono::microseconds m_rtt{};
};

}
