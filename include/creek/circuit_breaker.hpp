#pragma once

#include "creek/types.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace creek {

struct CircuitConfig {
    std::chrono::milliseconds cooldown{30000};
    std::uint64_t latency_threshold_us{500000};
    double error_rate_threshold{0.5};
    std::uint64_t consecutive_failure_threshold{5};
    std::size_t min_samples{10};
    std::uint64_t half_open_max{1};
};

enum class CircuitState {
    Closed,
    Open,
    HalfOpen
};

struct EndpointStats {
    std::uint64_t total_calls{};
    std::uint64_t errors{};
    std::uint64_t latency_sum_us{};
    std::uint64_t consecutive_failures{};
    SteadyClock::time_point opened_at{};
    SteadyClock::time_point last_call{};
    CircuitState state{CircuitState::Closed};
    std::uint64_t half_open_count{};
};

class CircuitBreaker {
public:
    explicit CircuitBreaker(CircuitConfig config = CircuitConfig{});
    bool allow(std::string_view endpoint_id);
    void record_success(std::string_view endpoint_id, std::uint64_t latency_us);
    void record_failure(std::string_view endpoint_id);
    CircuitState state(std::string_view endpoint_id) const;
    const EndpointStats& stats(std::string_view endpoint_id) const;
    void reset(std::string_view endpoint_id);
    void reset_all();
    std::size_t size() const;
    std::unordered_map<std::string, EndpointStats> snapshot() const;

private:
    CircuitConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, EndpointStats> endpoints_;
};

}
