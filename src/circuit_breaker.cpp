#include "creek/circuit_breaker.hpp"

#include <algorithm>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace creek {

CircuitBreaker::CircuitBreaker(CircuitConfig config)
    : config_(std::move(config)) {}

bool CircuitBreaker::allow(std::string_view endpoint_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto& ep = endpoints_[std::string(endpoint_id)];
    auto now = SteadyClock::now();
    ep.last_call = now;

    if (ep.state == CircuitState::Closed) return true;

    if (ep.state == CircuitState::Open) {
        if (now - ep.opened_at >= config_.cooldown) {
            ep.state = CircuitState::HalfOpen;
            ep.half_open_count = 0;
        } else {
            return false;
        }
    }

    if (ep.state == CircuitState::HalfOpen) {
        if (ep.half_open_count < config_.half_open_max) {
            ++ep.half_open_count;
            return true;
        }
        return false;
    }
    return true;
}

void CircuitBreaker::record_success(std::string_view endpoint_id, std::uint64_t latency_us) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto& ep = endpoints_[std::string(endpoint_id)];
    ++ep.total_calls;
    ep.latency_sum_us += latency_us;
    ep.consecutive_failures = 0;

    if (ep.state == CircuitState::HalfOpen) {
        ep.state = CircuitState::Closed;
    }
}

void CircuitBreaker::record_failure(std::string_view endpoint_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    auto& ep = endpoints_[std::string(endpoint_id)];
    ++ep.total_calls;
    ++ep.errors;
    ++ep.consecutive_failures;
    auto now = SteadyClock::now();
    ep.last_call = now;

    if (ep.state != CircuitState::Closed) {
        ep.state = CircuitState::Open;
        ep.opened_at = now;
        return;
    }

    if (ep.consecutive_failures >= config_.consecutive_failure_threshold) {
        ep.state = CircuitState::Open;
        ep.opened_at = now;
        return;
    }

    if (ep.total_calls >= config_.min_samples) {
        double error_rate = static_cast<double>(ep.errors) / static_cast<double>(ep.total_calls);
        if (error_rate >= config_.error_rate_threshold) {
            ep.state = CircuitState::Open;
            ep.opened_at = now;
            return;
        }
        if (ep.total_calls > 0) {
            std::uint64_t avg_latency = ep.latency_sum_us / ep.total_calls;
            if (avg_latency >= config_.latency_threshold_us) {
                ep.state = CircuitState::Open;
                ep.opened_at = now;
                return;
            }
        }
    }
}

CircuitState CircuitBreaker::state(std::string_view endpoint_id) const {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = endpoints_.find(std::string(endpoint_id));
    if (it == endpoints_.end()) return CircuitState::Closed;
    return it->second.state;
}

const EndpointStats& CircuitBreaker::stats(std::string_view endpoint_id) const {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = endpoints_.find(std::string(endpoint_id));
    if (it == endpoints_.end()) {
        static EndpointStats empty;
        return empty;
    }
    return it->second;
}

void CircuitBreaker::reset(std::string_view endpoint_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    endpoints_.erase(std::string(endpoint_id));
}

void CircuitBreaker::reset_all() {
    std::lock_guard<std::mutex> lk(mutex_);
    endpoints_.clear();
}

std::size_t CircuitBreaker::size() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return endpoints_.size();
}

std::unordered_map<std::string, EndpointStats> CircuitBreaker::snapshot() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return endpoints_;
}

}
