#pragma once

#include "creek/framework/blackboard.hpp"
#include "creek/routing.hpp"
#include "creek/circuit_breaker.hpp"
#include "creek/metrics.hpp"

#include <memory>
#include <string>

namespace creek::framework {

class DirectoryBlackboardAdapter {
public:
    explicit DirectoryBlackboardAdapter(Blackboard* bb, const std::string& key = "endpoint_directory");

    bool merge(const creek::v1::Endpoint& endpoint);
    bool merge(const creek::v1::DirectorySnapshot& snapshot);
    void upsert_local(creek::v1::Endpoint endpoint);
    std::optional<creek::v1::Endpoint> find(std::string_view endpoint_id) const;
    std::vector<creek::v1::Endpoint> service(std::string_view name) const;
    creek::v1::DirectorySnapshot snapshot(std::string_view source_id) const;
    std::uint64_t version() const;
    std::size_t size() const;

    EndpointDirectory* directory() { return m_directory.get(); }

private:
    void mark_changed();

    Blackboard* m_blackboard;
    std::string m_key;
    std::unique_ptr<EndpointDirectory> m_directory;
};

class BalancerBlackboardAdapter {
public:
    explicit BalancerBlackboardAdapter(Blackboard* bb, const std::string& key = "sticky_balancer");

    std::optional<creek::v1::Endpoint> pick(
        std::string_view service, const Metadata& metadata,
        const std::vector<creek::v1::Endpoint>& endpoints,
        SteadyClock::time_point now = SteadyClock::now());
    void invalidate(std::string_view endpoint_id);
    void set_ttl(std::chrono::milliseconds ttl);
    std::size_t active_size() const;
    std::size_t lru_size() const;

    StickyBalancer* balancer() { return m_balancer.get(); }

private:
    void mark_changed();

    Blackboard* m_blackboard;
    std::string m_key;
    std::unique_ptr<StickyBalancer> m_balancer;
};

class BreakerBlackboardAdapter {
public:
    explicit BreakerBlackboardAdapter(Blackboard* bb, const std::string& key = "circuit_breaker");

    bool allow(std::string_view endpoint_id);
    void record_success(std::string_view endpoint_id, std::uint64_t latency_us);
    void record_failure(std::string_view endpoint_id);
    CircuitState state(std::string_view endpoint_id) const;
    const EndpointStats& stats(std::string_view endpoint_id) const;
    void reset(std::string_view endpoint_id);
    void reset_all();
    std::size_t size() const;

    CircuitBreaker* breaker() { return m_breaker.get(); }

private:
    void mark_changed();

    Blackboard* m_blackboard;
    std::string m_key;
    std::unique_ptr<CircuitBreaker> m_breaker;
};

class MetricsBlackboardAdapter {
public:
    explicit MetricsBlackboardAdapter(Blackboard* bb, const std::string& key = "metrics_store");

    void record(const MetricEvent& event);
    MetricMap previous();
    MetricMap current();
    MetricMap take();
    std::string openmetrics();
    std::string json(bool previous_minute, bool take_values);

    MetricsStore* store() { return m_store.get(); }

private:
    void mark_changed();

    Blackboard* m_blackboard;
    std::string m_key;
    std::shared_ptr<MetricsStore> m_store;
};

} // namespace creek::framework
