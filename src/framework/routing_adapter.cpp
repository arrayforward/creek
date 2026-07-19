#include "creek/framework/routing_adapter.hpp"

namespace creek::framework {

DirectoryBlackboardAdapter::DirectoryBlackboardAdapter(Blackboard* bb, const std::string& key)
    : m_blackboard(bb), m_key(key), m_directory(std::make_unique<EndpointDirectory>()) {
    m_blackboard->register_value(m_key, m_directory.get(), DataPolicy::SharedLocked);
}

bool DirectoryBlackboardAdapter::merge(const creek::v1::Endpoint& endpoint) {
    bool changed = m_directory->merge(endpoint);
    if (changed) mark_changed();
    return changed;
}

bool DirectoryBlackboardAdapter::merge(const creek::v1::DirectorySnapshot& snapshot) {
    bool changed = m_directory->merge(snapshot);
    if (changed) mark_changed();
    return changed;
}

void DirectoryBlackboardAdapter::upsert_local(creek::v1::Endpoint endpoint) {
    m_directory->upsert_local(std::move(endpoint));
    mark_changed();
}

std::optional<creek::v1::Endpoint> DirectoryBlackboardAdapter::find(std::string_view endpoint_id) const {
    return m_directory->find(endpoint_id);
}

std::vector<creek::v1::Endpoint> DirectoryBlackboardAdapter::service(std::string_view name) const {
    return m_directory->service(name);
}

creek::v1::DirectorySnapshot DirectoryBlackboardAdapter::snapshot(std::string_view source_id) const {
    return m_directory->snapshot(source_id);
}

std::uint64_t DirectoryBlackboardAdapter::version() const {
    return m_directory->version();
}

std::size_t DirectoryBlackboardAdapter::size() const {
    return m_directory->size();
}

void DirectoryBlackboardAdapter::mark_changed() {
    m_blackboard->mark_changed(m_key);
}

BalancerBlackboardAdapter::BalancerBlackboardAdapter(Blackboard* bb, const std::string& key)
    : m_blackboard(bb), m_key(key), m_balancer(std::make_unique<StickyBalancer>()) {
    m_blackboard->register_value(m_key, m_balancer.get(), DataPolicy::SharedLocked);
}

std::optional<creek::v1::Endpoint> BalancerBlackboardAdapter::pick(
    std::string_view service, const Metadata& metadata,
    const std::vector<creek::v1::Endpoint>& endpoints,
    SteadyClock::time_point now) {
    auto result = m_balancer->pick(service, metadata, endpoints, now);
    mark_changed();
    return result;
}

void BalancerBlackboardAdapter::invalidate(std::string_view endpoint_id) {
    m_balancer->invalidate(endpoint_id);
    mark_changed();
}

void BalancerBlackboardAdapter::set_ttl(std::chrono::milliseconds ttl) {
    m_balancer->set_ttl(ttl);
    mark_changed();
}

std::size_t BalancerBlackboardAdapter::active_size() const {
    return m_balancer->active_size();
}

std::size_t BalancerBlackboardAdapter::lru_size() const {
    return m_balancer->lru_size();
}

void BalancerBlackboardAdapter::mark_changed() {
    m_blackboard->mark_changed(m_key);
}

BreakerBlackboardAdapter::BreakerBlackboardAdapter(Blackboard* bb, const std::string& key)
    : m_blackboard(bb), m_key(key), m_breaker(std::make_unique<CircuitBreaker>()) {
    m_blackboard->register_value(m_key, m_breaker.get(), DataPolicy::SharedLocked);
}

bool BreakerBlackboardAdapter::allow(std::string_view endpoint_id) {
    bool result = m_breaker->allow(endpoint_id);
    mark_changed();
    return result;
}

void BreakerBlackboardAdapter::record_success(std::string_view endpoint_id, std::uint64_t latency_us) {
    m_breaker->record_success(endpoint_id, latency_us);
    mark_changed();
}

void BreakerBlackboardAdapter::record_failure(std::string_view endpoint_id) {
    m_breaker->record_failure(endpoint_id);
    mark_changed();
}

CircuitState BreakerBlackboardAdapter::state(std::string_view endpoint_id) const {
    return m_breaker->state(endpoint_id);
}

const EndpointStats& BreakerBlackboardAdapter::stats(std::string_view endpoint_id) const {
    return m_breaker->stats(endpoint_id);
}

void BreakerBlackboardAdapter::reset(std::string_view endpoint_id) {
    m_breaker->reset(endpoint_id);
    mark_changed();
}

void BreakerBlackboardAdapter::reset_all() {
    m_breaker->reset_all();
    mark_changed();
}

std::size_t BreakerBlackboardAdapter::size() const {
    return m_breaker->size();
}

void BreakerBlackboardAdapter::mark_changed() {
    m_blackboard->mark_changed(m_key);
}

MetricsBlackboardAdapter::MetricsBlackboardAdapter(Blackboard* bb, const std::string& key)
    : m_blackboard(bb), m_key(key), m_store(std::make_shared<MetricsStore>()) {
    m_blackboard->register_value(m_key, m_store.get(), DataPolicy::SharedLocked);
}

void MetricsBlackboardAdapter::record(const MetricEvent& event) {
    m_store->record(event);
    mark_changed();
}

MetricMap MetricsBlackboardAdapter::previous() {
    return m_store->previous();
}

MetricMap MetricsBlackboardAdapter::current() {
    return m_store->current();
}

MetricMap MetricsBlackboardAdapter::take() {
    auto result = m_store->take();
    mark_changed();
    return result;
}

std::string MetricsBlackboardAdapter::openmetrics() {
    return m_store->openmetrics();
}

std::string MetricsBlackboardAdapter::json(bool previous_minute, bool take_values) {
    return m_store->json(previous_minute, take_values);
}

void MetricsBlackboardAdapter::mark_changed() {
    m_blackboard->mark_changed(m_key);
}

} // namespace creek::framework
