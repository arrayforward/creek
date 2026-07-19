#include "creek/framework/metrics.hpp"

#include <chrono>
#include <cstdint>

namespace creek::framework {

void MetricsCollector::snapshot(FrameworkMetrics& dst) const
{
    dst.messages_processed.store(
        m_metrics.messages_processed.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.messages_dropped.store(
        m_metrics.messages_dropped.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.heartbeats_run.store(
        m_metrics.heartbeats_run.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.m_timertasks_skipped.store(
        m_metrics.m_timertasks_skipped.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.m_timertasks_fired.store(
        m_metrics.m_timertasks_fired.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.io_queue_depth.store(
        m_metrics.io_queue_depth.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.cpu_queue_depth.store(
        m_metrics.cpu_queue_depth.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.m_heartbeatduration_us.store(
        m_metrics.m_heartbeatduration_us.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
    dst.max_heartbeat_duration_us.store(
        m_metrics.max_heartbeat_duration_us.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
}

void MetricsCollector::record_message_processed()
{
    m_metrics.messages_processed.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::record_message_dropped()
{
    m_metrics.messages_dropped.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::record_heartbeat(std::chrono::microseconds duration)
{
    m_metrics.heartbeats_run.fetch_add(1, std::memory_order_relaxed);
    auto us = static_cast<std::uint64_t>(duration.count());
    m_metrics.m_heartbeatduration_us.store(us, std::memory_order_relaxed);

    auto prev = m_metrics.max_heartbeat_duration_us.load(std::memory_order_relaxed);
    while (us > prev &&
           !m_metrics.max_heartbeat_duration_us.compare_exchange_weak(
               prev, us, std::memory_order_relaxed)) {
    }
}

void MetricsCollector::record_timer_task_skipped()
{
    m_metrics.m_timertasks_skipped.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::record_timer_task_fired()
{
    m_metrics.m_timertasks_fired.fetch_add(1, std::memory_order_relaxed);
}

void MetricsCollector::set_io_queue_depth(std::int64_t depth)
{
    m_metrics.io_queue_depth.store(depth, std::memory_order_relaxed);
}

void MetricsCollector::set_cpu_queue_depth(std::int64_t depth)
{
    m_metrics.cpu_queue_depth.store(depth, std::memory_order_relaxed);
}

MetricsCollector::PressureSnapshot MetricsCollector::pressure() const
{
    PressureSnapshot ps;
    ps.io_queue_depth = m_metrics.io_queue_depth.load(std::memory_order_relaxed);
    ps.cpu_queue_depth = m_metrics.cpu_queue_depth.load(std::memory_order_relaxed);
    ps.m_heartbeatduration_us = m_metrics.m_heartbeatduration_us.load(std::memory_order_relaxed);
    ps.messages_processed = m_metrics.messages_processed.load(std::memory_order_relaxed);
    return ps;
}

bool MetricsCollector::PressureSnapshot::overloaded() const
{
    return m_heartbeatduration_us > 40'000;
}

} // namespace creek::framework
