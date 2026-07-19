#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <vector>

namespace creek::framework {

struct FrameworkMetrics {
    std::atomic<std::uint64_t> messages_processed{0};
    std::atomic<std::uint64_t> messages_dropped{0};
    std::atomic<std::uint64_t> heartbeats_run{0};
    std::atomic<std::uint64_t> m_timertasks_skipped{0};
    std::atomic<std::uint64_t> m_timertasks_fired{0};
    std::atomic<std::int64_t> io_queue_depth{0};
    std::atomic<std::int64_t> cpu_queue_depth{0};
    std::atomic<std::uint64_t> m_heartbeatduration_us{0};
    std::atomic<std::uint64_t> max_heartbeat_duration_us{0};
};

class MetricsCollector {
public:
    MetricsCollector() = default;

    void snapshot(FrameworkMetrics& dst) const;
    const FrameworkMetrics& live() const { return m_metrics; }

    FrameworkMetrics& direct() { return m_metrics; }

    void record_message_processed();
    void record_message_dropped();
    void record_heartbeat(std::chrono::microseconds duration);
    void record_timer_task_skipped();
    void record_timer_task_fired();
    void set_io_queue_depth(std::int64_t depth);
    void set_cpu_queue_depth(std::int64_t depth);

    struct PressureSnapshot {
        std::int64_t io_queue_depth{0};
        std::int64_t cpu_queue_depth{0};
        std::uint64_t m_heartbeatduration_us{0};
        std::uint64_t messages_processed{0};
        bool overloaded() const;
    };
    PressureSnapshot pressure() const;

private:
    FrameworkMetrics m_metrics;
};

} // namespace creek::framework
