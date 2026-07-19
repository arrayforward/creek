#pragma once

#include "creek/framework/task.hpp"
#include "creek/framework/time_source.hpp"
#include "creek/framework/logger_adapter.hpp"

#include <chrono>
#include <cstdint>
#include <mutex>
#include <queue>
#include <vector>

namespace creek::framework {

constexpr std::chrono::milliseconds kDefaultSkipThreshold{1000};

struct TimerEntry {
    mutable Task task;
    TimePoint deadline;
    std::chrono::milliseconds period{0};
    TaskId periodic_id{0};
    bool operator>(const TimerEntry& other) const { return deadline > other.deadline; }
};

class PriorityTimer {
public:
    explicit PriorityTimer(TimeSource* time_source,
                           std::chrono::milliseconds skip_threshold = kDefaultSkipThreshold);

    PriorityTimer(const PriorityTimer&) = delete;
    PriorityTimer& operator=(const PriorityTimer&) = delete;

    void schedule_at(Task task, TimePoint deadline);
    void schedule_in(Task task, std::chrono::milliseconds delay);
    void schedule_periodic_entry(Task task, std::chrono::milliseconds period, TaskId periodic_id);

    std::vector<Task> get_expired_tasks();

    bool cancel(TaskId task_id);
    bool cancel_periodic(TaskId periodic_id);

    std::size_t pending() const;
    bool empty() const;

private:
    TimeSource* m_time_source;
    std::chrono::milliseconds m_skip_threshold;
    mutable std::mutex m_mutex;
    std::priority_queue<TimerEntry, std::vector<TimerEntry>, std::greater<TimerEntry>> m_heap;
};

} // namespace creek::framework
