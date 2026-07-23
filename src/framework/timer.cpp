#include "creek/framework/timer.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace creek::framework {

PriorityTimer::PriorityTimer(TimeSource* time_source,
                             std::chrono::milliseconds skip_threshold)
    : m_time_source(time_source)
    , m_skip_threshold(skip_threshold)
{
}

void PriorityTimer::schedule_at(Task task, TimePoint deadline)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_heap.push(TimerEntry{std::move(task), deadline, std::chrono::milliseconds(0), 0});
}

void PriorityTimer::schedule_in(Task task, std::chrono::milliseconds delay)
{
    auto deadline = m_time_source->steady_now() + delay;
    schedule_at(std::move(task), deadline);
}

void PriorityTimer::schedule_periodic_entry(Task task, std::chrono::milliseconds period, TaskId periodic_id)
{
    auto deadline = m_time_source->steady_now() + period;
    std::lock_guard<std::mutex> lock(m_mutex);
    m_heap.push(TimerEntry{std::move(task), deadline, period, periodic_id});
}

std::vector<Task> PriorityTimer::get_expired_tasks()
{
    std::vector<Task> tasks;
    // Slow-skip records are logged AFTER the lock is released: write_log is
    // synchronous (global mutex + fflush per line), and every
    // schedule_timer_in (e.g. heartbeat_tick re-arming itself) blocks on this
    // same m_mutex — logging inside the critical section stalls all timer
    // re-arms for the duration of the log write.
    struct SlowSkip {
        std::string name;
        TaskId id;
        std::uint64_t elapsed_us;
    };
    std::vector<SlowSkip> slow_skips;
    {
        std::lock_guard<std::mutex> lock(m_mutex);

        auto now = m_time_source->steady_now();

        while (!m_heap.empty()) {
            const auto& entry = m_heap.top();
            if (entry.deadline > now) {
                break;
            }

            if (entry.task.skippable()) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - entry.deadline);
                if (elapsed > m_skip_threshold) {
                    slow_skips.push_back(SlowSkip{
                        entry.task.name(),
                        entry.task.id(),
                        static_cast<std::uint64_t>(
                            std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count()),
                    });
                    m_heap.pop();
                    continue;
                }
            }

            if (entry.period.count() > 0 && entry.periodic_id != 0) {
                Task next(entry.task.name(), entry.task.func(), entry.task.priority(), entry.task.skippable());
                TimerEntry next_entry{std::move(next), now + entry.period, entry.period, entry.periodic_id};
                tasks.push_back(std::move(entry.task));
                m_heap.pop();
                m_heap.push(std::move(next_entry));
                continue;
            }

            tasks.push_back(std::move(entry.task));
            m_heap.pop();
        }
    }
    for (const auto& skip : slow_skips) {
        log_slow_task(
            skip.name,
            skip.id,
            skip.elapsed_us,
            static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    m_skip_threshold).count()),
            false);
    }

    return tasks;
}

bool PriorityTimer::cancel(TaskId task_id)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    std::vector<TimerEntry> entries;
    entries.reserve(m_heap.size());

    while (!m_heap.empty()) {
        entries.push_back(std::move(const_cast<TimerEntry&>(m_heap.top())));
        m_heap.pop();
    }

    auto it = std::find_if(entries.begin(), entries.end(),
        [task_id](const TimerEntry& e) { return e.task.id() == task_id; });

    bool found = (it != entries.end());
    if (found) {
        entries.erase(it);
    }

    for (auto& e : entries) {
        m_heap.push(std::move(e));
    }

    return found;
}

bool PriorityTimer::cancel_periodic(TaskId periodic_id)
{
    std::lock_guard<std::mutex> lock(m_mutex);

    std::vector<TimerEntry> entries;
    entries.reserve(m_heap.size());

    while (!m_heap.empty()) {
        entries.push_back(std::move(const_cast<TimerEntry&>(m_heap.top())));
        m_heap.pop();
    }

    auto it = std::find_if(entries.begin(), entries.end(),
        [periodic_id](const TimerEntry& e) { return e.periodic_id == periodic_id; });

    bool found = (it != entries.end());
    if (found) {
        entries.erase(it);
    }

    for (auto& e : entries) {
        m_heap.push(std::move(e));
    }

    return found;
}

std::size_t PriorityTimer::pending() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_heap.size();
}

bool PriorityTimer::empty() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_heap.empty();
}

} // namespace creek::framework
