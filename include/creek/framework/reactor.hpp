#pragma once

#include "creek/framework/task.hpp"
#include "creek/framework/time_source.hpp"
#include "creek/framework/timer.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace creek::framework {

struct ReactorConfig {
    std::size_t io_threads = 4;
    std::size_t cpu_threads = 2;
    std::chrono::milliseconds heartbeat_interval{20};
    std::chrono::milliseconds gc_interval{60000};
};

class Reactor {
public:
    explicit Reactor(ReactorConfig cfg, TimeSource* time_source = nullptr);
    ~Reactor();

    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    void submit_io(Task task);
    void submit_cpu(Task task);
    void schedule_timer_at(Task task, TimePoint deadline);
    void schedule_timer_in(Task task, std::chrono::milliseconds delay);
    TaskId schedule_periodic(std::string name, std::function<void()> fn,
                             std::chrono::milliseconds period,
                             TaskPriority priority = TaskPriority::Normal,
                             bool skippable = false);
    bool cancel_periodic(TaskId id);

    void start();
    void stop();
    bool is_running() const { return m_running.load(); }

    std::size_t io_queue_depth() const;
    std::size_t cpu_queue_depth() const;

    PriorityTimer& timer() { return m_timer; }

private:
    struct WorkerPool {
        std::vector<std::thread> threads;
        std::vector<Task> queue;
        mutable std::mutex mutex;
        std::condition_variable cv;
        std::atomic<bool> running{false};
    };

    void io_loop();
    void cpu_loop();
    void process_timer_expiry();

    ReactorConfig m_config;
    TimeSource* m_time_source;
    std::unique_ptr<TimeSource> m_owned_clock;
    PriorityTimer m_timer;

    WorkerPool m_io_pool;
    WorkerPool m_cpu_pool;

    std::atomic<bool> m_running{false};
    std::thread m_timer_thread;
};

} // namespace creek::framework
