#include "creek/framework/reactor.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

namespace creek::framework {

Reactor::Reactor(ReactorConfig cfg, TimeSource* time_source)
    : m_config(std::move(cfg))
    , m_time_source(time_source)
    , m_owned_clock(time_source ? nullptr : std::make_unique<RealTimeSource>())
    , m_timer(m_owned_clock ? m_owned_clock.get() : m_time_source)
{
    if (m_owned_clock) {
        m_time_source = m_owned_clock.get();
    }
}

Reactor::~Reactor()
{
    stop();
}

void Reactor::submit_io(Task task)
{
    {
        std::lock_guard<std::mutex> lock(m_io_pool.mutex);
        m_io_pool.queue.push_back(std::move(task));
    }
    m_io_pool.cv.notify_one();
}

void Reactor::submit_cpu(Task task)
{
    {
        std::lock_guard<std::mutex> lock(m_cpu_pool.mutex);
        m_cpu_pool.queue.push_back(std::move(task));
    }
    m_cpu_pool.cv.notify_one();
}

void Reactor::schedule_timer_at(Task task, TimePoint deadline)
{
    m_timer.schedule_at(std::move(task), deadline);
}

void Reactor::schedule_timer_in(Task task, std::chrono::milliseconds delay)
{
    m_timer.schedule_in(std::move(task), delay);
}

TaskId Reactor::schedule_periodic(std::string name, std::function<void()> fn,
                                  std::chrono::milliseconds period,
                                  TaskPriority priority, bool skippable)
{
    TaskId periodic_id = Task::next_id();
    Task task(std::move(name), std::move(fn), priority, skippable);
    m_timer.schedule_periodic_entry(std::move(task), period, periodic_id);
    return periodic_id;
}

bool Reactor::cancel_periodic(TaskId id)
{
    return m_timer.cancel_periodic(id);
}

void Reactor::start()
{
    if (m_running.load(std::memory_order_acquire)) {
        return;
    }
    m_running.store(true, std::memory_order_release);

    m_io_pool.running.store(true, std::memory_order_release);
    m_cpu_pool.running.store(true, std::memory_order_release);

    for (std::size_t i = 0; i < m_config.io_threads; ++i) {
        m_io_pool.threads.emplace_back(&Reactor::io_loop, this);
    }
    for (std::size_t i = 0; i < m_config.cpu_threads; ++i) {
        m_cpu_pool.threads.emplace_back(&Reactor::cpu_loop, this);
    }

    m_timer_thread = std::thread([this] {
        while (m_running.load(std::memory_order_acquire)) {
            auto tasks = m_timer.get_expired_tasks();
            for (auto& t : tasks) {
                if (t.priority() == TaskPriority::Critical ||
                    t.priority() == TaskPriority::High) {
                    submit_cpu(std::move(t));
                } else {
                    submit_io(std::move(t));
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });
}

void Reactor::stop()
{
    m_running.store(false, std::memory_order_release);

    m_io_pool.running.store(false, std::memory_order_release);
    m_cpu_pool.running.store(false, std::memory_order_release);
    m_io_pool.cv.notify_all();
    m_cpu_pool.cv.notify_all();

    if (m_timer_thread.joinable()) {
        m_timer_thread.join();
    }

    for (auto& t : m_io_pool.threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    for (auto& t : m_cpu_pool.threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    m_io_pool.threads.clear();
    m_cpu_pool.threads.clear();
}

std::size_t Reactor::io_queue_depth() const
{
    std::lock_guard<std::mutex> lock(m_io_pool.mutex);
    return m_io_pool.queue.size();
}

std::size_t Reactor::cpu_queue_depth() const
{
    std::lock_guard<std::mutex> lock(m_cpu_pool.mutex);
    return m_cpu_pool.queue.size();
}

void Reactor::io_loop()
{
    constexpr std::uint64_t kSlowThresholdUs = 1'000'000;

    while (m_io_pool.running.load(std::memory_order_acquire)) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(m_io_pool.mutex);
            m_io_pool.cv.wait_for(lock, std::chrono::milliseconds(100),
                [this] {
                    return !m_io_pool.queue.empty() ||
                           !m_io_pool.running.load(std::memory_order_acquire);
                });
            if (!m_io_pool.running.load(std::memory_order_acquire)) {
                break;
            }
            if (m_io_pool.queue.empty()) {
                continue;
            }
            task = std::move(m_io_pool.queue.front());
            m_io_pool.queue.erase(m_io_pool.queue.begin());
        }

        auto start = m_time_source->steady_now();
        task.run();
        auto end = m_time_source->steady_now();
        auto elapsed_us = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());

        if (elapsed_us > kSlowThresholdUs) {
            log_slow_task(task.name(), task.id(), elapsed_us, kSlowThresholdUs, false);
        }
    }
}

void Reactor::cpu_loop()
{
    constexpr std::uint64_t kSlowThresholdUs = 10'000;

    while (m_cpu_pool.running.load(std::memory_order_acquire)) {
        Task task;
        {
            std::unique_lock<std::mutex> lock(m_cpu_pool.mutex);
            m_cpu_pool.cv.wait_for(lock, std::chrono::milliseconds(100),
                [this] {
                    return !m_cpu_pool.queue.empty() ||
                           !m_cpu_pool.running.load(std::memory_order_acquire);
                });
            if (!m_cpu_pool.running.load(std::memory_order_acquire)) {
                break;
            }
            if (m_cpu_pool.queue.empty()) {
                continue;
            }
            task = std::move(m_cpu_pool.queue.front());
            m_cpu_pool.queue.erase(m_cpu_pool.queue.begin());
        }

        auto start = m_time_source->steady_now();
        task.run();
        auto end = m_time_source->steady_now();
        auto elapsed_us = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());

        if (elapsed_us > kSlowThresholdUs) {
            log_slow_task(task.name(), task.id(), elapsed_us, kSlowThresholdUs, true);
        }
    }
}

} // namespace creek::framework
