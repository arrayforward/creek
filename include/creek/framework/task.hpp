#pragma once

#include "creek/framework/time_source.hpp"

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>

namespace creek::framework {

enum class TaskPriority : std::uint8_t {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3
};

using TaskId = std::uint64_t;

class Task {
public:
    using Func = std::function<void()>;

    Task() noexcept;
    explicit Task(std::string name, Func func, TaskPriority priority = TaskPriority::Normal, bool skippable = false);

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept;
    Task& operator=(Task&& other) noexcept;

    TaskId id() const noexcept { return m_id; }
    const std::string& name() const noexcept { return m_name; }
    TaskPriority priority() const noexcept { return m_priority; }
    bool skippable() const noexcept { return m_skippable; }

    void run();
    const Func& func() const noexcept { return m_func; }

    static TaskId next_id() noexcept;

private:
    TaskId m_id{};
    std::string m_name;
    Func m_func;
    TaskPriority m_priority{TaskPriority::Normal};
    bool m_skippable{false};
    static std::atomic<std::uint64_t> m_idcounter_;
};

} // namespace creek::framework
