#include "creek/framework/task.hpp"

#include <utility>

namespace creek::framework {

std::atomic<std::uint64_t> Task::m_idcounter_{1};

Task::Task() noexcept = default;

Task::Task(std::string name, Func func, TaskPriority priority, bool skippable)
    : m_id(next_id())
    , m_name(std::move(name))
    , m_func(std::move(func))
    , m_priority(priority)
    , m_skippable(skippable)
{
}

Task::Task(Task&& other) noexcept
    : m_id(other.m_id)
    , m_name(std::move(other.m_name))
    , m_func(std::move(other.m_func))
    , m_priority(other.m_priority)
    , m_skippable(other.m_skippable)
{
    other.m_id = 0;
}

Task& Task::operator=(Task&& other) noexcept
{
    if (this != &other) {
        m_id = other.m_id;
        m_name = std::move(other.m_name);
        m_func = std::move(other.m_func);
        m_priority = other.m_priority;
        m_skippable = other.m_skippable;
        other.m_id = 0;
    }
    return *this;
}

void Task::run()
{
    if (m_func) {
        m_func();
    }
}

TaskId Task::next_id() noexcept
{
    return m_idcounter_.fetch_add(1, std::memory_order_relaxed);
}

} // namespace creek::framework
