#include "creek/framework/logger_adapter.hpp"
#include "creek/logger.hpp"

#include <sstream>

namespace creek::framework {

void log_slow_task(const std::string& task_name, std::uint64_t task_id,
                   std::uint64_t elapsed_us, std::uint64_t threshold_us,
                   bool is_cpu_bound)
{
    (void)is_cpu_bound;
    std::ostringstream oss;
    oss << "[fw] slow task " << task_name
        << " id=" << task_id
        << " elapsed=" << elapsed_us << " us"
        << " threshold=" << threshold_us << " us";
    log_warn(oss.str());
}

void log_info(const std::string& msg)
{
    if (creek::Logger::initialized()) {
        creek::Logger::info(msg);
    }
}

void log_warn(const std::string& msg)
{
    if (creek::Logger::initialized()) {
        creek::Logger::warn(msg);
    }
}

void log_error(const std::string& msg)
{
    if (creek::Logger::initialized()) {
        creek::Logger::error(msg);
    }
}

void log_debug(const std::string& msg)
{
    if (creek::Logger::initialized()) {
        creek::Logger::debug(msg);
    }
}

} // namespace creek::framework
