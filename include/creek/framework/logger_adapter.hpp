#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace creek::framework {

using TimePoint = std::chrono::steady_clock::time_point;

void log_slow_task(const std::string& task_name, std::uint64_t task_id,
                   std::uint64_t elapsed_us, std::uint64_t threshold_us,
                   bool is_cpu_bound);

void log_info(const std::string& msg);
void log_warn(const std::string& msg);
void log_error(const std::string& msg);
void log_debug(const std::string& msg);

} // namespace creek::framework
