#pragma once

#include <cstdio>
#include <mutex>
#include <string>

namespace creek {

class Logger {
public:
    static void init(const std::string& log_dir = "logs",
                     std::size_t max_size = 100ULL * 1024 * 1024,
                     std::size_t max_files = 30,
                     const std::string& pattern =
                         "[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");

    static void shutdown();

    static void trace(const std::string& msg);
    static void debug(const std::string& msg);
    static void info(const std::string& msg);
    static void warn(const std::string& msg);
    static void error(const std::string& msg);
    static void critical(const std::string& msg);

    static bool initialized();
};

#define CREEK_LOG_INFO(msg)     ::creek::Logger::info(msg)
#define CREEK_LOG_WARN(msg)     ::creek::Logger::warn(msg)
#define CREEK_LOG_ERROR(msg)    ::creek::Logger::error(msg)
#define CREEK_LOG_DEBUG(msg)    ::creek::Logger::debug(msg)
#define CREEK_LOG_TRACE(msg)    ::creek::Logger::trace(msg)
#define CREEK_LOG_CRITICAL(msg) ::creek::Logger::critical(msg)

}
