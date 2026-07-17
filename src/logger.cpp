#include "creek/logger.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string>
#include <unistd.h>

namespace creek {

namespace {
std::mutex g_log_mutex;
std::FILE* g_log_file = nullptr;
std::string g_log_path;
std::atomic<bool> g_initialized{false};

void write_log(const std::string& level, const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;
    std::tm tm_buf{};
    #ifdef _WIN32
    localtime_s(&tm_buf, &t);
    #else
    localtime_r(&t, &tm_buf);
    #endif
    char time_str[32];
    std::strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &tm_buf);
    std::string line = std::string(time_str) + "." +
                       std::to_string((ms.count() / 100) % 10) +
                       std::to_string((ms.count() / 10) % 10) +
                       std::to_string(ms.count() % 10) +
                       " [" + level + "] " + msg + "\n";
    if (g_log_file) {
        std::fputs(line.c_str(), g_log_file);
        std::fflush(g_log_file);
    }
}
} // namespace

void Logger::init(const std::string& log_dir, std::size_t /*max_size*/,
                  std::size_t /*max_files*/, const std::string& /*pattern*/) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_initialized.load()) return;
    int pid = static_cast<int>(::getpid());
    g_log_path = log_dir + "/creek." + std::to_string(pid) + ".log";
    g_log_file = std::fopen(g_log_path.c_str(), "a");
    if (g_log_file) {
        std::setvbuf(g_log_file, nullptr, _IOLBF, 4096);
    }
    g_initialized.store(true);
}

void Logger::shutdown() {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_file) {
        std::fclose(g_log_file);
        g_log_file = nullptr;
    }
    g_initialized.store(false);
}

bool Logger::initialized() { return g_initialized.load(); }

void Logger::trace(const std::string& msg) { write_log("trace", msg); }
void Logger::debug(const std::string& msg) { write_log("debug", msg); }
void Logger::info(const std::string& msg)  { write_log("info",  msg); }
void Logger::warn(const std::string& msg)  { write_log("warn",  msg); }
void Logger::error(const std::string& msg) { write_log("error", msg); }
void Logger::critical(const std::string& msg) { write_log("critical", msg); }

} // namespace creek
