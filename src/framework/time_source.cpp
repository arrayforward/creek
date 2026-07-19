#include "creek/framework/time_source.hpp"

#include <chrono>

namespace creek::framework {

std::chrono::steady_clock::time_point RealTimeSource::steady_now() const {
    return std::chrono::steady_clock::now();
}

std::chrono::system_clock::time_point RealTimeSource::system_now() const {
    return std::chrono::system_clock::now();
}

std::uint64_t RealTimeSource::steady_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

std::uint64_t RealTimeSource::system_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

void RealTimeSource::advance(std::chrono::milliseconds /*duration*/) {
}

std::chrono::steady_clock::time_point VirtualTimeSource::steady_now() const {
    return m_steady_now;
}

std::chrono::system_clock::time_point VirtualTimeSource::system_now() const {
    return std::chrono::system_clock::time_point(
        std::chrono::duration_cast<std::chrono::system_clock::duration>(
            m_steady_now.time_since_epoch()));
}

std::uint64_t VirtualTimeSource::steady_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        m_steady_now.time_since_epoch()).count();
}

std::uint64_t VirtualTimeSource::system_millis() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        m_steady_now.time_since_epoch()).count();
}

void VirtualTimeSource::advance(std::chrono::milliseconds duration) {
    m_steady_now += duration;
}

void VirtualTimeSource::reset(std::chrono::steady_clock::time_point base) {
    m_steady_base = base;
    m_steady_now = base;
}

} // namespace creek::framework
