#pragma once

#include <chrono>
#include <cstdint>

namespace creek::framework {

class TimeSource {
public:
    virtual ~TimeSource() = default;

    virtual std::chrono::steady_clock::time_point steady_now() const = 0;
    virtual std::chrono::system_clock::time_point system_now() const = 0;
    virtual std::uint64_t steady_millis() const = 0;
    virtual std::uint64_t system_millis() const = 0;

    virtual void advance(std::chrono::milliseconds duration) = 0;
};

class RealTimeSource final : public TimeSource {
public:
    std::chrono::steady_clock::time_point steady_now() const override;
    std::chrono::system_clock::time_point system_now() const override;
    std::uint64_t steady_millis() const override;
    std::uint64_t system_millis() const override;

    void advance(std::chrono::milliseconds duration) override;
};

class VirtualTimeSource final : public TimeSource {
public:
    std::chrono::steady_clock::time_point steady_now() const override;
    std::chrono::system_clock::time_point system_now() const override;
    std::uint64_t steady_millis() const override;
    std::uint64_t system_millis() const override;

    void advance(std::chrono::milliseconds duration) override;
    void reset(std::chrono::steady_clock::time_point base = std::chrono::steady_clock::time_point{});

private:
    std::chrono::steady_clock::time_point m_steady_base{};
    std::chrono::steady_clock::time_point m_steady_now{};
};

} // namespace creek::framework
