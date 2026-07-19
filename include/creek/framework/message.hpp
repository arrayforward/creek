#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace creek::framework {

using MessageId = std::uint64_t;

enum class MessageKind : std::uint8_t {
    UdpDatagram = 0,
    PeerEvent = 1,
    GrpcRequest = 2,
    JsonRpcRequest = 3,
    AdminCommand = 4,
    TimerTick = 5,
    ExternalCallResult = 6,
    SystemEvent = 7,
    Custom = 255
};

struct Message {
    MessageId id{0};
    MessageKind kind{MessageKind::Custom};
    std::chrono::steady_clock::time_point created_at{};
    std::string source;
    std::vector<std::uint8_t> payload;
    std::optional<std::string> service_name;
    std::optional<std::string> rpc_name;

    Message() = default;
    Message(MessageKind k, std::string src)
        : id(next_id()), kind(k), created_at(std::chrono::steady_clock::now()),
          source(std::move(src)) {}

    static MessageId next_id();
    static void reset_id_counter(MessageId base = 0);

private:
    static std::atomic<std::uint64_t> m_idcounter_;
};

} // namespace creek::framework
