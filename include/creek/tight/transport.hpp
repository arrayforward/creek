#pragma once

#include "creek/types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace creek {

enum class LinkRole : std::uint8_t {
    Node = 1,
    Leaf = 2
};

enum class LinkState : std::uint8_t {
    Handshake,
    Established,
    Online,
    Closed
};

struct TightConfig {
    std::string id;
    LinkRole role{LinkRole::Leaf};
    Address bind;
    std::string token;
    std::chrono::milliseconds heartbeat{100};
    std::chrono::milliseconds dead_timeout{10000};
    std::chrono::milliseconds retransmit_timeout{100};
    std::chrono::milliseconds flush_interval{10};
    std::chrono::milliseconds report_interval{100};
    std::size_t mtu{1200};
    std::size_t queue_limit{4096};
    std::uint64_t initial_bandwidth_bytes{10U * 1024U * 1024U};
};

struct PeerEvent {
    std::string id;
    LinkRole role{LinkRole::Leaf};
    LinkState state{LinkState::Closed};
    std::uint32_t client_id{};
};

class TightTransport {
public:
    using MessageCallback = std::function<void(const std::string&, Bytes)>;
    using PeerCallback = std::function<void(const PeerEvent&)>;

    explicit TightTransport(TightConfig config);
    ~TightTransport();
    TightTransport(const TightTransport&) = delete;
    TightTransport& operator=(const TightTransport&) = delete;

    void set_message_callback(MessageCallback callback);
    void set_peer_callback(PeerCallback callback);
    bool start();
    void stop();
    bool connect(const RemotePeer& remote);
    bool send(const std::string& peer_id, Bytes payload);
    bool send_priority(const std::string& peer_id, Bytes payload, int priority);
    std::vector<PeerEvent> peers() const;
    std::uint16_t local_port() const;

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

}
