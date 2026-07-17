#pragma once

#include "creek/types.hpp"

#include <atomic>
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

enum class PacketType : std::uint8_t {
    Handshake = 1,
    HandshakeAck = 2,
    Online = 3,
    Heartbeat = 4,
    Bye = 5,
    Data = 6,
    Ack = 7,
    Parity = 8,
    Report = 9
};

struct PacketHeader {
    std::uint32_t magic{0x54474854U};
    std::uint8_t version{1};
    PacketType type{PacketType::Data};
    std::uint16_t flags{};
    std::uint32_t client_id{};
    std::uint64_t session_id{};
    std::uint32_t sequence{};
    std::uint32_t acknowledgment{};
    std::uint32_t message_id{};
    std::uint16_t fragment_index{};
    std::uint16_t fragment_count{};
    std::uint16_t payload_size{};
    std::uint16_t reserved{};
    std::uint32_t tick{};
    std::uint32_t checksum{};
};

struct TightConfig {
    std::string id;
    LinkRole role{LinkRole::Leaf};
    Address bind;
    std::string token;
    std::chrono::milliseconds heartbeat{100};
    std::chrono::milliseconds dead_timeout{3000};
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

class PacketCodec {
public:
    static constexpr std::size_t header_size = 48;
    static Bytes encode(const PacketHeader& header, const Bytes& payload);
    static bool decode(const Bytes& datagram, PacketHeader& header, Bytes& payload);
    static std::uint32_t crc32(const std::uint8_t* data, std::size_t size);
};

class ReedSolomon {
public:
    static Bytes parity(const std::vector<Bytes>& fragments, std::size_t width);
    static bool recover_one(std::vector<Bytes>& fragments, const Bytes& parity,
                            std::size_t missing_index, std::size_t width);
};

class BandwidthEstimator {
public:
    explicit BandwidthEstimator(std::uint64_t initial_bytes_per_second);
    void on_ack(std::size_t bytes, std::chrono::microseconds rtt);
    std::uint64_t bytes_per_second() const;
    std::chrono::microseconds rtt() const;

private:
    std::uint64_t bandwidth_;
    std::chrono::microseconds rtt_{};
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
    std::unique_ptr<Impl> impl_;
};

}
