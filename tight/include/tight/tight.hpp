#pragma once

#include "tight/types.hpp"
#include "tight/packet_codec.hpp"
#include "tight/fec.hpp"
#include "tight/bandwidth.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace tight {

class TightTransport {
public:
    using MessageCallback = std::function<void(const std::string& peer_id, Bytes payload)>;
    using PeerCallback    = std::function<void(const PeerEvent& event)>;
    using CommandCallback = std::function<void(const std::string& peer_id, Bytes payload)>;

    explicit TightTransport(TightConfig config);
    ~TightTransport();

    TightTransport(const TightTransport&) = delete;
    TightTransport& operator=(const TightTransport&) = delete;

    void set_message_callback(MessageCallback callback);
    void set_peer_callback(PeerCallback callback);
    void set_command_callback(CommandCallback callback);

    bool start();
    void stop();

    bool connect(const RemotePeer& remote);
    bool send(const std::string& peer_id, Bytes payload);
    bool send_priority(const std::string& peer_id, Bytes payload, int priority);

    // Sends a command packet (control / button-key information). Commands
    // fit in a single datagram, so no fragmentation is needed, and they jump
    // the queue ahead of any pending data. The peer receives them via its
    // CommandCallback in order: out-of-order packets are held for at most
    // 3 RTT before the gap is skipped (late arrivals are then dropped).
    // Returns false when the payload exceeds one datagram.
    bool send_command(const std::string& peer_id, Bytes payload);

    // 运行时动态切换精简模式（本端本地属性，不影响对端）：
    //   true  —— 单线程（receiver/encode/sender 职责并入 reactor 节拍），
    //            64KB 小栈、小缓冲小队列，空闲实例约 76KB
    //   false —— 4 线程（reactor/receiver/encode/sender 全分离）
    // 队列容量按构造时配置固定，切换只改变线程模型；start() 前后均可调用。
    void set_lite_mode(bool lite);
    bool lite_mode() const;

    std::vector<PeerEvent> peers() const;
    std::uint16_t local_port() const;

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace tight
