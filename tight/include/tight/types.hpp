#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace tight {

using Bytes = std::vector<std::uint8_t>;

enum class PacketType : std::uint8_t {
    Handshake    = 0,
    HandshakeAck = 1,
    Online       = 2,
    Heartbeat    = 3,
    Bye          = 4,
    Data         = 5,
    Parity       = 6,
    Ack          = 7,
    Report       = 8,
    Probe        = 9,
    Command      = 10,
};

struct PacketHeader {
    std::uint32_t magic{};
    std::uint8_t  version{};
    PacketType    type{};
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

enum class LinkRole : std::uint8_t {
    Leaf = 0,
    Node = 1,
};

enum class LinkState : std::uint8_t {
    Closed      = 0,
    Handshake   = 1,
    Established = 2,
    Online      = 3,
};

struct PeerEvent {
    std::string  id;
    LinkRole     role{LinkRole::Leaf};
    LinkState    state{LinkState::Closed};
    std::uint32_t client_id{};
};

struct NetAddress {
    std::string  host;
    std::uint16_t port{};

    NetAddress() = default;
    NetAddress(std::string h, std::uint16_t p)
        : host(std::move(h)), port(p) {}
};

struct RemotePeer {
    std::string id;
    NetAddress address;
};

struct TightConfig {
    NetAddress    bind;
    std::string   id;
    std::string   token;
    LinkRole      role{LinkRole::Leaf};
    // 1350：单包载荷 1350-48(头)-16(GCM)=1286B，恰好整包容纳
    // 16kHz 单声道 PCM 40ms 帧（1280B），音视频场景免分片
    std::size_t    mtu{1350};
    std::chrono::milliseconds heartbeat{std::chrono::seconds(5)};
    std::chrono::milliseconds report_interval{std::chrono::seconds(1)};
    // 排空节拍。lite 模式自动钳制到 ≥10ms（IoT 设备降 CPU 唤醒/功耗，
    // 附加延迟 ≤10ms）；普通模式按配置值（可低至 1-2ms 保音频延迟）
    std::chrono::milliseconds flush_interval{std::chrono::milliseconds(10)};
    std::chrono::milliseconds dead_timeout{std::chrono::seconds(30)};
    std::chrono::milliseconds retransmit_timeout{std::chrono::milliseconds(500)};
    std::uint64_t  initial_bandwidth_bytes{100 * 1024 * 1024};
    std::size_t    queue_limit{65536};
    // 单条应用消息的最大长度（默认 64KB）。发送超限返回 false；
    // 接收侧按此上限防御异常 fragment_count（防内存耗尽）。
    // 有效范围自动钳制到 [8KB, 10MB]。
    std::size_t    max_message_bytes{64 * 1024};
    // 丢弃异常消息（超限/畸形分片）时输出告警日志。仅服务器端开启；
    // lite_mode 端点自动关闭（静默丢弃），不受此开关影响。
    bool           drop_log{true};
    // 数据面 NACK 重传开关。关闭后：本端不生成 NACK、缺口立即跳过；
    // 并经握手能力标志通告对端——对端收到后不再为本链路保留重传缓冲
    // （m_pending≈0，在途内存显著下降），纯 FEC 兜底。任一端可单方面
    // 关闭，不影响业务：控制包（握手/命令）可靠性独立，数据面实时流
    // 由 FEC + 应用层容错覆盖。适用：lite IoT 实时音视频/遥测；
    // 文件/关键数据勿关。
    bool           retransmit_enabled{true};
    // A data packet is "late" when its transit time (one-way, computed with
    // the per-peer clock offset) exceeds late_rtt_multiplier * RTT.
    double           late_rtt_multiplier{4.0};
    // After a link comes Online each end sends a speed-test train of blank
    // Probe datagrams; the receiver estimates the inbound bandwidth from the
    // packet arrival times and reports it back. Disable to skip probing.
    bool             speed_test_enabled{true};
    std::size_t      speed_test_bytes{100 * 1024};
    // ECDH + AEAD：握手阶段交换 X25519 公钥并 HKDF 派生会话密钥，
    // 数据分组（Data/Parity/Command）使用 AES-256-GCM 加密
    bool             encryption_enabled{true};
    // 资源占用：socket 内核缓冲（SO_RCVBUF/SO_SNDBUF 各自大小）与内部队列容量
    std::size_t    socket_buffer_bytes{8 * 1024 * 1024};
    std::size_t    encode_queue_limit{4096};
    std::size_t    outbound_queue_limit{65536};
    // 客户端精简模式：单线程（receiver/encode/sender 职责全部由 reactor
    // 节拍合并）、线程使用 64KB 小栈、内核缓冲与队列上限自动收紧
    // （socket_buffer≤16KB、encode≤64、outbound≤256、queue_limit≤128），
    // 面向嵌入式/单连接客户端；可经 set_lite_mode() 运行时动态切换
    bool             lite_mode{false};
};

inline std::uint64_t unix_millis() {
    using namespace std::chrono;
    auto now = system_clock::now();
    return static_cast<std::uint64_t>(
        duration_cast<milliseconds>(now.time_since_epoch()).count());
}

} // namespace tight
