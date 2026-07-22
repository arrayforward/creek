#pragma once

// Internal per-peer state shared by the tight transport translation units
// (transport, reassembler, fragmenter, report). Not part of the public API.

#include "socket_platform.hpp"

#include "tight/types.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace tight::tight_detail {

struct PendingSend {
    PacketHeader m_header{};
    Bytes m_payload;
    std::chrono::steady_clock::time_point m_last_send;
    std::size_t m_bytes{0};
    std::uint32_t m_retries{0};
};

struct IncomingMessage {
    std::uint32_t m_message_id{};
    std::uint16_t m_total_count{};
    std::uint16_t m_data_count{};
    std::vector<std::optional<Bytes>> m_fragments;
    std::vector<std::uint16_t> m_sizes;
    std::chrono::steady_clock::time_point m_first_seen;
};

struct Peer {
    std::mutex m_mu;
    std::string m_id;
    sockaddr_in m_addr{};
    bool m_addr_set{false};
    LinkRole m_role{LinkRole::Leaf};
    LinkState m_state{LinkState::Closed};
    std::uint32_t m_peer_client_id{};
    std::uint64_t m_peer_session_id{};
    std::chrono::steady_clock::time_point m_last_recv;
    std::chrono::steady_clock::time_point m_last_heartbeat_sent;
    std::chrono::steady_clock::time_point m_last_handshake_sent;
    // 握手重发退避（500ms 起步、封顶 5s），进入 Handshake 状态时重置。
    std::chrono::milliseconds m_handshake_backoff{500};
    std::chrono::steady_clock::time_point m_last_report_sent;
    std::uint32_t m_sequence_out{1};
    // 消息组分 id（fragment 组的 message_id）独立计数器。
    // 必须与数据序列号 m_sequence_out 分离：若共用，每条消息会消耗两个
    // 序号，使对端缺口跟踪出现永不到达的"幽灵序号"，全部误报为丢包，
    // 导致 ack 游标冻结、m_pending 无限堆积（内存泄漏）。
    std::uint32_t m_msg_id_out{1};
    std::map<std::uint32_t, PendingSend> m_pending;
    std::map<std::uint32_t, IncomingMessage> m_incoming;
    std::map<std::uint32_t, std::chrono::steady_clock::time_point> m_completed;
    std::map<std::uint32_t, std::chrono::steady_clock::time_point> m_missing_seqs;
    std::set<std::uint32_t> m_recv_seqs;
    std::uint32_t m_next_expected_seq{};
    bool m_seq_initialized{false};
    std::uint32_t m_sender_rtt_us{};
    bool m_reconnect{};

    // Clock sync (对表): offset = remote_clock - local_clock (µs), estimated
    // at handshake as (remote_tick - local_arrival) - rtt/2 and re-synced on
    // every heartbeat to track drift. Both ends store the peer's offset;
    // local time is never modified.
    std::int64_t m_clock_offset_us{0};
    bool m_clock_synced{false};
    bool m_clock_pending{false};
    std::uint32_t m_hs_tick{};
    std::uint64_t m_hs_arrival_ms{};
    // Receiver-side late-packet accounting (reset every report interval):
    // a data packet is late when its transit time exceeds the configured
    // multiple of the RTT.
    std::uint64_t m_transit_samples{};
    std::uint64_t m_late_samples{};
    // Sender side: latest late-packet ratio reported by the peer; drives the
    // FEC redundancy rate and acts as the secondary bandwidth-gain signal.
    double m_peer_late_ratio{0.0};

    // Inbound speed-test train accounting (receiver side): wire bytes and
    // arrival span of the current train; finalized into m_probe_bw_bps and
    // attached to the next report so the sender can seed its estimator.
    std::chrono::steady_clock::time_point m_probe_first;
    std::chrono::steady_clock::time_point m_probe_last;
    std::uint64_t m_probe_bytes{};
    std::uint32_t m_probe_count{};
    std::uint64_t m_probe_bw_bps{};

    // Command channel: ordered control/button packets (single datagram, no
    // reassembly). Out-of-order packets are held for at most 3 RTT before
    // the gap is skipped; later arrivals of skipped sequences are dropped.
    std::uint32_t m_cmd_seq_out{1};
    std::uint32_t m_cmd_next_expected{};
    bool m_cmd_initialized{false};
    std::map<std::uint32_t, Bytes> m_cmd_held;
    std::chrono::steady_clock::time_point m_cmd_gap_since;

    // 加密状态：握手阶段 ECDH 协商出的会话密钥（AES-256-GCM），
    // 双方 client_id 排序拼接为 salt 经 HKDF-SHA256 派生。
    bool m_crypto_ready{false};
    std::array<std::uint8_t, 32> m_crypto_key{};

    // 异常消息丢弃日志：由配置 TightConfig::drop_log 决定（默认开），
    // lite_mode 端点自动关闭（静默丢弃）；建 peer 时写入。
    bool m_drop_log{true};
    // 重传协商：m_retransmit 为本端配置（决定是否生成 NACK）；
    // m_peer_retransmit 为对端握手通告（决定是否保留重传缓冲）。
    bool m_retransmit{true};
    bool m_peer_retransmit{true};
    // 限流时间戳（每 peer 每秒最多一条，防日志洪水）
    std::chrono::steady_clock::time_point m_oversize_log_ts{};
};

// A probe train is considered finished once no Probe packet has arrived for
// this gap (trains are sent back-to-back, so inter-packet gaps are tiny).
inline constexpr std::chrono::milliseconds kProbeTrainGap{20};

// Finalizes the in-flight probe train when the gap has elapsed: bandwidth
// = received wire bytes / (last arrival - first arrival). No-op otherwise.
inline void finalize_probe_train(Peer& peer, std::chrono::steady_clock::time_point now) {
    if (peer.m_probe_count == 0) return;
    if (now - peer.m_probe_last <= kProbeTrainGap) return;
    auto span_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       peer.m_probe_last - peer.m_probe_first).count();
    if (peer.m_probe_count >= 2 && span_us > 0) {
        peer.m_probe_bw_bps = static_cast<std::uint64_t>(
            static_cast<double>(peer.m_probe_bytes) * 1000000.0 /
            static_cast<double>(span_us));
    }
    peer.m_probe_count = 0;
    peer.m_probe_bytes = 0;
}

// One-way transit time (µs) of a packet carrying the peer's send tick,
// converted into the local clock domain via the peer's clock offset.
// Returns -1 when the clock offset is not yet available or no tick is set.
inline std::int64_t transit_time_us(const Peer& peer, std::uint32_t tick,
                                    std::uint64_t arrival_ms) {
    if (!peer.m_clock_synced || tick == 0) return -1;
    std::uint32_t arrival_low = static_cast<std::uint32_t>(arrival_ms & 0xFFFFFFFFULL);
    std::int64_t t = static_cast<std::int64_t>(
                         static_cast<std::int32_t>(arrival_low - tick)) * 1000
                     + peer.m_clock_offset_us;
    return t >= 0 ? t : 0;
}

struct AddrKey {
    std::uint32_t m_addr;
    std::uint16_t m_port;
    bool operator<(const AddrKey& o) const {
        if (m_addr != o.m_addr) return m_addr < o.m_addr;
        return m_port < o.m_port;
    }
};

}
