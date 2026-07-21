#include "report.hpp"

#include "peer.hpp"
#include "wire_format.hpp"

#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <vector>

namespace tight::tight_detail {

Bytes Report::build_payload(Peer& peer, std::chrono::milliseconds report_interval) {
    auto now = std::chrono::steady_clock::now();
    std::vector<std::uint32_t> lost_seqs;
    std::uint16_t ratio_val;
    std::uint32_t ack_seq;
    std::uint64_t probe_bw;
    {
        // m_missing_seqs / m_transit_samples / m_late_samples /
        // m_next_expected_seq / m_probe_* are also mutated by
        // Reassembler::handle_data() and handle_probe() on the receiver
        // thread. Guard with peer.m_mu.
        std::lock_guard<std::mutex> seq_lock(peer.m_mu);
        std::uint32_t rtt_threshold = peer.m_sender_rtt_us > 0 ? peer.m_sender_rtt_us : 10000;
        std::uint32_t loss_threshold = rtt_threshold * 7 / 2;
        if (rtt_threshold < 10000) loss_threshold = 100000;
        const auto give_up_us = std::chrono::duration_cast<std::chrono::microseconds>(
            report_interval * (kMaxRetries + 2)).count();

        // 跳过缺口的公共动作：推进游标并消化已收的连续序号
        auto skip_gap = [&peer](std::uint32_t g) {
            if (peer.m_seq_initialized && g == peer.m_next_expected_seq) {
                ++peer.m_next_expected_seq;
                while (peer.m_recv_seqs.count(peer.m_next_expected_seq)) {
                    peer.m_recv_seqs.erase(peer.m_next_expected_seq);
                    ++peer.m_next_expected_seq;
                }
            }
        };

        for (auto mit = peer.m_missing_seqs.begin(); mit != peer.m_missing_seqs.end();) {
            auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - mit->second).count();
            // 重传关闭（本端配置或对端通告）：不生成 NACK，缺口立即跳过，
            // ack 游标照常前进，纯 FEC 兜底。
            if (!peer.m_retransmit || !peer.m_peer_retransmit) {
                skip_gap(mit->first);
                mit = peer.m_missing_seqs.erase(mit);
                continue;
            }
            if (elapsed_us > give_up_us) {
                // 对端长期未重传（Report 全丢或对端已放弃）：停止上报。
                // 缺口已在越限时跳过，ack 游标不受影响。
                mit = peer.m_missing_seqs.erase(mit);
                continue;
            }
            if (elapsed_us > static_cast<long long>(loss_threshold)) {
                // 确认前每个周期重复上报（不擦除）；重传到达时由
                // Reassembler 从 m_missing_seqs 移除，自然停止。
                lost_seqs.push_back(mit->first);
                // 超过 3.5×RTT 即跳过缺口：ack 游标不停滞，发送端可正常
                // 修剪已确认 pending；迟到的重传仍会被正常投递。
                skip_gap(mit->first);
            }
            ++mit;
        }
        // 硬上限：防御极端丢包下 missing 表无限增长（约 3MB/peer/千条级）
        while (peer.m_missing_seqs.size() > 4096) {
            peer.m_missing_seqs.erase(peer.m_missing_seqs.begin());
        }

        // Late-packet ratio over this report interval (slow-packet rate, NOT
        // loss rate); the counters are fed by the reassembler per data packet.
        double late_ratio = 0.0;
        if (peer.m_transit_samples > 0) {
            late_ratio = static_cast<double>(peer.m_late_samples) /
                         static_cast<double>(peer.m_transit_samples);
        }
        ratio_val = static_cast<std::uint16_t>(late_ratio * 10000.0);
        ack_seq = peer.m_seq_initialized ? (peer.m_next_expected_seq > 0 ? peer.m_next_expected_seq - 1 : 0) : 0;
        peer.m_transit_samples = 0;
        peer.m_late_samples = 0;

        // Finalize any in-flight speed-test train and attach the measured
        // inbound bandwidth once so the sender can seed its estimator.
        finalize_probe_train(peer, now);
        probe_bw = peer.m_probe_bw_bps;
        peer.m_probe_bw_bps = 0;
    }

    std::uint16_t lost_count = static_cast<std::uint16_t>(lost_seqs.size());
    if (lost_count > 256) lost_count = 256;
    Bytes payload(16 + lost_count * 4);
    std::uint32_t ack_seq_be = to_be32(ack_seq);
    std::uint16_t ratio_be = to_be16(ratio_val);
    std::uint16_t lost_be = to_be16(lost_count);
    std::uint32_t reserved_be = 0;  // offset 8: legacy hb-tick echo, deprecated
    std::memcpy(payload.data(), &ack_seq_be, 4);
    std::memcpy(payload.data() + 4, &ratio_be, 2);
    std::memcpy(payload.data() + 6, &lost_be, 2);
    std::memcpy(payload.data() + 8, &reserved_be, 4);
    for (std::uint16_t i = 0; i < lost_count; ++i) {
        std::uint32_t seq_be = to_be32(lost_seqs[i]);
        std::memcpy(payload.data() + 12 + i * 4, &seq_be, 4);
    }
    std::uint32_t bw_be = to_be32(static_cast<std::uint32_t>(probe_bw & 0xFFFFFFFFULL));
    std::memcpy(payload.data() + 12 + lost_count * 4, &bw_be, 4);
    return payload;
}

std::uint64_t Report::handle(Peer& peer, const Bytes& payload, const ResendCallback& resend) {
    if (payload.size() < 12) return 0;
    std::uint32_t ack_seq_be = 0;
    std::uint16_t late_ratio_be = 0;
    std::uint16_t lost_count_be = 0;
    std::uint32_t reserved_be = 0;
    std::memcpy(&ack_seq_be, payload.data(), 4);
    std::memcpy(&late_ratio_be, payload.data() + 4, 2);
    std::memcpy(&lost_count_be, payload.data() + 6, 2);
    std::memcpy(&reserved_be, payload.data() + 8, 4);
    std::uint32_t ack_seq = to_be32(ack_seq_be);
    std::uint16_t late_ratio_raw = to_be16(late_ratio_be);
    std::uint16_t lost_count = to_be16(lost_count_be);
    (void)to_be32(reserved_be);  // offset 8: legacy hb-tick echo, ignored
    std::size_t expected = 12U + static_cast<std::size_t>(lost_count) * 4U;
    if (payload.size() < expected) return 0;

    // Optional trailing field: inbound bandwidth measured by the peer from
    // the speed-test probe train (bytes/s).
    std::uint64_t probe_bw = 0;
    if (payload.size() >= expected + 4) {
        std::uint32_t bw_be = 0;
        std::memcpy(&bw_be, payload.data() + expected, 4);
        probe_bw = to_be32(bw_be);
    }

    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        // Latest late-packet ratio reported by the peer; drives the FEC
        // redundancy rate and acts as the secondary bandwidth-gain signal.
        double p = static_cast<double>(late_ratio_raw) / 10000.0;
        if (p < 0.0) p = 0.0;
        if (p > 1.0) p = 1.0;
        peer.m_peer_late_ratio = p;
    }

    std::set<std::uint32_t> lost_seqs;
    for (std::uint16_t i = 0; i < lost_count; ++i) {
        std::uint32_t lost_be = 0;
        std::memcpy(&lost_be, payload.data() + 12 + i * 4, 4);
        lost_seqs.insert(to_be32(lost_be));
    }

    // Snapshot pending under lock, then do the work without holding the lock.
    std::map<std::uint32_t, PendingSend> snapshot;
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        for (auto& kv : peer.m_pending) {
            if (kv.first <= ack_seq && !lost_seqs.count(kv.first)) {
                continue;
            }
            if (lost_seqs.count(kv.first) && kv.second.m_retries < kMaxRetries) {
                snapshot[kv.first] = kv.second;
            }
        }
        for (auto it = peer.m_pending.begin(); it != peer.m_pending.end();) {
            // 重传次数耗尽的 pending 一并修剪：接收端放弃后 ack 游标
            // 会跳过该缺口，此处防止 m_pending 无限滞留。
            if ((it->first <= ack_seq && !lost_seqs.count(it->first)) ||
                it->second.m_retries >= kMaxRetries) {
                it = peer.m_pending.erase(it);
            } else {
                ++it;
            }
        }
    }

    auto now = std::chrono::steady_clock::now();
    for (auto& kv : snapshot) {
        resend(&peer, kv.second.m_header, kv.second.m_payload);
        std::lock_guard<std::mutex> lock(peer.m_mu);
        auto it = peer.m_pending.find(kv.first);
        if (it != peer.m_pending.end() && it->second.m_retries < kMaxRetries) {
            it->second.m_last_send = now;
            ++it->second.m_retries;
        }
    }
    return probe_bw;
}

}
