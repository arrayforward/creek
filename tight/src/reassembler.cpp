#include "reassembler.hpp"

#include "peer.hpp"
#include "wire_format.hpp"

#include "tight/fec.hpp"
#include "tight/logger.hpp"

#include <algorithm>
#include <cstring>
#include <vector>

namespace tight::tight_detail {

namespace {

// 异常消息丢弃日志：每 peer 每秒最多一条（接收线程热路径，
// 恶意洪水时不能被日志拖垮）
void log_oversize_drop(Peer& peer, const char* reason, std::uint32_t msg_id,
                       std::size_t value, std::size_t limit) {
    auto now = std::chrono::steady_clock::now();
    if (now - peer.m_oversize_log_ts < std::chrono::seconds(1)) return;
    peer.m_oversize_log_ts = now;
    TIGHT_LOG_WARN(std::string("[tight] 丢弃异常消息(") + reason +
                   "): peer=" + peer.m_id +
                   " msg_id=" + std::to_string(msg_id) +
                   " value=" + std::to_string(value) +
                   " limit=" + std::to_string(limit));
}

} // namespace

void Reassembler::handle_data(Peer& peer, const PacketHeader& header,
                              const Bytes& payload, std::uint32_t rtt_us,
                              double late_multiplier, std::size_t max_message_bytes,
                              const DeliverCallback& deliver) {
    std::uint32_t seq = header.sequence;
    auto now = std::chrono::steady_clock::now();

    // One-way transit via the per-peer clock offset (computed outside the
    // lock: the clock fields are written exclusively on this receiver
    // thread). -1 means the clock is not synced yet -> no accounting.
    std::int64_t transit_us = transit_time_us(peer, header.tick, unix_millis());
    {
        // m_missing_seqs / m_recv_seqs / m_next_expected_seq /
        // m_transit_samples / m_late_samples are also mutated by the report
        // builder on the reactor thread. Guard them with peer.m_mu or the
        // two threads corrupt the std::map (rb_tree) internals -> crash in
        // _S_rebalance_for_erase.
        std::lock_guard<std::mutex> seq_lock(peer.m_mu);

        // Late-packet accounting: transit time above late_multiplier * RTT.
        if (transit_us >= 0) {
            ++peer.m_transit_samples;
            std::uint64_t threshold_us = static_cast<std::uint64_t>(
                late_multiplier * static_cast<double>(rtt_us > 0 ? rtt_us : 10000));
            if (static_cast<std::uint64_t>(transit_us) > threshold_us) {
                ++peer.m_late_samples;
            }
        }

        // seq == 0 为未编号报文（Parity 等）：不参与缺口跟踪，也不允许
        // 初始化序列基准。否则 Parity 先到会以 next_expected=1 初始化，
        // 而 seq 1/2 是握手等控制包、永远不会以 Data 到达，导致基准卡死：
        // 缺口被误报为丢包并引发控制包重传循环、ack_seq 冻结、m_pending
        // 与 m_recv_seqs 无限增长（内存泄漏）。
        if (seq == 0) {
            // 跳过节流统计之外的序列跟踪，但仍走下方的分片重组流程
        } else if (!peer.m_seq_initialized) {
            peer.m_next_expected_seq = seq + 1;
            peer.m_seq_initialized = true;
        } else if (seq >= peer.m_next_expected_seq) {
            for (std::uint32_t g = peer.m_next_expected_seq; g < seq; ++g) {
                if (!peer.m_recv_seqs.count(g)) {
                    peer.m_missing_seqs.emplace(g, now);
                }
            }
            peer.m_recv_seqs.insert(seq);
            while (peer.m_recv_seqs.count(peer.m_next_expected_seq)) {
                peer.m_recv_seqs.erase(peer.m_next_expected_seq);
                ++peer.m_next_expected_seq;
            }
        } else {
            auto mit = peer.m_missing_seqs.find(seq);
            if (mit != peer.m_missing_seqs.end()) {
                peer.m_missing_seqs.erase(mit);
            }
            while (peer.m_recv_seqs.count(peer.m_next_expected_seq)) {
                peer.m_recv_seqs.erase(peer.m_next_expected_seq);
                ++peer.m_next_expected_seq;
            }
        }
    }

    if (header.fragment_count == 0) return;
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        if (peer.m_completed.find(header.message_id) != peer.m_completed.end()) return;
    }
    std::uint16_t idx = header.fragment_index;
    std::uint16_t cnt = header.fragment_count;
    if (idx >= cnt) return;
    // 条目创建前按配置上限校验分片数：合法发送方的数据分片（除末片外）
    // 不小于 64 字节，超限的 fragment_count 必为异常/恶意，直接丢弃，
    // 防止 m_incoming 按虚假分片数预分配耗尽内存。
    const std::size_t max_fragments = max_message_bytes / 64 + 8;
    if (cnt > max_fragments) {
        if (peer.m_drop_log) {
            log_oversize_drop(peer, "fragment_count 超限", header.message_id, cnt, max_fragments);
        }
        return;
    }
    auto& in = peer.m_incoming[header.message_id];
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        if (in.m_message_id == 0) {
            in.m_message_id = header.message_id;
            in.m_total_count = cnt;
            in.m_data_count = header.flags;
            in.m_fragments.assign(cnt, std::nullopt);
            in.m_sizes.assign(cnt, 0);
            in.m_first_seen = now;
        }
        if (in.m_total_count != cnt) return;
        if (idx >= in.m_fragments.size()) return;
        if (!in.m_fragments[idx].has_value()) {
            in.m_fragments[idx] = payload;
            in.m_sizes[idx] = header.reserved;
        }
    }
    bool assembled = try_assemble(peer, in, max_message_bytes, deliver);
    if (assembled) {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        peer.m_completed[header.message_id] = now;
        peer.m_incoming.erase(header.message_id);
    }
}

bool Reassembler::try_assemble(Peer& peer, IncomingMessage& in,
                               std::size_t max_message_bytes,
                               const DeliverCallback& deliver) {
    if (in.m_total_count < 2) return false;
    std::size_t data_count = in.m_data_count > 0 ? in.m_data_count : (in.m_total_count - 1);
    std::size_t parity_count = in.m_total_count - data_count;
    std::size_t have = 0;
    for (std::size_t i = 0; i < data_count; ++i) {
        if (in.m_fragments[i].has_value()) ++have;
    }
    // 直接从分片组装完整消息：一次分配，跳过 4 字节总长前缀，
    // 不再为每个分片做中间拷贝。流的前 4 字节为总长（大端）。
    auto build_msg = [&](std::size_t data_count) -> Bytes {
        std::size_t stream_len = 0;
        for (std::size_t i = 0; i < data_count; ++i) {
            stream_len += std::min<std::size_t>(in.m_sizes[i], in.m_fragments[i]->size());
        }
        if (stream_len < 4) return Bytes{};
        // 读取流前 4 字节的总长前缀（可能横跨分片边界）
        std::uint8_t prefix[4] = {0, 0, 0, 0};
        std::size_t need = 4;
        for (std::size_t i = 0; i < data_count && need > 0; ++i) {
            std::size_t n = std::min<std::size_t>(in.m_sizes[i], in.m_fragments[i]->size());
            std::size_t take = std::min(n, need);
            std::memcpy(prefix + (4 - need), in.m_fragments[i]->data(), take);
            need -= take;
        }
        std::uint32_t total_be = 0;
        std::memcpy(&total_be, prefix, 4);
        std::uint32_t total = to_be32(total_be);
        // 报文声明总长超过配置上限：视为异常消息，丢弃不投递
        if (total > max_message_bytes) {
            if (peer.m_drop_log) {
                log_oversize_drop(peer, "消息总长超限", in.m_message_id, total, max_message_bytes);
            }
            return Bytes{};
        }
        if (total > stream_len - 4) total = static_cast<std::uint32_t>(stream_len - 4);

        Bytes result(total);
        std::size_t skip = 4;   // 跳过总长前缀
        std::size_t out_off = 0;
        for (std::size_t i = 0; i < data_count && out_off < total; ++i) {
            std::size_t n = std::min<std::size_t>(in.m_sizes[i], in.m_fragments[i]->size());
            const std::uint8_t* src = in.m_fragments[i]->data();
            if (skip >= n) { skip -= n; continue; }
            src += skip;
            n -= skip;
            skip = 0;
            if (out_off + n > total) n = total - out_off;
            std::memcpy(result.data() + out_off, src, n);
            out_off += n;
        }
        return result;
    };
    if (have == data_count) {
        for (std::size_t i = 0; i < data_count; ++i) {
            if (!in.m_fragments[i].has_value()) {
                TIGHT_LOG_DEBUG(std::string("[tight] try_assemble missing frag i=") + std::to_string(i) +
                                 " data_count=" + std::to_string(data_count) +
                                 " total=" + std::to_string(in.m_total_count));
                return false;  // race; retry next fragment
            }
        }
        deliver(&peer, build_msg(data_count));
        return true;
    }
    // 统计缺失的数据分片数
    std::size_t multi = 0;
    for (std::size_t i = 0; i < data_count; ++i) {
        if (!in.m_fragments[i].has_value()) ++multi;
    }
    if (multi > parity_count) return false;   // 缺失超过校验能力
    if (multi == 0) return false;
    std::size_t width = 0;
    for (auto& f : in.m_fragments) {
        if (f.has_value()) width = std::max(width, f->size());
    }
    if (width == 0) return false;

    // Reed-Solomon 解码：任意 multi 个缺失分片用任意 multi 个校验分片恢复
    std::vector<std::optional<Bytes>> data_frags(data_count);
    for (std::size_t i = 0; i < data_count; ++i) data_frags[i] = in.m_fragments[i];
    std::vector<std::pair<std::size_t, Bytes>> parities;
    for (std::size_t p = 0; p < parity_count; ++p) {
        if (data_count + p < in.m_fragments.size() &&
            in.m_fragments[data_count + p].has_value()) {
            parities.emplace_back(p, *in.m_fragments[data_count + p]);
        }
    }
    if (multi > parities.size()) return false;
    if (!ReedSolomon::decode(data_frags, parities, width)) return false;

    // 回填恢复出的分片；真实长度由报文内 4 字节总长前缀在 build_msg 裁剪
    for (std::size_t i = 0; i < data_count; ++i) {
        if (!in.m_fragments[i].has_value() && data_frags[i].has_value()) {
            in.m_fragments[i] = *data_frags[i];
            in.m_sizes[i] = static_cast<std::uint16_t>(width);
        }
    }
    deliver(&peer, build_msg(data_count));
    return true;
}

}
