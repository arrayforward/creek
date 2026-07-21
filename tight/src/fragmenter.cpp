#include "fragmenter.hpp"

#include "peer.hpp"
#include "wire_format.hpp"

#include "tight/fec.hpp"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <vector>

namespace tight::tight_detail {

std::uint16_t Fragmenter::compute_parity_count_for(double late_ratio, std::size_t data_count) {
    // Redundancy rate = binary entropy of the late-packet ratio p, scaled by
    // the safety coefficient. H(0) = H(1) = 0 (fully certain -> no
    // redundancy); H peaks at p = 0.5.
    if (late_ratio <= 0.0001 || late_ratio >= 0.9999) return 1;
    double h = -late_ratio * std::log2(late_ratio)
             - (1.0 - late_ratio) * std::log2(1.0 - late_ratio);
    double redundancy = h * kFecSafetyCoefficient;
    std::uint16_t p = static_cast<std::uint16_t>(
        std::ceil(static_cast<double>(data_count) * redundancy));
    if (p < 1) p = 1;
    if (p > 3) p = 3;
    return p;
}

void Fragmenter::fragment_and_send(Peer& peer, Bytes payload, std::size_t mtu,
                                   const SendFragmentCallback& send_fragment) {
    std::size_t frag_payload = mtu > kHeaderSize ? mtu - kHeaderSize : 64;
    if (frag_payload <= 4) frag_payload = 64;
    std::uint32_t msg_id;
    double late_ratio;
    {
        std::lock_guard<std::mutex> lock(peer.m_mu);
        // message_id 使用独立计数器，不消耗数据序列号 m_sequence_out
        do {
            msg_id = static_cast<std::uint32_t>((peer.m_msg_id_out++) & 0x7FFFFFFFu);
        } while (msg_id == 0);
        // FEC redundancy tracks the peer-reported late-packet ratio (each XOR
        // parity fragment recovers exactly one late/lost fragment).
        late_ratio = peer.m_peer_late_ratio;
    }
    std::size_t total = payload.size();
    std::uint32_t total_be = to_be32(static_cast<std::uint32_t>(total & 0xFFFFFFFFULL));
    // 4 字节总长前缀 + 负载：唯一的整体缓冲（分片以区间视图引用，零拷贝）
    Bytes full(4 + payload.size());
    std::memcpy(full.data(), &total_be, 4);
    std::memcpy(full.data() + 4, payload.data(), payload.size());
    std::size_t real_total = full.size();
    std::size_t data_count = (real_total + frag_payload - 1) / frag_payload;
    if (data_count == 0) data_count = 1;
    std::size_t width = frag_payload;
    std::uint16_t parity_count = compute_parity_count_for(late_ratio, data_count);

    // RS 直接以 full 上的分片区间为输入（不足 width 的尾部按零处理，
    // 与补齐后编码结果一致）；区间/校验缓冲用 thread_local 复用，
    // 热点路径摊销零堆分配。
    thread_local std::vector<ReedSolomon::Span> spans;
    spans.clear();
    spans.reserve(data_count);
    for (std::size_t i = 0; i < data_count; ++i) {
        std::size_t off = i * frag_payload;
        std::size_t len = std::min(frag_payload, real_total - off);
        spans.push_back({full.data() + off, len});
    }
    thread_local std::vector<Bytes> parities;
    ReedSolomon::encode_into(spans, parity_count, width, parities);

    std::uint16_t cnt = static_cast<std::uint16_t>(data_count + parity_count);
    std::uint16_t d_cnt = static_cast<std::uint16_t>(data_count);
    for (std::size_t i = 0; i < data_count; ++i) {
        send_fragment(&peer, msg_id, static_cast<std::uint16_t>(i), cnt, d_cnt,
                      static_cast<std::uint16_t>(spans[i].size),
                      spans[i].data, spans[i].size, width, true);
    }
    for (std::uint16_t p = 0; p < parity_count; ++p) {
        send_fragment(&peer, msg_id, static_cast<std::uint16_t>(data_count + p), cnt, d_cnt,
                      static_cast<std::uint16_t>(width), parities[p].data(), width, width, true);
    }
}

}
