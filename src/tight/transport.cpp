#include "creek/tight.hpp"
#include "creek/blocking_queue.hpp"
#include "creek/logger.hpp"
#include "creek/types.hpp"

#include "address.hpp"
#include "socket_platform.hpp"
#include "wire_format.hpp"
#include "wsa.hpp"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace creek {

using namespace tight_detail;

class TightTransport::Impl {
public:
    TightConfig m_config;
    NativeSocket m_sock{kInvalidSocket};
    std::uint16_t m_local_port{};
    std::atomic<bool> m_running{false};
    std::thread m_reactor_thread;

    mutable std::mutex m_send_mutex;
    std::condition_variable m_send_cv;
    std::map<int, std::deque<std::pair<std::string, Bytes>>> m_send_queue;

    mutable std::mutex m_peers_mutex;

    // Worker thread for CPU-bound fragment encoding (FEC + RS).
    // The reactor stays free to process incoming packets.
    struct Peer; // forward
    struct EncodeTask {
        Peer* m_peer;
        Bytes m_payload;
    };
    BlockingQueue<EncodeTask> m_encode_queue{4096};
    std::thread m_encode_thread;

    // Single-producer-multiple-consumer outbound packet queue.
    // The sender thread drains this and calls ::sendto; reactor and encode
    // thread only enqueue. This guarantees sendto (and any token-bucket
    // back-pressure) never blocks the reactor or the encode thread.
    struct OutboundPacket {
        Peer* m_peer;
        Bytes m_datagram;
    };
    BlockingQueue<OutboundPacket> m_outbound_queue{65536};
    std::thread m_sender_thread;
    std::atomic<bool> m_sender_running{false};

    // Dedicated receiver thread. Calls recvfrom + handle_packet.
    std::thread m_receiver_thread;

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
        std::chrono::steady_clock::time_point m_last_report_sent;
        std::uint32_t m_sequence_out{1};
        std::uint32_t m_highest_recv_seq{};
        std::map<std::uint32_t, PendingSend> m_pending;
        std::map<std::uint32_t, IncomingMessage> m_incoming;
        std::map<std::uint32_t, std::chrono::steady_clock::time_point> m_completed;
        std::map<std::uint32_t, std::chrono::steady_clock::time_point> m_missing_seqs;
        std::set<std::uint32_t> m_recv_seqs;
        std::uint32_t m_next_expected_seq{};
        bool m_seq_initialized{false};
        std::uint32_t m_sender_rtt_us{};
        std::uint32_t m_last_hb_tick{};
        std::uint64_t m_delay_samples{};
        std::uint64_t m_delay_exceeded{};
        double m_delay_entropy{0.0};
        bool m_reconnect{};
    };

    std::map<std::string, Peer> m_peers;

    struct AddrKey {
        std::uint32_t m_addr;
        std::uint16_t m_port;
        bool operator<(const AddrKey& o) const {
            if (m_addr != o.m_addr) return m_addr < o.m_addr;
            return m_port < o.m_port;
        }
    };
    std::map<AddrKey, std::string> m_peer_by_addr;

    std::uint32_t m_local_client_id{};
    std::uint64_t m_local_session_id{};

    double m_token_bucket{0};
    std::chrono::steady_clock::time_point m_token_bucket_time;

    mutable std::mutex m_callback_mutex;
    TightTransport::MessageCallback m_message_cb;
    TightTransport::PeerCallback m_peer_cb;

    BandwidthEstimator m_bandwidth;

    std::mt19937_64 m_rng;
    std::mutex m_rng_mutex;

    Impl(TightConfig cfg)
        : m_config(std::move(cfg)),
          m_bandwidth(m_config.initial_bandwidth_bytes) {
        m_local_client_id = static_cast<std::uint32_t>(random_u64() & 0x7FFFFFFFu);
        m_local_session_id = random_u64();
        if (m_local_client_id == 0) m_local_client_id = 1;
        m_token_bucket_time = std::chrono::steady_clock::now();
    }

    std::uint64_t random_u64() {
        std::lock_guard<std::mutex> lock(m_rng_mutex);
        if (m_rng() == 0 && (m_rng() == 0)) {
            std::random_device rd;
            return (static_cast<std::uint64_t>(rd()) << 32) | rd();
        }
        std::uint64_t a = m_rng();
        std::uint64_t b = m_rng();
        return a ^ (b << 1);
    }

    void set_message_callback(TightTransport::MessageCallback cb) {
        std::lock_guard<std::mutex> lock(m_callback_mutex);
        m_message_cb = std::move(cb);
    }

    void set_peer_callback(TightTransport::PeerCallback cb) {
        std::lock_guard<std::mutex> lock(m_callback_mutex);
        m_peer_cb = std::move(cb);
    }

    void fire_peer_event(Peer* peer, LinkState new_state) {
        TightTransport::PeerCallback cb_copy;
        {
            std::lock_guard<std::mutex> lock(m_callback_mutex);
            cb_copy = m_peer_cb;
        }
        if (!cb_copy) return;
        PeerEvent ev{};
        ev.id = peer->m_id;
        ev.role = peer->m_role;
        ev.state = new_state;
        ev.client_id = peer->m_peer_client_id;
        cb_copy(ev);
    }

    void deliver_message(Peer* peer, Bytes payload) {
        TightTransport::MessageCallback cb_copy;
        {
            std::lock_guard<std::mutex> lock(m_callback_mutex);
            cb_copy = m_message_cb;
        }
        if (!cb_copy) return;
        cb_copy(peer->m_id, std::move(payload));
    }

    bool start() {
        if (m_running.load()) return true;
        if (!wsa_acquire()) return false;

        m_sock = static_cast<NativeSocket>(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP));
        if (m_sock == kInvalidSocket) {
            wsa_release();
            return false;
        }

        sockaddr_in local{};
        if (!resolve_address(m_config.bind.host, m_config.bind.port, local)) {
            close_socket(m_sock);
            m_sock = kInvalidSocket;
            wsa_release();
            return false;
        }
        if (::bind(m_sock, reinterpret_cast<sockaddr*>(&local), sizeof(local)) != 0) {
            close_socket(m_sock);
            m_sock = kInvalidSocket;
            wsa_release();
            return false;
        }

        // Large buffers are essential on localhost: the receiver thread does
        // per-packet work (dispatch + logging), so a small kernel buffer
        // (Windows default 64 KiB ~ 54 x 1200-byte datagrams) overflows and
        // silently drops datagrams under any burst. 8 MiB absorbs bursts.
        int bufsize = 8 * 1024 * 1024;
        creek_setsockopt(m_sock, SOL_SOCKET, SO_RCVBUF,
                         reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));
        creek_setsockopt(m_sock, SOL_SOCKET, SO_SNDBUF,
                         reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));
#ifdef _WIN32
        // On Windows, set SO_EXCLUSIVEADDRUSE off and disable WSAECONNRESET
        // delivery on UDP sockets (we don't want ICMP unreachable errors to
        // abort the recvfrom loop when a peer disappears mid-stream).
        BOOL false_ = FALSE;
        setsockopt(m_sock, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char*>(&false_), sizeof(false_));
        // SIO_UDP_CONNRESET (0x9800000C) -- disable WSAECONNRESET errors.
        DWORD bytes_returned = 0;
        WSAIoctl(m_sock, SIO_UDP_CONNRESET, &false_, sizeof(false_),
                  nullptr, 0, &bytes_returned, nullptr, nullptr);
        u_long nonblock = 1;
        ioctlsocket(m_sock, FIONBIO, &nonblock);
#else
        int fl = fcntl(m_sock, F_GETFL, 0);
        if (fl >= 0) fcntl(m_sock, F_SETFL, fl | O_NONBLOCK);
#endif

        sockaddr_in bound{};
        SockLen blen = sizeof(bound);
        if (::getsockname(m_sock, reinterpret_cast<sockaddr*>(&bound), &blen) == 0) {
            m_local_port = ntohs(bound.sin_port);
        }

        m_running.store(true);
        m_reactor_thread = std::thread([this] { reactor_loop(); });
        m_receiver_thread = std::thread([this] { receiver_loop(); });
        m_encode_thread = std::thread([this] { encode_loop(); });
        m_sender_thread = std::thread([this] { sender_loop(); });
        return true;
    }

    void stop() {
        if (!m_running.exchange(false)) {
            return;
        }
        m_encode_queue.close();
        m_outbound_queue.close();
        if (m_reactor_thread.joinable()) {
            try { m_reactor_thread.join(); } catch (...) {}
        }
        if (m_receiver_thread.joinable()) {
            try { m_receiver_thread.join(); } catch (...) {}
        }
        if (m_encode_thread.joinable()) {
            try { m_encode_thread.join(); } catch (...) {}
        }
        if (m_sender_thread.joinable()) {
            try { m_sender_thread.join(); } catch (...) {}
        }
        if (m_sock != kInvalidSocket) {
            close_socket(m_sock);
            m_sock = kInvalidSocket;
        }
        wsa_release();
    }

    bool connect(const RemotePeer& remote) {
        if (!m_running.load()) return false;
        sockaddr_in addr{};
        if (!resolve_address(remote.address.host, remote.address.port, addr)) return false;
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        AddrKey key{addr.sin_addr.s_addr, addr.sin_port};
        auto addr_it = m_peer_by_addr.find(key);
        if (addr_it != m_peer_by_addr.end() && addr_it->second != remote.id) {
            // Remove the old peer entry; the new add_peer below will create a fresh one.
            // We can't move a Peer (it holds a mutex), so we just erase and recreate.
            std::string old_id = addr_it->second;
            m_peers.erase(old_id);
            addr_it->second = remote.id;
        }
        auto& peer = m_peers[remote.id];
        peer.m_id = remote.id;
        peer.m_addr = addr;
        peer.m_addr_set = true;
        peer.m_role = LinkRole::Node;
        peer.m_reconnect = true;
        m_peer_by_addr[key] = remote.id;
        if (peer.m_state == LinkState::Closed) {
            peer.m_state = LinkState::Handshake;
            peer.m_last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
        }
        send_handshake(&peer);
        return true;
    }

    bool send_message(const std::string& peer_id, Bytes payload, int priority = 0) {
        if (!m_running.load()) return false;
        {
            std::lock_guard<std::mutex> lock(m_send_mutex);
            std::size_t total = 0;
            for (const auto& kv : m_send_queue) total += kv.second.size();
            if (total >= m_config.queue_limit) return false;
            m_send_queue[priority].emplace_back(peer_id, std::move(payload));
        }
        m_send_cv.notify_one();
        return true;
    }

    std::vector<PeerEvent> peers_snapshot() const {
        std::vector<PeerEvent> out;
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        out.reserve(m_peers.size());
        for (const auto& kv : m_peers) {
            PeerEvent ev{};
            ev.id = kv.second.m_id;
            ev.role = kv.second.m_role;
            ev.state = kv.second.m_state;
            ev.client_id = kv.second.m_peer_client_id;
            out.push_back(std::move(ev));
        }
        return out;
    }

    void reactor_loop() {
        auto next_tick = std::chrono::steady_clock::now();
        while (m_running.load(std::memory_order_acquire)) {
            send_handshakes();
            send_heartbeats();
            send_reports();
            check_offline();
            process_send_queue();
            next_tick += m_config.flush_interval;
            auto now = std::chrono::steady_clock::now();
            if (now < next_tick) {
                std::this_thread::sleep_for(next_tick - now);
            } else {
                std::this_thread::yield();
            }
        }
    }

    void refill_token_bucket() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now - m_token_bucket_time).count();
        if (elapsed_us <= 0) return;
        std::uint64_t bps = m_bandwidth.bytes_per_second();
        double tokens = static_cast<double>(bps) * static_cast<double>(elapsed_us) / 1000000.0;
        m_token_bucket += tokens;
        double cap = static_cast<double>(m_config.mtu) * 4.0;
        if (m_token_bucket > cap) m_token_bucket = cap;
        m_token_bucket_time = now;
    }

    void receive_datagrams() {
        std::uint8_t buf[2048];
        while (m_running.load()) {
            sockaddr_in from{};
            socklen_t flen = sizeof(from);
            int n = ::recvfrom(m_sock, reinterpret_cast<char*>(buf), sizeof(buf), 0,
                               reinterpret_cast<sockaddr*>(&from), &flen);
            if (n < 0) {
                int e = last_socket_error();
                if (would_block(e)) return;
                return;
            }
            if (static_cast<std::size_t>(n) < kHeaderSize) continue;
            Bytes datagram(buf, buf + n);
            PacketHeader header{};
            Bytes payload;
            if (!PacketCodec::decode(datagram, header, payload)) continue;
            handle_packet(from, header, payload);
        }
    }

    Peer* find_or_create_peer_by_addr(const sockaddr_in& from) {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        AddrKey key{from.sin_addr.s_addr, from.sin_port};
        auto it = m_peer_by_addr.find(key);
        if (it != m_peer_by_addr.end()) {
            auto pit = m_peers.find(it->second);
            if (pit != m_peers.end()) return &pit->second;
        }
        std::string id = "anon-" + std::to_string(static_cast<unsigned>(ntohs(from.sin_port))) +
                         "-" + std::to_string(random_u64() & 0xFFFFFFFFu);
        auto& peer = m_peers[id];
        peer.m_id = id;
        peer.m_addr = from;
        peer.m_addr_set = true;
        peer.m_role = LinkRole::Leaf;
        peer.m_state = LinkState::Handshake;
        peer.m_last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
        m_peer_by_addr[key] = id;
        return &peer;
    }

    void handle_packet(const sockaddr_in& from, const PacketHeader& header, const Bytes& payload) {
        // NOTE: no per-packet logging here. This fires for every datagram
        // (data/parity/ack/heartbeat) on the single receiver thread; a log
        // write + fflush per datagram slows the recvfrom loop enough to
        // overflow the kernel UDP buffer and cause massive packet loss.
        Peer* peer;
        {
            std::lock_guard<std::mutex> lock(m_peers_mutex);
            AddrKey key{from.sin_addr.s_addr, from.sin_port};
            auto it = m_peer_by_addr.find(key);
            if (it == m_peer_by_addr.end()) {
                std::string id = "anon-" + std::to_string(static_cast<unsigned>(ntohs(from.sin_port))) +
                                 "-" + std::to_string(random_u64() & 0xFFFFFFFFu);
                auto& np = m_peers[id];
                np.m_id = id;
                np.m_addr = from;
                np.m_addr_set = true;
                np.m_role = LinkRole::Leaf;
                np.m_state = LinkState::Handshake;
                np.m_last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
                m_peer_by_addr[key] = id;
                peer = &np;
            } else {
                auto pit = m_peers.find(it->second);
                if (pit == m_peers.end()) return;
                peer = &pit->second;
            }
        }

        peer->m_last_recv = std::chrono::steady_clock::now();
        if (peer->m_peer_client_id == 0 && header.client_id != 0) {
            peer->m_peer_client_id = header.client_id;
        }
        if (peer->m_peer_session_id == 0 && header.session_id != 0) {
            peer->m_peer_session_id = header.session_id;
        }

        bool need_ack = false;
        std::uint32_t ack_to_send = 0;
        if (header.sequence > peer->m_highest_recv_seq) {
            peer->m_highest_recv_seq = header.sequence;
        }

        switch (header.type) {
            case PacketType::Handshake:    handle_handshake(peer, header, payload);
                                           need_ack = true; ack_to_send = header.sequence; break;
            case PacketType::HandshakeAck: handle_handshake_ack(peer, header, payload);
                                           need_ack = true; ack_to_send = header.sequence; break;
            case PacketType::Online:       handle_online(peer, header, payload);
                                           need_ack = true; ack_to_send = header.sequence; break;
            case PacketType::Heartbeat:    handle_heartbeat(peer, header, payload); break;
            case PacketType::Bye:          handle_bye(peer, header, payload); break;
            case PacketType::Data:         handle_data(peer, header, payload); break;
            case PacketType::Parity:       handle_data(peer, header, payload); break;
            case PacketType::Ack:          handle_ack(peer, header); break;
            case PacketType::Report:       handle_report(peer, header, payload); break;
        }

        if (need_ack) {
            send_ack_packet(peer, ack_to_send);
        }
    }

    void handle_handshake(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (payload.size() < 3) return;
        std::uint8_t role_byte = payload[0];
        std::uint16_t id_size = static_cast<std::uint16_t>(payload[1]) << 8U;
        id_size |= static_cast<std::uint16_t>(payload[2]);
        if (id_size == 0 || payload.size() < 3U + id_size) return;
        std::string peer_id(reinterpret_cast<const char*>(payload.data() + 3), id_size);
        std::string token(reinterpret_cast<const char*>(payload.data() + 3 + id_size),
                          payload.size() - 3U - id_size);
        if (token != m_config.token) return;
        peer->m_id = std::move(peer_id);
        peer->m_role = role_byte == static_cast<std::uint8_t>(LinkRole::Node)
                         ? LinkRole::Node
                         : LinkRole::Leaf;
        if (!peer->m_reconnect && peer->m_addr_set) {
            send_handshake(peer);
        }
        if (peer->m_state != LinkState::Established && peer->m_state != LinkState::Online) {
            peer->m_state = LinkState::Established;
            fire_peer_event(peer, LinkState::Established);
        }
        send_control(peer, PacketType::HandshakeAck, Bytes{}, true);
    }

    void handle_handshake_ack(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->m_state == LinkState::Online) return;
        if (peer->m_state == LinkState::Handshake || peer->m_state == LinkState::Established) {
            send_control(peer, PacketType::Online, Bytes{}, true);
            peer->m_state = LinkState::Online;
            fire_peer_event(peer, LinkState::Online);
        }
    }

    void handle_online(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->m_state != LinkState::Online) {
            peer->m_state = LinkState::Online;
            fire_peer_event(peer, LinkState::Online);
        }
    }

    void handle_heartbeat(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (payload.size() >= 4) {
            std::uint32_t rtt_be = 0;
            std::memcpy(&rtt_be, payload.data(), 4);
            peer->m_sender_rtt_us = to_be32(rtt_be);
        }
        peer->m_last_hb_tick = header.tick;
    }

    void handle_bye(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->m_state != LinkState::Closed) {
            peer->m_state = LinkState::Closed;
            fire_peer_event(peer, LinkState::Closed);
        }
    }

    void handle_data(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        std::uint32_t seq = header.sequence;
        auto now = std::chrono::steady_clock::now();
        {
            // m_missing_seqs / m_recv_seqs / m_next_expected_seq / m_delay_*
            // are also mutated by send_reports() on the reactor thread. Guard
            // them with peer->m_mu or the two threads corrupt the std::map
            // (rb_tree) internals -> crash in _S_rebalance_for_erase.
            std::lock_guard<std::mutex> seq_lock(peer->m_mu);
            std::uint32_t rtt_threshold = peer->m_sender_rtt_us > 0 ? peer->m_sender_rtt_us : 10000;

            if (!peer->m_seq_initialized) {
                peer->m_next_expected_seq = seq + 1;
                peer->m_seq_initialized = true;
            } else if (seq >= peer->m_next_expected_seq) {
                for (std::uint32_t g = peer->m_next_expected_seq; g < seq; ++g) {
                    if (!peer->m_recv_seqs.count(g)) {
                        peer->m_missing_seqs.emplace(g, now);
                    }
                }
                peer->m_recv_seqs.insert(seq);
                while (peer->m_recv_seqs.count(peer->m_next_expected_seq)) {
                    peer->m_recv_seqs.erase(peer->m_next_expected_seq);
                    ++peer->m_next_expected_seq;
                }
            } else {
                auto mit = peer->m_missing_seqs.find(seq);
                if (mit != peer->m_missing_seqs.end()) {
                    auto delay_us = std::chrono::duration_cast<std::chrono::microseconds>(now - mit->second).count();
                    peer->m_delay_samples++;
                    if (delay_us > static_cast<long long>(rtt_threshold)) {
                        peer->m_delay_exceeded++;
                    }
                    peer->m_missing_seqs.erase(mit);
                }
                while (peer->m_recv_seqs.count(peer->m_next_expected_seq)) {
                    peer->m_recv_seqs.erase(peer->m_next_expected_seq);
                    ++peer->m_next_expected_seq;
                }
            }
        }

        if (header.fragment_count == 0) return;
        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            if (peer->m_completed.contains(header.message_id)) return;
        }
        std::uint16_t idx = header.fragment_index;
        std::uint16_t cnt = header.fragment_count;
        if (idx >= cnt) return;
        auto& in = peer->m_incoming[header.message_id];
        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
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
        bool assembled = try_assemble(peer, in);
        if (assembled) {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            peer->m_completed[header.message_id] = now;
            peer->m_incoming.erase(header.message_id);
        }
    }

    bool try_assemble(Peer* peer, IncomingMessage& in) {
        if (in.m_total_count < 2) return false;
        std::size_t data_count = in.m_data_count > 0 ? in.m_data_count : (in.m_total_count - 1);
        std::size_t parity_count = in.m_total_count - data_count;
        std::size_t have = 0;
        for (std::size_t i = 0; i < data_count; ++i) {
            if (in.m_fragments[i].has_value()) ++have;
        }
        auto build_msg = [&](const std::vector<Bytes>& parts, const std::vector<std::uint16_t>& real_sizes) -> Bytes {
            Bytes full;
            for (std::size_t i = 0; i < parts.size() && i < real_sizes.size(); ++i) {
                std::size_t n = std::min<std::size_t>(real_sizes[i], parts[i].size());
                full.insert(full.end(), parts[i].begin(), parts[i].begin() + n);
            }
            if (full.size() < 4) return Bytes{};
            std::uint32_t total_be = 0;
            std::memcpy(&total_be, full.data(), 4);
            std::uint32_t total = to_be32(total_be);
            if (total > full.size() - 4) total = static_cast<std::uint32_t>(full.size() - 4);
            return Bytes(full.begin() + 4, full.begin() + 4 + total);
        };
        if (have == data_count) {
            std::vector<Bytes> parts;
            std::vector<std::uint16_t> real_sizes;
            parts.reserve(data_count);
            real_sizes.reserve(data_count);
            for (std::size_t i = 0; i < data_count; ++i) {
                if (!in.m_fragments[i].has_value()) {
                    CREEK_LOG_DEBUG(std::string("[tight] try_assemble missing frag i=") + std::to_string(i) +
                                     " data_count=" + std::to_string(data_count) +
                                     " total=" + std::to_string(in.m_total_count));
                    return false;  // race; retry next fragment
                }
                parts.push_back(*in.m_fragments[i]);
                real_sizes.push_back(in.m_sizes[i]);
            }
            deliver_message(peer, build_msg(parts, real_sizes));
            return true;
        }
        std::size_t missing = SIZE_MAX;
        std::size_t multi = 0;
        for (std::size_t i = 0; i < data_count; ++i) {
            if (!in.m_fragments[i].has_value()) { ++multi; missing = i; }
        }
        if (multi > parity_count) return false;
        if (multi == 0) return false;
        std::size_t width = 0;
        for (auto& f : in.m_fragments) {
            if (f.has_value()) width = std::max(width, f->size());
        }
        if (width == 0) return false;
        std::vector<Bytes> frags(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            frags[i] = in.m_fragments[i].has_value() ? *in.m_fragments[i] : Bytes(width, 0);
        }
        std::vector<std::size_t> missing_indices;
        for (std::size_t i = 0; i < data_count; ++i) {
            if (!in.m_fragments[i].has_value()) missing_indices.push_back(i);
        }
        for (std::size_t p = 0; p < parity_count && !missing_indices.empty(); ++p) {
            if (data_count + p >= in.m_fragments.size()) continue;
            if (!in.m_fragments[data_count + p].has_value()) continue;
            Bytes parity_data = *in.m_fragments[data_count + p];
            std::vector<Bytes> rotated(data_count);
            for (std::size_t i = 0; i < data_count; ++i) {
                rotated[i] = frags[(i + p) % data_count];
            }
            std::vector<std::size_t> rotated_missing;
            for (auto idx : missing_indices) {
                rotated_missing.push_back((idx + data_count - p) % data_count);
            }
            if (rotated_missing.size() == 1) {
                if (ReedSolomon::recover_one(rotated, parity_data, rotated_missing[0], width)) {
                    frags[(rotated_missing[0] + p) % data_count] = rotated[rotated_missing[0]];
                    in.m_fragments[(rotated_missing[0] + p) % data_count] = rotated[rotated_missing[0]];
                    in.m_sizes[(rotated_missing[0] + p) % data_count] = in.m_sizes[data_count + p];
                    missing_indices.clear();
                    break;
                }
            }
        }
        if (!missing_indices.empty()) return false;
        std::vector<Bytes> recovered_parts(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            recovered_parts[i] = frags[i];
        }
        deliver_message(peer, build_msg(recovered_parts, in.m_sizes));
        return true;
    }

    void handle_ack(Peer* peer, const PacketHeader& header) {
        std::uint32_t ack = header.acknowledgment;
        if (ack == 0) return;
        std::size_t erased_bytes = 0;
        std::chrono::steady_clock::time_point last_send;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            auto it = peer->m_pending.find(ack);
            if (it == peer->m_pending.end()) return;
            last_send = it->second.m_last_send;
            erased_bytes = it->second.m_bytes;
            peer->m_pending.erase(it);
            found = true;
        }
        if (!found) return;
        auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - last_send);
        if (rtt.count() < 0) rtt = std::chrono::microseconds(0);
        m_bandwidth.on_ack(erased_bytes, rtt);
    }

    void handle_report(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (payload.size() < 12) return;
        std::uint32_t ack_seq_be = 0;
        std::uint16_t delay_ratio_be = 0;
        std::uint16_t lost_count_be = 0;
        std::uint32_t hb_tick_be = 0;
        std::memcpy(&ack_seq_be, payload.data(), 4);
        std::memcpy(&delay_ratio_be, payload.data() + 4, 2);
        std::memcpy(&lost_count_be, payload.data() + 6, 2);
        std::memcpy(&hb_tick_be, payload.data() + 8, 4);
        std::uint32_t ack_seq = to_be32(ack_seq_be);
        std::uint16_t delay_ratio = to_be16(delay_ratio_be);
        std::uint16_t lost_count = to_be16(lost_count_be);
        // hb_tick (offset 8) is parsed for wire-format completeness but is no
        // longer used to update the bandwidth estimator (see NOTE below).
        (void)to_be32(hb_tick_be);
        std::size_t expected = 12U + static_cast<std::size_t>(lost_count) * 4U;
        if (payload.size() < expected) return;

        // NOTE: do NOT feed the report round-trip time into the bandwidth
        // estimator. hb_tick is echoed back only after the peer's next report
        // tick (up to report_interval later), so this "rtt" is dominated by
        // batching delay, not network RTT. A tiny sample like 64 bytes / 50ms
        // (~1 KB/s) would drag the estimate down and starve the sender's
        // token bucket. Real bandwidth samples come from handle_ack.

        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            double p = static_cast<double>(delay_ratio) / 10000.0;
            if (p <= 0.0 || p >= 1.0) {
                peer->m_delay_entropy = 0.0;
            } else {
                double h = -p * std::log2(p) - (1.0 - p) * std::log2(1.0 - p);
                peer->m_delay_entropy = h;
            }
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
            std::lock_guard<std::mutex> lock(peer->m_mu);
            for (auto& kv : peer->m_pending) {
                if (kv.first <= ack_seq && !lost_seqs.count(kv.first)) {
                    continue;
                }
                if (lost_seqs.count(kv.first) && kv.second.m_retries < 10) {
                    snapshot[kv.first] = kv.second;
                }
            }
            for (auto it = peer->m_pending.begin(); it != peer->m_pending.end();) {
                if (it->first <= ack_seq && !lost_seqs.count(it->first)) {
                    it = peer->m_pending.erase(it);
                } else {
                    ++it;
                }
            }
        }

        auto now = std::chrono::steady_clock::now();
        for (auto& kv : snapshot) {
            Bytes datagram = PacketCodec::encode(kv.second.m_header, kv.second.m_payload);
            send_raw(peer, datagram);
            std::lock_guard<std::mutex> lock(peer->m_mu);
            auto it = peer->m_pending.find(kv.first);
            if (it != peer->m_pending.end() && it->second.m_retries < 10) {
                it->second.m_last_send = now;
                ++it->second.m_retries;
            }
        }
    }

    void send_control(Peer* peer, PacketType type, const Bytes& payload, bool ackable) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = type;
        header.client_id = m_local_client_id;
        header.session_id = m_local_session_id;
        if (ackable) {
            header.sequence = peer->m_sequence_out++;
        }
        header.payload_size = static_cast<std::uint16_t>(payload.size());
        header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
        Bytes datagram = PacketCodec::encode(header, payload);
        send_raw(peer, datagram);
        if (ackable) {
            auto& ps = peer->m_pending[header.sequence];
            ps.m_header = header;
            ps.m_payload = payload;
            ps.m_last_send = std::chrono::steady_clock::now();
            ps.m_bytes = datagram.size();
        }
    }

    void send_ack_packet(Peer* peer, std::uint32_t ack) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = PacketType::Ack;
        header.client_id = m_local_client_id;
        header.session_id = m_local_session_id;
        header.acknowledgment = ack;
        header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
        Bytes datagram = PacketCodec::encode(header, Bytes{});
        send_raw(peer, datagram);
    }

    void send_handshake(Peer* peer) {
        Bytes payload;
        payload.push_back(static_cast<std::uint8_t>(m_config.role));
        auto id_size = static_cast<std::uint16_t>(
            std::min<std::size_t>(m_config.id.size(), 65535U));
        payload.push_back(static_cast<std::uint8_t>((id_size >> 8U) & 0xFFU));
        payload.push_back(static_cast<std::uint8_t>(id_size & 0xFFU));
        payload.insert(payload.end(), m_config.id.begin(), m_config.id.begin() + id_size);
        payload.insert(payload.end(), m_config.token.begin(), m_config.token.end());
        peer->m_last_handshake_sent = std::chrono::steady_clock::now();
        send_control(peer, PacketType::Handshake, payload, true);
    }

    void send_data_packet(Peer* peer, std::uint32_t msg_id, std::uint16_t idx,
                          std::uint16_t cnt, std::uint16_t data_cnt,
                          std::uint16_t real_size,
                          const Bytes& payload, bool ackable) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = (idx + 1 == cnt) ? PacketType::Parity : PacketType::Data;
        header.flags = data_cnt;
        bool is_data = (header.type == PacketType::Data);
        header.client_id = m_local_client_id;
        header.session_id = m_local_session_id;
        bool is_acked_data = ackable && is_data;
        // Brief lock: only for sequence assignment and pending insertion.
        // send_raw is called WITHOUT any lock, so the reactor thread can run
        // even when this thread is blocked on token-bucket back-pressure.
        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            if (is_acked_data) {
                header.sequence = peer->m_sequence_out++;
            }
            header.message_id = msg_id;
            header.fragment_index = idx;
            header.fragment_count = cnt;
            header.reserved = real_size;
            header.payload_size = static_cast<std::uint16_t>(payload.size());
            header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
            if (is_acked_data) {
                auto& ps = peer->m_pending[header.sequence];
                ps.m_header = header;
                ps.m_payload = payload;
                ps.m_last_send = std::chrono::steady_clock::now();
                ps.m_bytes = 0;  // filled in after encode to avoid re-encoding
            }
        }
        Bytes datagram = PacketCodec::encode(header, payload);
        send_raw(peer, datagram);
        if (is_acked_data) {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            peer->m_pending[header.sequence].m_bytes = datagram.size();
        }
    }

    // Enqueue an outbound packet. The sender thread will do the actual sendto
    // (with token-bucket back-pressure). This MUST NOT block the caller.
    void send_raw(Peer* peer, const Bytes& datagram) {
        if (!peer->m_addr_set || m_sock == kInvalidSocket) return;
        m_outbound_queue.try_push(OutboundPacket{peer, Bytes(datagram)});
    }

    void receiver_loop() {
        // Continuously recvfrom and dispatch to handle_packet. This
        // guarantees the reactor thread is never blocked on socket I/O.
        std::uint8_t buf[2048];
        while (m_running.load(std::memory_order_acquire)) {
            sockaddr_in from{};
            SockLen flen = sizeof(from);
            int n = creek_recvfrom(m_sock, reinterpret_cast<char*>(buf),
                                   static_cast<int>(sizeof(buf)), 0,
                                   reinterpret_cast<sockaddr*>(&from), &flen);
            if (n < 0) {
                int e = last_socket_error();
                if (would_block(e)) {
                    // Avoid busy-spin: short sleep then retry.
                    std::this_thread::sleep_for(std::chrono::microseconds(500));
                    continue;
                }
                // Other error: brief sleep to avoid hot loop, then retry.
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            if (static_cast<std::size_t>(n) < kHeaderSize) continue;
            Bytes datagram(buf, buf + n);
            PacketHeader header{};
            Bytes payload;
            if (!PacketCodec::decode(datagram, header, payload)) continue;
            handle_packet(from, header, payload);
        }
    }

    void sender_loop() {
        m_sender_running.store(true);
        while (m_running.load(std::memory_order_acquire)) {
            OutboundPacket pkt;
            // Wait briefly for a packet.
            auto opt = m_outbound_queue.take_for(std::chrono::milliseconds(10));
            if (!opt) continue;
            pkt = std::move(*opt);
            // Send it with token-bucket back-pressure.
            if (!pkt.m_peer->m_addr_set || m_sock == kInvalidSocket) continue;
            double cost = static_cast<double>(pkt.m_datagram.size());
            refill_token_bucket();
            while (m_running.load(std::memory_order_acquire) && m_token_bucket < cost) {
                auto rate = std::max<std::uint64_t>(m_bandwidth.bytes_per_second(), 1U);
                auto deficit = cost - m_token_bucket;
                auto wait_us = static_cast<std::uint64_t>(
                    std::max(1.0, deficit * 1000000.0 / static_cast<double>(rate)));
                std::this_thread::sleep_for(std::chrono::microseconds(
                    std::min<std::uint64_t>(wait_us, 10000U)));
                refill_token_bucket();
            }
            if (m_token_bucket >= cost) m_token_bucket -= cost;
            creek_sendto(m_sock, reinterpret_cast<const char*>(pkt.m_datagram.data()),
                         static_cast<int>(pkt.m_datagram.size()), 0,
                         reinterpret_cast<const sockaddr*>(&pkt.m_peer->m_addr),
                         static_cast<int>(sizeof(pkt.m_peer->m_addr)));
        }
        // Drain remaining packets on shutdown.
        while (true) {
            auto opt = m_outbound_queue.poll();
            if (!opt) break;
        }
        m_sender_running.store(false);
    }

    void send_handshakes() {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        for (auto& kv : m_peers) {
            auto& peer = kv.second;
            if (peer.m_state != LinkState::Handshake) continue;
            if (peer.m_pending.empty()) {
                send_handshake(&peer);
            }
        }
    }

    void send_heartbeats() {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : m_peers) {
            auto& peer = kv.second;
            if (peer.m_state != LinkState::Established && peer.m_state != LinkState::Online) continue;
            if (now - peer.m_last_heartbeat_sent < m_config.heartbeat) continue;
            Bytes hb_payload(4);
            std::uint32_t rtt_us = static_cast<std::uint32_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(m_bandwidth.rtt()).count());
            std::uint32_t rtt_be = to_be32(rtt_us);
            std::memcpy(hb_payload.data(), &rtt_be, 4);
            send_control(&peer, PacketType::Heartbeat, hb_payload, false);
            peer.m_last_heartbeat_sent = now;
        }
    }

    void send_reports() {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : m_peers) {
            auto& peer = kv.second;
            if (peer.m_state != LinkState::Established && peer.m_state != LinkState::Online) continue;
            if (now - peer.m_last_report_sent < m_config.report_interval) continue;
            std::vector<std::uint32_t> lost_seqs;
            std::uint16_t ratio_val;
            std::uint32_t ack_seq;
            {
                // m_missing_seqs / m_delay_* / m_next_expected_seq are also
                // mutated by handle_data() on the receiver thread. Guard with
                // peer.m_mu.
                std::lock_guard<std::mutex> seq_lock(peer.m_mu);
                std::uint32_t rtt_threshold = peer.m_sender_rtt_us > 0 ? peer.m_sender_rtt_us : 10000;
                std::uint32_t loss_threshold = rtt_threshold * 7 / 2;
                if (rtt_threshold < 10000) loss_threshold = 100000;

                for (auto mit = peer.m_missing_seqs.begin(); mit != peer.m_missing_seqs.end();) {
                    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - mit->second).count();
                    if (elapsed_us > static_cast<long long>(loss_threshold)) {
                        lost_seqs.push_back(mit->first);
                        mit = peer.m_missing_seqs.erase(mit);
                    } else {
                        ++mit;
                    }
                }

                double delay_ratio = 0.0;
                if (peer.m_delay_samples > 0) {
                    delay_ratio = static_cast<double>(peer.m_delay_exceeded) / static_cast<double>(peer.m_delay_samples);
                }
                ratio_val = static_cast<std::uint16_t>(delay_ratio * 10000.0);
                ack_seq = peer.m_seq_initialized ? (peer.m_next_expected_seq > 0 ? peer.m_next_expected_seq - 1 : 0) : 0;
                peer.m_delay_samples = 0;
                peer.m_delay_exceeded = 0;
            }

            std::uint16_t lost_count = static_cast<std::uint16_t>(lost_seqs.size());
            if (lost_count > 256) lost_count = 256;
            Bytes payload(12 + lost_count * 4);
            std::uint32_t ack_seq_be = to_be32(ack_seq);
            std::uint16_t ratio_be = to_be16(ratio_val);
            std::uint16_t lost_be = to_be16(lost_count);
            std::uint32_t hb_tick_be = to_be32(peer.m_last_hb_tick);
            std::memcpy(payload.data(), &ack_seq_be, 4);
            std::memcpy(payload.data() + 4, &ratio_be, 2);
            std::memcpy(payload.data() + 6, &lost_be, 2);
            std::memcpy(payload.data() + 8, &hb_tick_be, 4);
            for (std::uint16_t i = 0; i < lost_count; ++i) {
                std::uint32_t seq_be = to_be32(lost_seqs[i]);
                std::memcpy(payload.data() + 12 + i * 4, &seq_be, 4);
            }

            PacketHeader rpt{};
            rpt.magic = kMagic;
            rpt.version = kVersion;
            rpt.type = PacketType::Report;
            rpt.client_id = m_local_client_id;
            rpt.session_id = m_local_session_id;
            rpt.payload_size = static_cast<std::uint16_t>(payload.size());
            rpt.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
            Bytes datagram = PacketCodec::encode(rpt, payload);
            send_raw(&peer, datagram);

            peer.m_last_report_sent = now;
        }
    }

    void check_offline() {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : m_peers) {
            auto& peer = kv.second;
            {
                std::lock_guard<std::mutex> lk(peer.m_mu);
                for (auto it = peer.m_incoming.begin(); it != peer.m_incoming.end();) {
                    if (now - it->second.m_first_seen >= m_config.dead_timeout) {
                        it = peer.m_incoming.erase(it);
                    } else {
                        ++it;
                    }
                }
                for (auto it = peer.m_completed.begin(); it != peer.m_completed.end();) {
                    if (now - it->second >= m_config.dead_timeout) {
                        it = peer.m_completed.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            if (peer.m_state != LinkState::Established && peer.m_state != LinkState::Online) continue;
            if (now - peer.m_last_recv >= m_config.dead_timeout) {
                peer.m_state = LinkState::Closed;
                {
                    // m_pending is also mutated by handle_ack/handle_report
                    // (receiver) and send_data_packet (encode) under peer.m_mu.
                    std::lock_guard<std::mutex> lk(peer.m_mu);
                    peer.m_pending.clear();
                }
                fire_peer_event(&peer, LinkState::Closed);
                if (peer.m_reconnect) {
                    peer.m_state = LinkState::Handshake;
                    peer.m_last_handshake_sent = now - m_config.retransmit_timeout;
                    peer.m_last_recv = now;
                }
            }
        }
    }

    void process_send_queue() {
        std::map<int, std::deque<std::pair<std::string, Bytes>>> local;
        {
            std::lock_guard<std::mutex> lock(m_send_mutex);
            local.swap(m_send_queue);
        }
        for (auto it = local.rbegin(); it != local.rend(); ++it) {
            auto& queue = it->second;
            while (!queue.empty()) {
                auto item = std::move(queue.front());
                queue.pop_front();

                std::string peer_id = item.first;
                Bytes payload = std::move(item.second);
                auto payload_ptr = std::make_shared<Bytes>(std::move(payload));

                {
                    std::lock_guard<std::mutex> lock(m_peers_mutex);
                    auto pit = m_peers.find(peer_id);
                    if (pit == m_peers.end()) {
                        pit = std::find_if(m_peers.begin(), m_peers.end(), [&](const auto& entry) {
                            return entry.second.m_id == peer_id;
                        });
                    }
                    if (pit == m_peers.end()) continue;
                    auto& peer = pit->second;
                    // NOTE: do not log here. When a peer is not Online the
                    // message is re-queued and retried every reactor tick, so
                    // a log line here fires thousands of times while holding
                    // m_peers_mutex -- starving the receiver thread (which
                    // also needs m_peers_mutex) and causing UDP receive-buffer
                    // loss.
                    if (peer.m_state != LinkState::Online) {
                        if (peer.m_state != LinkState::Closed) {
                            std::lock_guard<std::mutex> lk(m_send_mutex);
                            std::size_t total = 0;
                            for (const auto& kv : m_send_queue) total += kv.second.size();
                            if (total < m_config.queue_limit) {
                                m_send_queue[it->first].push_back({peer_id, *payload_ptr});
                            }
                        }
                        continue;
                    }

                    EncodeTask task{&peer, std::move(*payload_ptr)};
                    if (!m_encode_queue.try_push(std::move(task))) {
                        std::lock_guard<std::mutex> lk(m_send_mutex);
                        std::size_t total = 0;
                        for (const auto& kv : m_send_queue) total += kv.second.size();
                        if (total < m_config.queue_limit) {
                            m_send_queue[0].push_back({peer_id, *payload_ptr});
                        }
                    }
                }
            }
        }
    }

    void encode_loop() {
        while (m_running.load(std::memory_order_acquire)) {
            auto task = m_encode_queue.take_for(m_config.flush_interval);
            if (!task) continue;
            try {
                fragment_and_send(task->m_peer, std::move(task->m_payload));
            } catch (...) {}
        }
        while (true) {
            auto task = m_encode_queue.poll();
            if (!task) break;
            try {
                fragment_and_send(task->m_peer, std::move(task->m_payload));
            } catch (...) {}
        }
    }

    static constexpr double kFecSafetyCoefficient = 1.2;

    uint16_t compute_parity_count(Peer* peer, std::size_t data_count) {
        if (peer->m_delay_entropy <= 0.0001) return 1;
        double redundancy = peer->m_delay_entropy * kFecSafetyCoefficient;
        std::uint16_t p = static_cast<std::uint16_t>(std::ceil(static_cast<double>(data_count) * redundancy));
        if (p < 1) p = 1;
        if (p > 3) p = 3;
        return p;
    }

    void fragment_and_send(Peer* peer, Bytes payload) {
        std::size_t frag_payload = m_config.mtu > kHeaderSize ? m_config.mtu - kHeaderSize : 64;
        if (frag_payload <= 4) frag_payload = 64;
        std::uint32_t msg_id;
        double delay_entropy;
        {
            std::lock_guard<std::mutex> lock(peer->m_mu);
            do {
                msg_id = static_cast<std::uint32_t>((peer->m_sequence_out++) & 0x7FFFFFFFu);
            } while (msg_id == 0);
            delay_entropy = peer->m_delay_entropy;
        }
        std::size_t total = payload.size();
        std::uint32_t total_be = to_be32(static_cast<std::uint32_t>(total & 0xFFFFFFFFULL));
        Bytes size_prefix(4);
        std::memcpy(size_prefix.data(), &total_be, 4);
        Bytes full;
        full.reserve(4 + payload.size());
        full.insert(full.end(), size_prefix.begin(), size_prefix.end());
        full.insert(full.end(), payload.begin(), payload.end());
        std::size_t real_total = full.size();
        std::size_t data_count = (real_total + frag_payload - 1) / frag_payload;
        if (data_count == 0) data_count = 1;
        std::size_t width = frag_payload;
        std::vector<Bytes> frags(data_count);
        std::vector<std::uint16_t> frag_lens(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            std::size_t off = i * frag_payload;
            std::size_t len = std::min(frag_payload, real_total - off);
            frags[i].assign(full.begin() + off, full.begin() + off + len);
            frag_lens[i] = static_cast<std::uint16_t>(len);
            if (frags[i].size() < width) frags[i].resize(width, 0);
        }
        Bytes parity = ReedSolomon::parity(frags, width);
        std::uint16_t parity_count = compute_parity_count_for(delay_entropy, data_count);
        std::vector<Bytes> parities;
        parities.push_back(parity);
        for (std::uint16_t p = 1; p < parity_count; ++p) {
            std::vector<Bytes> rotated(frags.size());
            for (std::size_t i = 0; i < frags.size(); ++i) {
                rotated[i] = frags[(i + p) % frags.size()];
            }
            parities.push_back(ReedSolomon::parity(rotated, width));
        }
        std::uint16_t cnt = static_cast<std::uint16_t>(data_count + parity_count);
        std::uint16_t d_cnt = static_cast<std::uint16_t>(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            send_data_packet(peer, msg_id, static_cast<std::uint16_t>(i), cnt, d_cnt, frag_lens[i], frags[i], true);
        }
        for (std::uint16_t p = 0; p < parity_count; ++p) {
            send_data_packet(peer, msg_id, static_cast<std::uint16_t>(data_count + p), cnt, d_cnt, static_cast<std::uint16_t>(width),
                             parities[p], true);
        }
    }

    static std::uint16_t compute_parity_count_for(double delay_entropy, std::size_t data_count) {
        if (delay_entropy <= 0.0001) return 1;
        double redundancy = delay_entropy * kFecSafetyCoefficient;
        std::uint16_t p = static_cast<std::uint16_t>(std::ceil(static_cast<double>(data_count) * redundancy));
        if (p < 1) p = 1;
        if (p > 3) p = 3;
        return p;
    }

    void send_byes() {
        std::lock_guard<std::mutex> lock(m_peers_mutex);
        if (m_sock == kInvalidSocket) return;
        for (auto& kv : m_peers) {
            auto& peer = kv.second;
            if (peer.m_state == LinkState::Closed) continue;
            if (!peer.m_addr_set) continue;
            PacketHeader header{};
            header.magic = kMagic;
            header.version = kVersion;
            header.type = PacketType::Bye;
            header.client_id = m_local_client_id;
            header.session_id = m_local_session_id;
            Bytes datagram = PacketCodec::encode(header, Bytes{});
            creek_sendto(m_sock, reinterpret_cast<const char*>(datagram.data()),
                         static_cast<int>(datagram.size()), 0,
                         reinterpret_cast<const sockaddr*>(&peer.m_addr),
                         static_cast<int>(sizeof(peer.m_addr)));
            peer.m_state = LinkState::Closed;
        }
    }
};

TightTransport::TightTransport(TightConfig config)
    : m_impl(std::make_unique<Impl>(std::move(config))) {}

TightTransport::~TightTransport() {
    if (m_impl) {
        m_impl->send_byes();
        m_impl->stop();
    }
}

void TightTransport::set_message_callback(MessageCallback callback) {
    m_impl->set_message_callback(std::move(callback));
}

void TightTransport::set_peer_callback(PeerCallback callback) {
    m_impl->set_peer_callback(std::move(callback));
}

bool TightTransport::start() {
    return m_impl->start();
}

void TightTransport::stop() {
    m_impl->send_byes();
    m_impl->stop();
}

bool TightTransport::connect(const RemotePeer& remote) {
    return m_impl->connect(remote);
}

bool TightTransport::send(const std::string& peer_id, Bytes payload) {
    return m_impl->send_message(peer_id, std::move(payload), 0);
}

bool TightTransport::send_priority(const std::string& peer_id, Bytes payload, int priority) {
    return m_impl->send_message(peer_id, std::move(payload), priority);
}

std::vector<PeerEvent> TightTransport::peers() const {
    return m_impl->peers_snapshot();
}

std::uint16_t TightTransport::local_port() const {
    return m_impl->m_local_port;
}

}
