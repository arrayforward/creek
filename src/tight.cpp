#include "creek/tight.hpp"
#include "creek/types.hpp"

#include <algorithm>
#include <array>
#include <atomic>
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
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
typedef int socklen_t;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#endif

namespace creek {

namespace {

constexpr std::uint32_t kMagic = 0x54474854U;
constexpr std::uint8_t kVersion = 1;
constexpr std::size_t kHeaderSize = 48;

inline std::uint16_t to_be16(std::uint16_t v) {
    return static_cast<std::uint16_t>(((v & 0x00FFU) << 8) | ((v & 0xFF00U) >> 8));
}
inline std::uint32_t to_be32(std::uint32_t v) {
    return ((v & 0x000000FFU) << 24) | ((v & 0x0000FF00U) << 8)
         | ((v & 0x00FF0000U) >> 8)  | ((v & 0xFF000000U) >> 24);
}
inline std::uint64_t to_be64(std::uint64_t v) {
    return (static_cast<std::uint64_t>(to_be32(static_cast<std::uint32_t>((v >> 32) & 0xFFFFFFFFULL))) << 32)
         | to_be32(static_cast<std::uint32_t>(v & 0xFFFFFFFFULL));
}

std::once_flag g_crc_table_once;
std::array<std::uint32_t, 256> g_crc_table{};

void init_crc_table() {
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1U) ? (0xEDB88320U ^ (c >> 1)) : (c >> 1);
        }
        g_crc_table[i] = c;
    }
}

std::uint32_t crc32_compute(const std::uint8_t* data, std::size_t size) {
    std::call_once(g_crc_table_once, init_crc_table);
    std::uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < size; ++i) {
        crc = g_crc_table[(crc ^ data[i]) & 0xFFU] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFU;
}

struct WsaRef {
    std::mutex mu;
    int refcount{0};
    bool ok{false};
};

WsaRef& wsa_ref() {
    static WsaRef r;
    return r;
}

bool wsa_acquire() {
    auto& r = wsa_ref();
    std::lock_guard<std::mutex> lock(r.mu);
    if (!r.ok) {
        WSADATA wsa;
        r.ok = (WSAStartup(MAKEWORD(2, 2), &wsa) == 0);
    }
    if (r.ok) ++r.refcount;
    return r.ok;
}

void wsa_release() {
    auto& r = wsa_ref();
    std::lock_guard<std::mutex> lock(r.mu);
    if (r.refcount > 0) {
        --r.refcount;
        if (r.refcount == 0 && r.ok) {
            WSACleanup();
            r.ok = false;
        }
    }
}

bool resolve_address(const std::string& host, std::uint16_t port, sockaddr_in& out) {
    std::memset(&out, 0, sizeof(out));
    out.sin_family = AF_INET;
    out.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") {
        out.sin_addr.s_addr = htonl(INADDR_ANY);
        return true;
    }
    if (host == "127.0.0.1" || host == "localhost") {
        out.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        return true;
    }
    in_addr addr;
    if (inet_pton(AF_INET, host.c_str(), &addr) == 1) {
        out.sin_addr = addr;
        return true;
    }
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    addrinfo* res = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) {
        return false;
    }
    bool ok = false;
    for (addrinfo* p = res; p; p = p->ai_next) {
        if (p->ai_family == AF_INET) {
            auto* sin = reinterpret_cast<sockaddr_in*>(p->ai_addr);
            out.sin_addr = sin->sin_addr;
            ok = true;
            break;
        }
    }
    freeaddrinfo(res);
    return ok;
}

#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;
inline void close_socket(NativeSocket s) { closesocket(s); }
inline int last_socket_error() { return WSAGetLastError(); }
inline bool would_block(int e) { return e == WSAEWOULDBLOCK; }
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidSocket = -1;
inline void close_socket(NativeSocket s) { ::close(s); }
inline int last_socket_error() { return errno; }
inline bool would_block(int e) { return e == EWOULDBLOCK || e == EAGAIN; }
#endif

}

Bytes PacketCodec::encode(const PacketHeader& header, const Bytes& payload) {
    Bytes buf(kHeaderSize + payload.size());
    std::uint8_t* p = buf.data();

    auto put32 = [&](std::size_t off, std::uint32_t v) {
        std::uint32_t be = to_be32(v);
        std::memcpy(p + off, &be, 4);
    };
    auto put16 = [&](std::size_t off, std::uint16_t v) {
        std::uint16_t be = to_be16(v);
        std::memcpy(p + off, &be, 2);
    };
    auto put64 = [&](std::size_t off, std::uint64_t v) {
        std::uint64_t be = to_be64(v);
        std::memcpy(p + off, &be, 8);
    };

    put32(0, header.magic);
    p[4] = header.version;
    p[5] = static_cast<std::uint8_t>(header.type);
    put16(6, header.flags);
    put32(8, header.client_id);
    put64(12, header.session_id);
    put32(20, header.sequence);
    put32(24, header.acknowledgment);
    put32(28, header.message_id);
    put16(32, header.fragment_index);
    put16(34, header.fragment_count);
    put16(36, static_cast<std::uint16_t>(payload.size()));
    put16(38, header.reserved);
    put32(40, header.tick);
    put32(44, 0);

    if (!payload.empty()) {
        std::memcpy(p + kHeaderSize, payload.data(), payload.size());
    }

    std::uint32_t crc = crc32_compute(buf.data(), buf.size());
    put32(44, crc);
    return buf;
}

bool PacketCodec::decode(const Bytes& datagram, PacketHeader& header, Bytes& payload) {
    if (datagram.size() < kHeaderSize) return false;
    const std::uint8_t* p = datagram.data();

    auto get32 = [&](std::size_t off) {
        std::uint32_t v;
        std::memcpy(&v, p + off, 4);
        return to_be32(v);
    };
    auto get16 = [&](std::size_t off) {
        std::uint16_t v;
        std::memcpy(&v, p + off, 2);
        return to_be16(v);
    };
    auto get64 = [&](std::size_t off) {
        std::uint32_t lo = get32(off);
        std::uint32_t hi = get32(off + 4);
        return (static_cast<std::uint64_t>(hi) << 32) | lo;
    };

    std::uint32_t magic = get32(0);
    if (magic != kMagic) return false;
    std::uint8_t version = p[4];
    if (version != kVersion) return false;

    header.magic = magic;
    header.version = version;
    header.type = static_cast<PacketType>(p[5]);
    header.flags = get16(6);
    header.client_id = get32(8);
    header.session_id = get64(12);
    header.sequence = get32(20);
    header.acknowledgment = get32(24);
    header.message_id = get32(28);
    header.fragment_index = get16(32);
    header.fragment_count = get16(34);
    std::uint16_t payload_size = get16(36);
    header.payload_size = payload_size;
    header.reserved = get16(38);
    header.tick = get32(40);
    header.checksum = get32(44);

    if (datagram.size() < kHeaderSize + payload_size) return false;

    Bytes tmp(kHeaderSize + payload_size);
    std::memcpy(tmp.data(), p, kHeaderSize);
    if (payload_size > 0) {
        std::memcpy(tmp.data() + kHeaderSize, p + kHeaderSize, payload_size);
    }
    std::uint32_t zero = 0;
    std::memcpy(tmp.data() + 44, &zero, 4);
    std::uint32_t calc = crc32_compute(tmp.data(), tmp.size());
    if (calc != header.checksum) return false;

    payload.assign(p + kHeaderSize, p + kHeaderSize + payload_size);
    return true;
}

std::uint32_t PacketCodec::crc32(const std::uint8_t* data, std::size_t size) {
    return crc32_compute(data, size);
}

Bytes ReedSolomon::parity(const std::vector<Bytes>& fragments, std::size_t width) {
    Bytes p(width, 0);
    for (const auto& f : fragments) {
        std::size_t n = std::min(f.size(), width);
        for (std::size_t i = 0; i < n; ++i) p[i] ^= f[i];
    }
    return p;
}

bool ReedSolomon::recover_one(std::vector<Bytes>& fragments, const Bytes& parity,
                              std::size_t missing_index, std::size_t width) {
    if (missing_index >= fragments.size()) return false;
    if (parity.size() < width) return false;
    Bytes rec(width, 0);
    for (std::size_t i = 0; i < fragments.size(); ++i) {
        if (i == missing_index) continue;
        const auto& f = fragments[i];
        std::size_t n = std::min(f.size(), width);
        for (std::size_t j = 0; j < n; ++j) rec[j] ^= f[j];
    }
    for (std::size_t j = 0; j < width; ++j) rec[j] ^= parity[j];
    fragments[missing_index] = std::move(rec);
    return true;
}

BandwidthEstimator::BandwidthEstimator(std::uint64_t initial_bytes_per_second)
    : bandwidth_(initial_bytes_per_second == 0 ? 1 : initial_bytes_per_second) {
}

void BandwidthEstimator::on_ack(std::size_t bytes, std::chrono::microseconds rtt) {
    auto rtt_count = rtt.count();
    if (rtt_count <= 0) return;
    if (bytes == 0) {
        if (rtt_.count() == 0) rtt_ = rtt;
        else rtt_ = std::chrono::microseconds((rtt_.count() * 7 + rtt_count) / 8);
        return;
    }
    std::uint64_t sample = (static_cast<std::uint64_t>(bytes) * 1000000ULL)
                         / static_cast<std::uint64_t>(rtt_count);
    if (sample == 0) sample = 1;
    if (bandwidth_ == 0) bandwidth_ = sample;
    else bandwidth_ = std::max<std::uint64_t>(1ULL, (bandwidth_ * 7ULL + sample) / 8ULL);

    if (rtt_.count() == 0) rtt_ = rtt;
    else rtt_ = std::chrono::microseconds((rtt_.count() * 7 + rtt_count) / 8);
}

std::uint64_t BandwidthEstimator::bytes_per_second() const {
    return bandwidth_;
}

std::chrono::microseconds BandwidthEstimator::rtt() const {
    return rtt_;
}

class TightTransport::Impl {
public:
    TightConfig config;
    NativeSocket sock{kInvalidSocket};
    std::uint16_t local_port{};
    std::atomic<bool> running{false};
    std::thread reactor_thread;

    mutable std::mutex send_mutex;
    std::condition_variable send_cv;
    std::map<int, std::deque<std::pair<std::string, Bytes>>> send_queue;

    mutable std::mutex peers_mutex;

    struct PendingSend {
        PacketHeader header{};
        Bytes payload;
        std::chrono::steady_clock::time_point last_send;
        std::size_t bytes{0};
        std::uint32_t retries{0};
    };

    struct IncomingMessage {
        std::uint32_t message_id{};
        std::uint16_t total_count{};
        std::vector<std::optional<Bytes>> fragments;
        std::vector<std::uint16_t> sizes;
        std::chrono::steady_clock::time_point first_seen;
    };

    struct Peer {
        std::string id;
        sockaddr_in addr{};
        bool addr_set{false};
        LinkRole role{LinkRole::Leaf};
        LinkState state{LinkState::Closed};
        std::uint32_t peer_client_id{};
        std::uint64_t peer_session_id{};
        std::chrono::steady_clock::time_point last_recv;
        std::chrono::steady_clock::time_point last_heartbeat_sent;
        std::chrono::steady_clock::time_point last_handshake_sent;
        std::uint32_t sequence_out{1};
        std::uint32_t highest_recv_seq{};
        std::map<std::uint32_t, PendingSend> pending;
        std::map<std::uint32_t, IncomingMessage> incoming;
        std::map<std::uint32_t, std::chrono::steady_clock::time_point> completed;
        bool reconnect{};
    };

    std::map<std::string, Peer> peers;

    struct AddrKey {
        std::uint32_t addr;
        std::uint16_t port;
        bool operator<(const AddrKey& o) const {
            if (addr != o.addr) return addr < o.addr;
            return port < o.port;
        }
    };
    std::map<AddrKey, std::string> peer_by_addr;

    std::uint32_t local_client_id{};
    std::uint64_t local_session_id{};

    double token_bucket{0};
    std::chrono::steady_clock::time_point token_bucket_time;

    mutable std::mutex callback_mutex;
    TightTransport::MessageCallback message_cb;
    TightTransport::PeerCallback peer_cb;

    BandwidthEstimator bandwidth;

    std::mt19937_64 rng;
    std::mutex rng_mutex;

    Impl(TightConfig cfg)
        : config(std::move(cfg)),
          bandwidth(config.initial_bandwidth_bytes) {
        local_client_id = static_cast<std::uint32_t>(random_u64() & 0x7FFFFFFFu);
        local_session_id = random_u64();
        if (local_client_id == 0) local_client_id = 1;
        token_bucket_time = std::chrono::steady_clock::now();
    }

    std::uint64_t random_u64() {
        std::lock_guard<std::mutex> lock(rng_mutex);
        if (rng() == 0 && (rng() == 0)) {
            std::random_device rd;
            return (static_cast<std::uint64_t>(rd()) << 32) | rd();
        }
        std::uint64_t a = rng();
        std::uint64_t b = rng();
        return a ^ (b << 1);
    }

    void set_message_callback(TightTransport::MessageCallback cb) {
        std::lock_guard<std::mutex> lock(callback_mutex);
        message_cb = std::move(cb);
    }

    void set_peer_callback(TightTransport::PeerCallback cb) {
        std::lock_guard<std::mutex> lock(callback_mutex);
        peer_cb = std::move(cb);
    }

    void fire_peer_event(Peer* peer, LinkState new_state) {
        TightTransport::PeerCallback cb_copy;
        {
            std::lock_guard<std::mutex> lock(callback_mutex);
            cb_copy = peer_cb;
        }
        if (!cb_copy) return;
        PeerEvent ev{};
        ev.id = peer->id;
        ev.role = peer->role;
        ev.state = new_state;
        ev.client_id = peer->peer_client_id;
        cb_copy(ev);
    }

    void deliver_message(Peer* peer, Bytes payload) {
        TightTransport::MessageCallback cb_copy;
        {
            std::lock_guard<std::mutex> lock(callback_mutex);
            cb_copy = message_cb;
        }
        if (!cb_copy) return;
        cb_copy(peer->id, std::move(payload));
    }

    bool start() {
        if (running.load()) return true;
        if (!wsa_acquire()) return false;

        sock = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
        if (sock == kInvalidSocket) {
            wsa_release();
            return false;
        }

        sockaddr_in local{};
        if (!resolve_address(config.bind.host, config.bind.port, local)) {
            close_socket(sock);
            sock = kInvalidSocket;
            wsa_release();
            return false;
        }
        if (::bind(sock, reinterpret_cast<sockaddr*>(&local), sizeof(local)) != 0) {
            close_socket(sock);
            sock = kInvalidSocket;
            wsa_release();
            return false;
        }

        int bufsize = 512 * 1024;
        ::setsockopt(sock, SOL_SOCKET, SO_RCVBUF,
                     reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));
        ::setsockopt(sock, SOL_SOCKET, SO_SNDBUF,
                     reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));

        sockaddr_in bound{};
        socklen_t blen = sizeof(bound);
        if (::getsockname(sock, reinterpret_cast<sockaddr*>(&bound), &blen) == 0) {
            local_port = ntohs(bound.sin_port);
        }

        u_long nonblock = 1;
#ifdef _WIN32
        ioctlsocket(sock, FIONBIO, &nonblock);
#else
        int fl = fcntl(sock, F_GETFL, 0);
        if (fl >= 0) fcntl(sock, F_SETFL, fl | O_NONBLOCK);
#endif

        running.store(true);
        reactor_thread = std::thread([this] { run(); });
        return true;
    }

    void stop() {
        if (!running.exchange(false)) {
            return;
        }
        send_cv.notify_all();
        if (reactor_thread.joinable()) {
            try { reactor_thread.join(); } catch (...) {}
        }
        if (sock != kInvalidSocket) {
            close_socket(sock);
            sock = kInvalidSocket;
        }
        wsa_release();
    }

    bool connect(const RemotePeer& remote) {
        if (!running.load()) return false;
        sockaddr_in addr{};
        if (!resolve_address(remote.address.host, remote.address.port, addr)) return false;
        std::lock_guard<std::mutex> lock(peers_mutex);
        AddrKey key{addr.sin_addr.s_addr, addr.sin_port};
        auto addr_it = peer_by_addr.find(key);
        if (addr_it != peer_by_addr.end() && addr_it->second != remote.id) {
            auto existing = peers.find(addr_it->second);
            if (existing != peers.end()) {
                auto target = peers.find(remote.id);
                if (target != peers.end()) {
                    target->second = std::move(existing->second);
                    target->second.id = remote.id;
                    peers.erase(existing);
                } else {
                    auto node = peers.extract(existing);
                    node.key() = remote.id;
                    node.mapped().id = remote.id;
                    peers.insert(std::move(node));
                }
            }
            addr_it->second = remote.id;
        }
        auto& peer = peers[remote.id];
        peer.id = remote.id;
        peer.addr = addr;
        peer.addr_set = true;
        peer.role = LinkRole::Node;
        peer.reconnect = true;
        peer_by_addr[key] = remote.id;
        if (peer.state == LinkState::Closed) {
            peer.state = LinkState::Handshake;
            peer.last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
        }
        send_handshake(&peer);
        return true;
    }

    bool send_message(const std::string& peer_id, Bytes payload, int priority = 0) {
        if (!running.load()) return false;
        {
            std::lock_guard<std::mutex> lock(send_mutex);
            std::size_t total = 0;
            for (const auto& kv : send_queue) total += kv.second.size();
            if (total >= config.queue_limit) return false;
            send_queue[priority].emplace_back(peer_id, std::move(payload));
        }
        send_cv.notify_one();
        return true;
    }

    std::vector<PeerEvent> peers_snapshot() const {
        std::vector<PeerEvent> out;
        std::lock_guard<std::mutex> lock(peers_mutex);
        out.reserve(peers.size());
        for (const auto& kv : peers) {
            PeerEvent ev{};
            ev.id = kv.second.id;
            ev.role = kv.second.role;
            ev.state = kv.second.state;
            ev.client_id = kv.second.peer_client_id;
            out.push_back(std::move(ev));
        }
        return out;
    }

    void run() {
        while (running.load(std::memory_order_acquire)) {
            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(sock, &read_fds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = static_cast<long>(std::chrono::duration_cast<std::chrono::microseconds>(
                config.flush_interval).count());
            if (tv.tv_usec <= 0) tv.tv_usec = 10000;
            int ready = ::select(0, &read_fds, nullptr, nullptr, &tv);
            if (ready > 0 && FD_ISSET(sock, &read_fds)) {
                receive_datagrams();
            }
            refill_token_bucket();
            send_handshakes();
            send_heartbeats();
            check_offline();
            retransmit_pending();
            process_send_queue();
        }
    }

    void refill_token_bucket() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
            now - token_bucket_time).count();
        if (elapsed_us <= 0) return;
        std::uint64_t bps = bandwidth.bytes_per_second();
        double tokens = static_cast<double>(bps) * static_cast<double>(elapsed_us) / 1000000.0;
        token_bucket += tokens;
        double cap = static_cast<double>(config.mtu) * 4.0;
        if (token_bucket > cap) token_bucket = cap;
        token_bucket_time = now;
    }

    void receive_datagrams() {
        std::uint8_t buf[2048];
        while (running.load()) {
            sockaddr_in from{};
            socklen_t flen = sizeof(from);
            int n = ::recvfrom(sock, reinterpret_cast<char*>(buf), sizeof(buf), 0,
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
        std::lock_guard<std::mutex> lock(peers_mutex);
        AddrKey key{from.sin_addr.s_addr, from.sin_port};
        auto it = peer_by_addr.find(key);
        if (it != peer_by_addr.end()) {
            auto pit = peers.find(it->second);
            if (pit != peers.end()) return &pit->second;
        }
        std::string id = "anon-" + std::to_string(static_cast<unsigned>(ntohs(from.sin_port))) +
                         "-" + std::to_string(random_u64() & 0xFFFFFFFFu);
        auto& peer = peers[id];
        peer.id = id;
        peer.addr = from;
        peer.addr_set = true;
        peer.role = LinkRole::Leaf;
        peer.state = LinkState::Handshake;
        peer.last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
        peer_by_addr[key] = id;
        return &peer;
    }

    void handle_packet(const sockaddr_in& from, const PacketHeader& header, const Bytes& payload) {
        Peer* peer;
        {
            std::lock_guard<std::mutex> lock(peers_mutex);
            AddrKey key{from.sin_addr.s_addr, from.sin_port};
            auto it = peer_by_addr.find(key);
            if (it == peer_by_addr.end()) {
                std::string id = "anon-" + std::to_string(static_cast<unsigned>(ntohs(from.sin_port))) +
                                 "-" + std::to_string(random_u64() & 0xFFFFFFFFu);
                auto& np = peers[id];
                np.id = id;
                np.addr = from;
                np.addr_set = true;
                np.role = LinkRole::Leaf;
                np.state = LinkState::Handshake;
                np.last_handshake_sent = std::chrono::steady_clock::now() - std::chrono::hours(1);
                peer_by_addr[key] = id;
                peer = &np;
            } else {
                auto pit = peers.find(it->second);
                if (pit == peers.end()) return;
                peer = &pit->second;
            }
        }

        peer->last_recv = std::chrono::steady_clock::now();
        if (peer->peer_client_id == 0 && header.client_id != 0) {
            peer->peer_client_id = header.client_id;
        }
        if (peer->peer_session_id == 0 && header.session_id != 0) {
            peer->peer_session_id = header.session_id;
        }

        std::uint32_t ack_to_send = header.sequence;
        bool need_ack = header.sequence != 0;
        if (header.sequence > peer->highest_recv_seq) {
            peer->highest_recv_seq = header.sequence;
        }

        switch (header.type) {
            case PacketType::Handshake:    handle_handshake(peer, header, payload); break;
            case PacketType::HandshakeAck: handle_handshake_ack(peer, header, payload); break;
            case PacketType::Online:       handle_online(peer, header, payload); break;
            case PacketType::Heartbeat:    break;
            case PacketType::Bye:          handle_bye(peer, header, payload); break;
            case PacketType::Data:         handle_data(peer, header, payload); break;
            case PacketType::Parity:       handle_data(peer, header, payload); break;
            case PacketType::Ack:          handle_ack(peer, header); break;
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
        if (token != config.token) return;
        peer->id = std::move(peer_id);
        peer->role = role_byte == static_cast<std::uint8_t>(LinkRole::Node)
                         ? LinkRole::Node
                         : LinkRole::Leaf;
        if (peer->state != LinkState::Established && peer->state != LinkState::Online) {
            peer->state = LinkState::Established;
            fire_peer_event(peer, LinkState::Established);
        }
        send_control(peer, PacketType::HandshakeAck, Bytes{}, true);
    }

    void handle_handshake_ack(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->state == LinkState::Online) return;
        if (peer->state == LinkState::Handshake || peer->state == LinkState::Established) {
            send_control(peer, PacketType::Online, Bytes{}, true);
            peer->state = LinkState::Online;
            fire_peer_event(peer, LinkState::Online);
        }
    }

    void handle_online(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->state != LinkState::Online) {
            peer->state = LinkState::Online;
            fire_peer_event(peer, LinkState::Online);
        }
    }

    void handle_bye(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (peer->state != LinkState::Closed) {
            peer->state = LinkState::Closed;
            fire_peer_event(peer, LinkState::Closed);
        }
    }

    void handle_data(Peer* peer, const PacketHeader& header, const Bytes& payload) {
        if (header.fragment_count == 0) return;
        if (peer->completed.contains(header.message_id)) return;
        std::uint16_t idx = header.fragment_index;
        std::uint16_t cnt = header.fragment_count;
        if (idx >= cnt) return;
        auto& in = peer->incoming[header.message_id];
        if (in.message_id == 0) {
            in.message_id = header.message_id;
            in.total_count = cnt;
            in.fragments.assign(cnt, std::nullopt);
            in.sizes.assign(cnt, 0);
            in.first_seen = std::chrono::steady_clock::now();
        }
        if (in.total_count != cnt) return;
        if (idx >= in.fragments.size()) return;
        if (!in.fragments[idx].has_value()) {
            in.fragments[idx] = payload;
            in.sizes[idx] = header.reserved;
        }
        if (try_assemble(peer, in)) {
            peer->completed[header.message_id] = std::chrono::steady_clock::now();
            peer->incoming.erase(header.message_id);
        }
    }

    bool try_assemble(Peer* peer, IncomingMessage& in) {
        if (in.total_count < 2) return false;
        std::size_t data_count = in.total_count - 1;
        std::size_t have = 0;
        for (std::size_t i = 0; i < data_count; ++i) {
            if (in.fragments[i].has_value()) ++have;
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
                parts.push_back(*in.fragments[i]);
                real_sizes.push_back(in.sizes[i]);
            }
            deliver_message(peer, build_msg(parts, real_sizes));
            return true;
        }
        if (!in.fragments[in.total_count - 1].has_value()) return false;
        std::size_t missing = SIZE_MAX;
        std::size_t multi = 0;
        for (std::size_t i = 0; i < data_count; ++i) {
            if (!in.fragments[i].has_value()) {
                ++multi;
                missing = i;
            }
        }
        if (multi != 1) return false;
        std::size_t width = 0;
        for (auto& f : in.fragments) {
            if (f.has_value()) {
                width = std::max(width, f->size());
            }
        }
        std::vector<Bytes> frags;
        frags.reserve(data_count);
        for (std::size_t i = 0; i < data_count; ++i) {
            if (in.fragments[i].has_value()) frags.push_back(*in.fragments[i]);
            else frags.push_back(Bytes(width, 0));
        }
        Bytes parity = *in.fragments[in.total_count - 1];
        if (!ReedSolomon::recover_one(frags, parity, missing, width)) return false;
        deliver_message(peer, build_msg(frags, in.sizes));
        return true;
    }

    void handle_ack(Peer* peer, const PacketHeader& header) {
        std::uint32_t ack = header.acknowledgment;
        if (ack == 0) return;
        auto it = peer->pending.find(ack);
        if (it == peer->pending.end()) return;
        auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - it->second.last_send);
        if (rtt.count() < 0) rtt = std::chrono::microseconds(0);
        bandwidth.on_ack(it->second.bytes, rtt);
        peer->pending.erase(it);
    }

    void send_control(Peer* peer, PacketType type, const Bytes& payload, bool ackable) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = type;
        header.client_id = local_client_id;
        header.session_id = local_session_id;
        if (ackable) {
            header.sequence = peer->sequence_out++;
        }
        header.payload_size = static_cast<std::uint16_t>(payload.size());
        header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
        Bytes datagram = PacketCodec::encode(header, payload);
        send_raw(peer, datagram);
        if (ackable) {
            auto& ps = peer->pending[header.sequence];
            ps.header = header;
            ps.payload = payload;
            ps.last_send = std::chrono::steady_clock::now();
            ps.bytes = datagram.size();
        }
    }

    void send_ack_packet(Peer* peer, std::uint32_t ack) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = PacketType::Ack;
        header.client_id = local_client_id;
        header.session_id = local_session_id;
        header.acknowledgment = ack;
        header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
        Bytes datagram = PacketCodec::encode(header, Bytes{});
        send_raw(peer, datagram);
    }

    void send_handshake(Peer* peer) {
        Bytes payload;
        payload.push_back(static_cast<std::uint8_t>(config.role));
        auto id_size = static_cast<std::uint16_t>(
            std::min<std::size_t>(config.id.size(), 65535U));
        payload.push_back(static_cast<std::uint8_t>((id_size >> 8U) & 0xFFU));
        payload.push_back(static_cast<std::uint8_t>(id_size & 0xFFU));
        payload.insert(payload.end(), config.id.begin(), config.id.begin() + id_size);
        payload.insert(payload.end(), config.token.begin(), config.token.end());
        peer->last_handshake_sent = std::chrono::steady_clock::now();
        send_control(peer, PacketType::Handshake, payload, true);
    }

    void send_data_packet(Peer* peer, std::uint32_t msg_id, std::uint16_t idx,
                          std::uint16_t cnt, std::uint16_t real_size,
                          const Bytes& payload, bool ackable) {
        PacketHeader header{};
        header.magic = kMagic;
        header.version = kVersion;
        header.type = (idx + 1 == cnt) ? PacketType::Parity : PacketType::Data;
        header.client_id = local_client_id;
        header.session_id = local_session_id;
        if (ackable) {
            header.sequence = peer->sequence_out++;
        }
        header.message_id = msg_id;
        header.fragment_index = idx;
        header.fragment_count = cnt;
        header.reserved = real_size;
        header.payload_size = static_cast<std::uint16_t>(payload.size());
        header.tick = static_cast<std::uint32_t>(unix_millis() & 0xFFFFFFFFULL);
        Bytes datagram = PacketCodec::encode(header, payload);
        send_raw(peer, datagram);
        if (ackable) {
            auto& ps = peer->pending[header.sequence];
            ps.header = header;
            ps.payload = payload;
            ps.last_send = std::chrono::steady_clock::now();
            ps.bytes = datagram.size();
        }
    }

    void send_raw(Peer* peer, const Bytes& datagram) {
        if (!peer->addr_set || sock == kInvalidSocket) return;
        double cost = static_cast<double>(datagram.size());
        refill_token_bucket();
        while (running.load() && token_bucket < cost) {
            auto rate = std::max<std::uint64_t>(bandwidth.bytes_per_second(), 1U);
            auto deficit = cost - token_bucket;
            auto wait_us = static_cast<std::uint64_t>(
                std::max(1.0, deficit * 1000000.0 / static_cast<double>(rate)));
            std::this_thread::sleep_for(std::chrono::microseconds(
                std::min<std::uint64_t>(wait_us, 10000U)));
            refill_token_bucket();
        }
        if (token_bucket >= cost) token_bucket -= cost;
        ::sendto(sock, reinterpret_cast<const char*>(datagram.data()),
                 static_cast<int>(datagram.size()), 0,
                 reinterpret_cast<const sockaddr*>(&peer->addr), sizeof(peer->addr));
    }

    void send_handshakes() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        for (auto& kv : peers) {
            auto& peer = kv.second;
            if (peer.state != LinkState::Handshake) continue;
            if (peer.pending.empty()) {
                send_handshake(&peer);
            }
        }
    }

    void send_heartbeats() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : peers) {
            auto& peer = kv.second;
            if (peer.state != LinkState::Established && peer.state != LinkState::Online) continue;
            if (now - peer.last_heartbeat_sent < config.heartbeat) continue;
            send_control(&peer, PacketType::Heartbeat, Bytes{}, true);
            peer.last_heartbeat_sent = now;
        }
    }

    void check_offline() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : peers) {
            auto& peer = kv.second;
            for (auto it = peer.incoming.begin(); it != peer.incoming.end();) {
                if (now - it->second.first_seen >= config.dead_timeout) {
                    it = peer.incoming.erase(it);
                } else {
                    ++it;
                }
            }
            for (auto it = peer.completed.begin(); it != peer.completed.end();) {
                if (now - it->second >= config.dead_timeout) {
                    it = peer.completed.erase(it);
                } else {
                    ++it;
                }
            }
            if (peer.state != LinkState::Established && peer.state != LinkState::Online) continue;
            if (now - peer.last_recv >= config.dead_timeout) {
                peer.state = LinkState::Closed;
                peer.pending.clear();
                fire_peer_event(&peer, LinkState::Closed);
                if (peer.reconnect) {
                    peer.state = LinkState::Handshake;
                    peer.last_handshake_sent = now - config.retransmit_timeout;
                    peer.last_recv = now;
                }
            }
        }
    }

    void retransmit_pending() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        auto now = std::chrono::steady_clock::now();
        for (auto& kv : peers) {
            auto& peer = kv.second;
            bool exhausted = false;
            for (auto& kv2 : peer.pending) {
                if (now - kv2.second.last_send < config.retransmit_timeout) continue;
                if (kv2.second.retries >= 10) {
                    exhausted = true;
                    break;
                }
                Bytes datagram = PacketCodec::encode(kv2.second.header, kv2.second.payload);
                send_raw(&peer, datagram);
                kv2.second.last_send = now;
                ++kv2.second.retries;
            }
            if (!exhausted) continue;
            peer.pending.clear();
            peer.state = LinkState::Closed;
            fire_peer_event(&peer, LinkState::Closed);
            if (peer.reconnect) {
                peer.state = LinkState::Handshake;
                peer.last_handshake_sent = now - config.retransmit_timeout;
                peer.last_recv = now;
            }
        }
    }

    void process_send_queue() {
        std::map<int, std::deque<std::pair<std::string, Bytes>>> local;
        {
            std::lock_guard<std::mutex> lock(send_mutex);
            local.swap(send_queue);
        }
        for (auto it = local.rbegin(); it != local.rend(); ++it) {
            auto& queue = it->second;
            while (!queue.empty()) {
                auto item = std::move(queue.front());
                queue.pop_front();
                std::lock_guard<std::mutex> lock(peers_mutex);
                auto pit = peers.find(item.first);
                if (pit == peers.end()) {
                    pit = std::find_if(peers.begin(), peers.end(), [&](const auto& entry) {
                        return entry.second.id == item.first;
                    });
                }
                if (pit == peers.end()) continue;
                auto& peer = pit->second;
                if (peer.state != LinkState::Online) {
                    if (peer.state != LinkState::Closed) {
                        std::lock_guard<std::mutex> lk(send_mutex);
                        if ([&]() {
                            std::size_t total = 0;
                            for (const auto& kv : send_queue) total += kv.second.size();
                            return total;
                        }() < config.queue_limit) {
                            send_queue[it->first].push_back(std::move(item));
                        }
                    }
                    continue;
                }
                fragment_and_send(&peer, std::move(item.second));
            }
        }
    }

    void fragment_and_send(Peer* peer, Bytes payload) {
        std::size_t frag_payload = config.mtu > kHeaderSize ? config.mtu - kHeaderSize : 64;
        if (frag_payload <= 4) frag_payload = 64;
        std::uint32_t msg_id{};
        do {
            msg_id = static_cast<std::uint32_t>((peer->sequence_out++) & 0x7FFFFFFFu);
        } while (msg_id == 0);
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
        std::uint16_t cnt = static_cast<std::uint16_t>(data_count + 1);
        for (std::size_t i = 0; i < data_count; ++i) {
            send_data_packet(peer, msg_id, static_cast<std::uint16_t>(i), cnt, frag_lens[i], frags[i], true);
        }
        send_data_packet(peer, msg_id, static_cast<std::uint16_t>(data_count), cnt, static_cast<std::uint16_t>(width),
                         parity, true);
    }

    void send_byes() {
        std::lock_guard<std::mutex> lock(peers_mutex);
        if (sock == kInvalidSocket) return;
        for (auto& kv : peers) {
            auto& peer = kv.second;
            if (peer.state == LinkState::Closed) continue;
            if (!peer.addr_set) continue;
            PacketHeader header{};
            header.magic = kMagic;
            header.version = kVersion;
            header.type = PacketType::Bye;
            header.client_id = local_client_id;
            header.session_id = local_session_id;
            Bytes datagram = PacketCodec::encode(header, Bytes{});
            ::sendto(sock, reinterpret_cast<const char*>(datagram.data()),
                     static_cast<int>(datagram.size()), 0,
                     reinterpret_cast<const sockaddr*>(&peer.addr), sizeof(peer.addr));
            peer.state = LinkState::Closed;
        }
    }
};

TightTransport::TightTransport(TightConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

TightTransport::~TightTransport() {
    if (impl_) {
        impl_->send_byes();
        impl_->stop();
    }
}

void TightTransport::set_message_callback(MessageCallback callback) {
    impl_->set_message_callback(std::move(callback));
}

void TightTransport::set_peer_callback(PeerCallback callback) {
    impl_->set_peer_callback(std::move(callback));
}

bool TightTransport::start() {
    return impl_->start();
}

void TightTransport::stop() {
    impl_->send_byes();
    impl_->stop();
}

bool TightTransport::connect(const RemotePeer& remote) {
    return impl_->connect(remote);
}

bool TightTransport::send(const std::string& peer_id, Bytes payload) {
    return impl_->send_message(peer_id, std::move(payload), 0);
}

bool TightTransport::send_priority(const std::string& peer_id, Bytes payload, int priority) {
    return impl_->send_message(peer_id, std::move(payload), priority);
}

std::vector<PeerEvent> TightTransport::peers() const {
    return impl_->peers_snapshot();
}

std::uint16_t TightTransport::local_port() const {
    return impl_->local_port;
}

}