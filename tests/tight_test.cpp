#include "creek/tight.hpp"
#include "creek/types.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace creek;
using namespace std::chrono_literals;

namespace {

int failures = 0;
int tests = 0;

#define CHECK(cond) do { \
    ++tests; \
    if (!(cond)) { \
        std::cerr << "FAIL [" << __FILE__ << ":" << __LINE__ << "]: " << #cond << "\n"; \
        ++failures; \
    } \
} while (0)

void test_packet_codec_roundtrip() {
    PacketHeader header{};
    header.magic = 0x54474854U;
    header.version = 1;
    header.type = PacketType::Data;
    header.flags = 0x1234;
    header.client_id = 0xdeadbeefU;
    header.session_id = 0x1122334455667788ULL;
    header.sequence = 42;
    header.acknowledgment = 41;
    header.message_id = 0xcafebabeU;
    header.fragment_index = 2;
    header.fragment_count = 5;
    header.payload_size = 3;
    header.reserved = 0;
    header.tick = 0x12345678U;

    Bytes payload{0xAA, 0xBB, 0xCC};
    Bytes encoded = PacketCodec::encode(header, payload);
    CHECK(encoded.size() == PacketCodec::header_size + payload.size());

    PacketHeader decoded{};
    Bytes decoded_payload;
    CHECK(PacketCodec::decode(encoded, decoded, decoded_payload));
    CHECK(decoded.magic == header.magic);
    CHECK(decoded.version == header.version);
    CHECK(decoded.type == header.type);
    CHECK(decoded.flags == header.flags);
    CHECK(decoded.client_id == header.client_id);
    CHECK(decoded.session_id == header.session_id);
    CHECK(decoded.sequence == header.sequence);
    CHECK(decoded.acknowledgment == header.acknowledgment);
    CHECK(decoded.message_id == header.message_id);
    CHECK(decoded.fragment_index == header.fragment_index);
    CHECK(decoded.fragment_count == header.fragment_count);
    CHECK(decoded.payload_size == header.payload_size);
    CHECK(decoded.tick == header.tick);
    CHECK(decoded.checksum != 0);
    CHECK(decoded_payload == payload);

    Bytes empty_payload;
    Bytes empty_encoded = PacketCodec::encode(header, empty_payload);
    PacketHeader dh{};
    Bytes dp;
    CHECK(PacketCodec::decode(empty_encoded, dh, dp));
    CHECK(dp.empty());

    Bytes short_bytes{1, 2, 3};
    PacketHeader dh2{};
    Bytes dp2;
    CHECK(!PacketCodec::decode(short_bytes, dh2, dp2));

    Bytes big(2048, 0x5A);
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<std::uint8_t>(i & 0xFF);
    PacketHeader bh{};
    bh.type = PacketType::Data;
    bh.message_id = 99;
    bh.fragment_index = 0;
    bh.fragment_count = 1;
    Bytes big_enc = PacketCodec::encode(bh, big);
    PacketHeader bh2{};
    Bytes bp2;
    CHECK(PacketCodec::decode(big_enc, bh2, bp2));
    CHECK(bp2 == big);
}

void test_crc_corruption() {
    PacketHeader header{};
    header.type = PacketType::Data;
    header.client_id = 1;
    header.session_id = 2;
    header.sequence = 7;
    header.message_id = 11;
    header.fragment_index = 0;
    header.fragment_count = 1;
    Bytes payload{'h','e','l','l','o'};

    Bytes encoded = PacketCodec::encode(header, payload);
    PacketHeader decoded{};
    Bytes decoded_payload;
    CHECK(PacketCodec::decode(encoded, decoded, decoded_payload));
    CHECK(decoded_payload == payload);

    Bytes corrupt = encoded;
    corrupt[PacketCodec::header_size + 2] ^= 0xFF;
    PacketHeader dh{};
    Bytes dp;
    CHECK(!PacketCodec::decode(corrupt, dh, dp));

    Bytes corrupt2 = encoded;
    corrupt2[10] ^= 0x01;
    CHECK(!PacketCodec::decode(corrupt2, dh, dp));

    Bytes corrupt3 = encoded;
    corrupt3[44] ^= 0xFF;
    CHECK(!PacketCodec::decode(corrupt3, dh, dp));

    Bytes truncated = encoded;
    truncated.pop_back();
    CHECK(!PacketCodec::decode(truncated, dh, dp));

    Bytes bad_magic = encoded;
    bad_magic[0] = 0;
    CHECK(!PacketCodec::decode(bad_magic, dh, dp));

    Bytes bad_version = encoded;
    bad_version[4] = 99;
    CHECK(!PacketCodec::decode(bad_version, dh, dp));

    std::uint32_t crc_empty = PacketCodec::crc32(nullptr, 0);
    std::uint8_t one = 0xAA;
    std::uint32_t crc_one = PacketCodec::crc32(&one, 1);
    CHECK(crc_empty != crc_one);
}

void test_fec_recovery() {
    std::vector<Bytes> fragments;
    fragments.push_back(Bytes{1, 2, 3, 4});
    fragments.push_back(Bytes{5, 6, 7, 8});
    fragments.push_back(Bytes{9, 10, 11, 12});
    fragments.push_back(Bytes{13, 14, 15, 16});

    Bytes parity = ReedSolomon::parity(fragments, 4);
    Bytes expected{static_cast<std::uint8_t>(1 ^ 5 ^ 9 ^ 13),
                   static_cast<std::uint8_t>(0x02 ^ 0x06 ^ 0x0A ^ 0x0E),
                   static_cast<std::uint8_t>(0x03 ^ 0x07 ^ 0x0B ^ 0x0F),
                   static_cast<std::uint8_t>(0x04 ^ 0x08 ^ 0x0C ^ 0x10)};
    CHECK(parity == expected);

    std::vector<Bytes> lost = fragments;
    lost[2].clear();
    CHECK(ReedSolomon::recover_one(lost, parity, 2, 4));
    CHECK(lost[2] == Bytes({9, 10, 11, 12}));

    std::vector<Bytes> lost0 = fragments;
    lost0[0].clear();
    CHECK(ReedSolomon::recover_one(lost0, parity, 0, 4));
    CHECK(lost0[0] == Bytes({1, 2, 3, 4}));

    std::vector<Bytes> lost3 = fragments;
    lost3[3].clear();
    CHECK(ReedSolomon::recover_one(lost3, parity, 3, 4));
    CHECK(lost3[3] == Bytes({13, 14, 15, 16}));

    std::vector<Bytes> uneven = {
        Bytes{0x01, 0x02, 0x03, 0x04, 0x05},
        Bytes{0x10, 0x20},
        Bytes{0xAA, 0xBB, 0xCC, 0xDD}
    };
    Bytes p2 = ReedSolomon::parity(uneven, 5);
    CHECK(p2.size() == 5);
    std::vector<Bytes> rec = uneven;
    rec[1].clear();
    CHECK(ReedSolomon::recover_one(rec, p2, 1, 5));
    CHECK(rec[1].size() == 5);
}

void test_bandwidth_estimator() {
    BandwidthEstimator bw(1000000);
    CHECK(bw.bytes_per_second() == 1000000);

    bw.on_ack(1000, 1000us);
    auto rtt1 = bw.rtt();
    CHECK(rtt1.count() == 1000);

    bw.on_ack(1000, 1000us);
    auto rtt2 = bw.rtt();
    CHECK(rtt2.count() == 1000);

    bw.on_ack(2000, 1000us);
    auto bps = bw.bytes_per_second();
    CHECK(bps >= 1000000);

    BandwidthEstimator bw2(100000);
    bw2.on_ack(1000, 100us);
    CHECK(bw2.bytes_per_second() > 100000);
    CHECK(bw2.bytes_per_second() < 10000000);

    BandwidthEstimator bw3(0);
    bw3.on_ack(5000, 1000us);
    CHECK(bw3.bytes_per_second() > 0);

    bw3.on_ack(0, 500us);
    auto r = bw3.rtt();
    CHECK(r.count() > 0);

    bw3.on_ack(1000, 0us);
    CHECK(bw3.bytes_per_second() > 0);
}

struct TwoTransportFixture {
    std::unique_ptr<TightTransport> a;
    std::unique_ptr<TightTransport> b;
    std::mutex mu;
    std::condition_variable cv;
    bool a_online{false};
    bool b_online{false};
    bool a_seen_beta{false};
    bool b_seen_alpha{false};
    Bytes received_payload;
    bool message_received{false};
    std::string received_from;

    TwoTransportFixture() {
        TightConfig ca{};
        ca.id = "alpha";
        ca.role = LinkRole::Node;
        ca.bind = Address{"127.0.0.1", 0};
        ca.token = "secret-token";
        ca.heartbeat = 50ms;
        ca.dead_timeout = 1500ms;
        ca.retransmit_timeout = 50ms;
        ca.flush_interval = 10ms;
        ca.mtu = 600;
        ca.initial_bandwidth_bytes = 50ULL * 1024 * 1024;

        a = std::make_unique<TightTransport>(std::move(ca));
        a->set_peer_callback([this](const PeerEvent& e) {
            if (e.state == LinkState::Online) {
                std::lock_guard<std::mutex> lock(mu);
                if (e.id == "beta") {
                    a_online = true;
                    a_seen_beta = true;
                }
            }
            cv.notify_all();
        });
        a->set_message_callback([this](const std::string& id, Bytes payload) {
            std::lock_guard<std::mutex> lock(mu);
            received_payload = std::move(payload);
            received_from = id;
            message_received = true;
            cv.notify_all();
        });

        TightConfig cb{};
        cb.id = "beta";
        cb.role = LinkRole::Leaf;
        cb.bind = Address{"127.0.0.1", 0};
        cb.token = "secret-token";
        cb.heartbeat = 50ms;
        cb.dead_timeout = 1500ms;
        cb.retransmit_timeout = 50ms;
        cb.flush_interval = 10ms;
        cb.mtu = 600;
        cb.initial_bandwidth_bytes = 50ULL * 1024 * 1024;

        b = std::make_unique<TightTransport>(std::move(cb));
        b->set_peer_callback([this](const PeerEvent& e) {
            if (e.state == LinkState::Online) {
                std::lock_guard<std::mutex> lock(mu);
                if (e.id == "alpha") {
                    b_online = true;
                    b_seen_alpha = true;
                }
            }
            cv.notify_all();
        });
        b->set_message_callback([this](const std::string& id, Bytes payload) {
            std::lock_guard<std::mutex> lock(mu);
            received_payload = std::move(payload);
            received_from = id;
            message_received = true;
            cv.notify_all();
        });
    }

    bool wait_online(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu);
        return cv.wait_for(lock, timeout, [this] {
            return a_online && b_online;
        });
    }

    bool wait_message(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu);
        return cv.wait_for(lock, timeout, [this] {
            return message_received;
        });
    }
};

void test_two_transport_handshake_and_big_message() {
    TwoTransportFixture f;
    CHECK(f.a->start());
    CHECK(f.b->start());

    std::uint16_t port_a = f.a->local_port();
    std::uint16_t port_b = f.b->local_port();
    CHECK(port_a > 0);
    CHECK(port_b > 0);
    CHECK(port_a != port_b);

    CHECK(f.a->connect({"beta", {"127.0.0.1", port_b}}));
    CHECK(f.b->connect({"alpha", {"127.0.0.1", port_a}}));

    CHECK(f.wait_online(5000ms));
    CHECK(f.a_seen_beta);
    CHECK(f.b_seen_alpha);

    std::mt19937_64 rng{std::random_device{}()};
    Bytes payload(50000);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::uint8_t>(rng() & 0xFF);
    }

    CHECK(f.a->send("beta", payload));
    CHECK(f.wait_message(5000ms));
    {
        std::lock_guard<std::mutex> lock(f.mu);
        CHECK(f.message_received);
        CHECK(f.received_payload == payload);
        CHECK(f.received_from == "alpha");
    }

    CHECK(f.b->send("alpha", Bytes(12345, 0x5A)));
    std::unique_lock<std::mutex> lock(f.mu);
    bool got_back = f.cv.wait_for(lock, 5000ms, [&f] {
        return !f.received_payload.empty() && f.received_from == "beta" &&
               f.received_payload.size() == 12345;
    });
    lock.unlock();
    {
        std::lock_guard<std::mutex> lk(f.mu);
        CHECK(got_back);
        CHECK(f.received_payload.size() == 12345);
        bool all_match = true;
        for (auto b : f.received_payload) {
            if (b != 0x5A) { all_match = false; break; }
        }
        CHECK(all_match);
    }

    auto pa = f.a->peers();
    auto pb = f.b->peers();
    if (pa.size() != 1 || pb.size() != 1) {
        std::cerr << "peer sizes: a=" << pa.size() << " b=" << pb.size() << '\n';
        for (const auto& peer : pa) {
            std::cerr << "a peer: " << peer.id << " role=" << static_cast<int>(peer.role)
                      << " state=" << static_cast<int>(peer.state) << '\n';
        }
        for (const auto& peer : pb) {
            std::cerr << "b peer: " << peer.id << " role=" << static_cast<int>(peer.role)
                      << " state=" << static_cast<int>(peer.state) << '\n';
        }
    }
    CHECK(pa.size() == 1);
    CHECK(pb.size() == 1);
    CHECK(pa[0].state == LinkState::Online);
    CHECK(pb[0].state == LinkState::Online);

    f.a->stop();
    f.b->stop();
}

void test_token_mismatch_blocks_handshake() {
    TightConfig ca{};
    ca.id = "x";
    ca.bind = Address{"127.0.0.1", 0};
    ca.token = "right";
    ca.heartbeat = 50ms;
    ca.dead_timeout = 1000ms;
    ca.retransmit_timeout = 50ms;
    ca.flush_interval = 10ms;
    ca.mtu = 600;
    ca.initial_bandwidth_bytes = 10ULL * 1024 * 1024;
    auto a = std::make_unique<TightTransport>(std::move(ca));

    TightConfig cb{};
    cb.id = "y";
    cb.bind = Address{"127.0.0.1", 0};
    cb.token = "wrong";
    cb.heartbeat = 50ms;
    cb.dead_timeout = 1000ms;
    cb.retransmit_timeout = 50ms;
    cb.flush_interval = 10ms;
    cb.mtu = 600;
    cb.initial_bandwidth_bytes = 10ULL * 1024 * 1024;
    auto b = std::make_unique<TightTransport>(std::move(cb));

    CHECK(a->start());
    CHECK(b->start());

    std::atomic<int> a_events{0};
    std::atomic<int> b_events{0};
    a->set_peer_callback([&](const PeerEvent&) { ++a_events; });
    b->set_peer_callback([&](const PeerEvent&) { ++b_events; });

    CHECK(a->connect({"y", {"127.0.0.1", b->local_port()}}));
    CHECK(b->connect({"x", {"127.0.0.1", a->local_port()}}));

    std::this_thread::sleep_for(500ms);

    CHECK(a_events.load() == 0);
    CHECK(b_events.load() == 0);

    a->stop();
    b->stop();
}

}

int main() {
    std::cout << "Running tight tests...\n";

    test_packet_codec_roundtrip();
    test_crc_corruption();
    test_fec_recovery();
    test_bandwidth_estimator();
    test_two_transport_handshake_and_big_message();
    test_token_mismatch_blocks_handshake();

    std::cout << "Tests run: " << tests << ", failures: " << failures << "\n";
    return failures == 0 ? 0 : 1;
}