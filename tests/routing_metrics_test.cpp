#include "creek/metrics.hpp"
#include "creek/routing.hpp"
#include "creek/types.hpp"
#include "creek.pb.h"

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

int g_passed = 0;
int g_failed = 0;

#define EXPECT(cond)                                                      \
    do {                                                                  \
        if (!(cond)) {                                                    \
            std::cerr << "FAIL " << __FILE__ << ":" << __LINE__            \
                      << " " << #cond << std::endl;                       \
            ++g_failed;                                                   \
        } else {                                                          \
            ++g_passed;                                                   \
        }                                                                 \
    } while (0)

creek::v1::Endpoint ep(const std::string& id, const std::string& svc,
                       const std::string& target, std::uint64_t version,
                       std::uint64_t updated_ms, bool alive = true) {
    creek::v1::Endpoint e;
    e.set_endpoint_id(id);
    e.set_service(svc);
    e.set_target(target);
    e.set_version(version);
    e.set_updated_ms(updated_ms);
    e.set_alive(alive);
    return e;
}

creek::MetricEvent make_event(const std::string& direction,
                              const std::string& rpc,
                              const std::string& sid_value,
                              std::uint64_t bytes,
                              std::uint64_t latency_us,
                              bool success) {
    creek::MetricEvent ev;
    ev.direction = direction;
    ev.rpc_name = rpc;
    if (!sid_value.empty()) {
        ev.metadata["sid"] = sid_value;
    }
    ev.bytes = bytes;
    ev.latency_us = latency_us;
    ev.success = success;
    return ev;
}

void test_directory_conflict() {
    creek::EndpointDirectory dir;
    EXPECT(dir.version() == 0);

    EXPECT(dir.merge(ep("e1", "svc", "10.0.0.1:9000", 1, 100)));
    EXPECT(dir.version() == 1);

    EXPECT(!dir.merge(ep("e1", "svc", "10.0.0.1:9000", 1, 50)));
    EXPECT(dir.version() == 1);

    EXPECT(dir.merge(ep("e1", "svc", "10.0.0.1:9001", 1, 200)));
    EXPECT(dir.version() == 2);
    {
        auto found = dir.find("e1");
        EXPECT(found.has_value());
        EXPECT(found.has_value() && found->target() == "10.0.0.1:9001");
    }

    EXPECT(dir.merge(ep("e1", "svc", "10.0.0.1:9002", 2, 50)));
    EXPECT(dir.version() == 3);
    {
        auto found = dir.find("e1");
        EXPECT(found.has_value() && found->target() == "10.0.0.1:9002");
    }

    EXPECT(!dir.merge(ep("e1", "svc", "10.0.0.1:9003", 1, 9999)));
    EXPECT(dir.version() == 3);
    {
        auto found = dir.find("e1");
        EXPECT(found.has_value() && found->target() == "10.0.0.1:9002");
    }

    EXPECT(dir.merge(ep("e2", "svc", "10.0.0.2:9000", 1, 110)));
    EXPECT(dir.version() == 4);
    EXPECT(dir.service("svc").size() == 2);
    EXPECT(dir.service("other").empty());
    EXPECT(!dir.find("missing").has_value());

    creek::v1::DirectorySnapshot snap;
    *snap.add_endpoints() = ep("e1", "svc", "10.0.0.1:9002", 2, 50);
    *snap.add_endpoints() = ep("e3", "svc", "10.0.0.3:9000", 5, 900);
    EXPECT(dir.merge(snap));
    EXPECT(dir.version() == 5);
    EXPECT(dir.service("svc").size() == 3);

    auto out = dir.snapshot("node-1");
    EXPECT(out.source_id() == "node-1");
    EXPECT(out.version() == 5);
    EXPECT(out.endpoints_size() == 3);

    creek::v1::Endpoint local;
    local.set_endpoint_id("local-1");
    local.set_service("svc");
    local.set_target("127.0.0.1:7000");
    dir.upsert_local(local);
    EXPECT(dir.version() == 6);
    {
        auto found = dir.find("local-1");
        EXPECT(found.has_value());
        EXPECT(found.has_value() && found->version() == 1);
    }
    dir.upsert_local(local);
    {
        auto found = dir.find("local-1");
        EXPECT(found.has_value() && found->version() == 2);
    }
}

void test_sticky_basic() {
    creek::StickyBalancer balancer;
    std::vector<creek::v1::Endpoint> eps;
    eps.push_back(ep("e1", "svc", "10.0.0.1:9001", 1, 100));
    eps.push_back(ep("e2", "svc", "10.0.0.1:9002", 1, 100));
    eps.push_back(ep("e3", "svc", "10.0.0.1:9003", 1, 100));

    creek::Metadata a;
    a["sticky"] = "true";
    a["sid"] = "alpha";

    creek::Metadata b;
    b["sticky"] = "1";
    b["sid"] = "beta";

    creek::Metadata none;
    none["sticky"] = "false";
    none["sid"] = "gamma";

    creek::Metadata true_no_sid;
    true_no_sid["sticky"] = "true";

    auto t0 = creek::SteadyClock::now();
    auto p1 = balancer.pick("svc", a, eps, t0);
    EXPECT(p1.has_value());
    EXPECT(p1.has_value() && p1->endpoint_id() == "e1");

    auto p2 = balancer.pick("svc", a, eps, t0);
    EXPECT(p2.has_value() && p2->endpoint_id() == "e1");

    auto p3 = balancer.pick("svc", b, eps, t0);
    EXPECT(p3.has_value() && p3->endpoint_id() == "e2");

    auto p4 = balancer.pick("svc", b, eps, t0);
    EXPECT(p4.has_value() && p4->endpoint_id() == "e2");

    auto p5 = balancer.pick("svc", none, eps, t0);
    EXPECT(p5.has_value());
    EXPECT(p5.has_value() && p5->endpoint_id() == "e3");

    auto p6 = balancer.pick("svc", true_no_sid, eps, t0);
    EXPECT(p6.has_value());
    EXPECT(p6.has_value() && p6->endpoint_id() == "e1");

    EXPECT(balancer.active_size() == 2);
    EXPECT(balancer.lru_size() == 0);

    std::vector<creek::v1::Endpoint> empty;
    auto p_empty = balancer.pick("svc", a, empty, t0);
    EXPECT(!p_empty.has_value());
}

void test_sticky_timeout() {
    creek::StickyBalancer balancer(1024, std::chrono::milliseconds(60));
    std::vector<creek::v1::Endpoint> eps;
    eps.push_back(ep("e1", "svc", "10.0.0.1:9001", 1, 100));
    eps.push_back(ep("e2", "svc", "10.0.0.1:9002", 1, 100));
    eps.push_back(ep("e3", "svc", "10.0.0.1:9003", 1, 100));

    creek::Metadata meta;
    meta["sticky"] = "true";
    meta["sid"] = "session-A";

    auto t0 = creek::SteadyClock::now();
    auto p1 = balancer.pick("svc", meta, eps, t0);
    EXPECT(p1.has_value() && p1->endpoint_id() == "e1");

    auto p2 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(20));
    EXPECT(p2.has_value() && p2->endpoint_id() == "e1");
    EXPECT(balancer.active_size() == 1);
    EXPECT(balancer.lru_size() == 0);

    auto p3 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(200));
    EXPECT(p3.has_value());
    EXPECT(p3.has_value() && p3->endpoint_id() != "e1");
    EXPECT(balancer.active_size() == 1);
    EXPECT(balancer.lru_size() == 0);

    auto winner = p3->endpoint_id();
    auto p4 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(220));
    EXPECT(p4.has_value() && p4->endpoint_id() == winner);
}

void test_sticky_offline() {
    creek::StickyBalancer balancer;
    std::vector<creek::v1::Endpoint> eps;
    eps.push_back(ep("e1", "svc", "10.0.0.1:9001", 1, 100, true));
    eps.push_back(ep("e2", "svc", "10.0.0.1:9002", 1, 100, true));
    eps.push_back(ep("e3", "svc", "10.0.0.1:9003", 1, 100, true));

    creek::Metadata meta;
    meta["sticky"] = "true";
    meta["sid"] = "session-A";

    auto t0 = creek::SteadyClock::now();
    auto p1 = balancer.pick("svc", meta, eps, t0);
    EXPECT(p1.has_value());
    auto first = p1->endpoint_id();
    EXPECT(first == "e1");

    for (auto& e : eps) {
        if (e.endpoint_id() == first) e.set_alive(false);
    }

    auto p2 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(10));
    EXPECT(p2.has_value());
    EXPECT(p2.has_value() && p2->endpoint_id() != first);
    auto second = p2->endpoint_id();
    EXPECT(second == "e2" || second == "e3");

    auto p3 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(20));
    EXPECT(p3.has_value() && p3->endpoint_id() == second);

    balancer.invalidate(second);
    EXPECT(balancer.active_size() == 0);

    auto p4 = balancer.pick("svc", meta, eps, t0 + std::chrono::milliseconds(30));
    EXPECT(p4.has_value());
    EXPECT(p4.has_value() && p4->endpoint_id() != second);
    EXPECT(p4.has_value() && p4->endpoint_id() != first);
}

void test_minute_rotation() {
    creek::MetricsStore store(std::chrono::milliseconds(60));
    auto ev = make_event("in", "SayHello", "alpha", 100, 250, true);
    auto ev_err = make_event("in", "SayHello", "alpha", 0, 50, false);

    store.record(ev);
    EXPECT(store.current().size() == 1);
    EXPECT(store.previous().empty());

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    store.record(ev_err);
    auto prev = store.previous();
    auto curr = store.current();
    EXPECT(prev.size() == 1);
    EXPECT(curr.size() == 1);
    {
        auto it = prev.begin();
        EXPECT(it->second.calls == 1);
        EXPECT(it->second.errors == 0);
        EXPECT(it->second.bytes == 100);
    }
    {
        auto it = curr.begin();
        EXPECT(it->second.calls == 1);
        EXPECT(it->second.errors == 1);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    auto curr2 = store.current();
    EXPECT(curr2.empty());

    auto prev2 = store.previous();
    EXPECT(prev2.size() == 1);
    {
        auto it = prev2.begin();
        EXPECT(it->second.calls == 1);
        EXPECT(it->second.errors == 1);
    }
}

void test_take_clear() {
    creek::MetricsStore store;
    auto ev = make_event("in", "SayHello", "alpha", 100, 250, true);
    auto ev2 = make_event("out", "SayBye", "beta", 200, 500, true);

    store.record(ev);
    store.record(ev2);
    EXPECT(store.current().size() == 2);

    auto reply = store.protobuf_snapshot(false, true);
    EXPECT(reply.points_size() == 2);
    EXPECT(store.current().size() == 2);
    EXPECT(store.protobuf_snapshot(false, true).points_size() == 0);

    store.record(ev);
    auto reply2 = store.protobuf_snapshot(false, false);
    EXPECT(reply2.points_size() == 2);
    EXPECT(store.current().size() == 2);

    auto taken = store.take();
    EXPECT(taken.size() == 1);
    EXPECT(store.current().size() == 2);

    auto empty_take = store.take();
    EXPECT(empty_take.empty());

    auto prev_reply = store.protobuf_snapshot(true, false);
    EXPECT(prev_reply.points_size() == 0);
    EXPECT(store.previous().empty());
}

void test_openmetrics_and_json() {
    creek::MetricsStore store;
    auto ev = make_event("in", "SayHello", "alpha", 100, 250, true);
    store.record(ev);

    auto text = store.openmetrics();
    EXPECT(text.find("creek_rpc_calls_total") != std::string::npos);
    EXPECT(text.find("# TYPE creek_rpc_calls_total counter") != std::string::npos);
    EXPECT(text.find("direction=\"in\"") != std::string::npos);
    EXPECT(text.find("# EOF") != std::string::npos);

    auto j = store.json(false, false);
    EXPECT(j.find("\"direction\":\"in\"") != std::string::npos);
    EXPECT(j.find("\"rpc_name\":\"SayHello\"") != std::string::npos);
    EXPECT(j.find("\"calls\":1") != std::string::npos);
}

}

int main() {
    test_directory_conflict();
    test_sticky_basic();
    test_sticky_timeout();
    test_sticky_offline();
    test_minute_rotation();
    test_take_clear();
    test_openmetrics_and_json();

    std::cout << "Passed: " << g_passed << " Failed: " << g_failed << std::endl;
    return g_failed == 0 ? 0 : 1;
}
