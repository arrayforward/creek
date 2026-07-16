#include "creek/wasm_runtime.hpp"

#include <cstdio>
#include <cstdlib>
#include <string>

namespace {

int g_passed = 0;
int g_failed = 0;

void EXPECT(bool cond, const std::string& msg) {
    if (cond) ++g_passed;
    else { ++g_failed; std::fprintf(stdout, "FAIL [wasm_test]: %s\n", msg.c_str()); }
}

creek::Metadata make_meta() { return creek::Metadata{}; }

void test_wasm_filter_chain_json() {
    creek::JsonRuleFilterChain chain;
    chain.add_json(R"({"type":"mirror","mirror_target":"shadow:9000"})");
    chain.add_json(R"({"type":"delay","probability":1.0,"delay_ms":10})");

    EXPECT(chain.size() == 2, "chain should have 2 filters");

    auto m = make_meta();
    auto r = chain.process_request("svc", "SayHello", m);

    EXPECT(r.action == creek::JsonRuleFilter::Action::Delay, "last filter action should be delay");
    EXPECT(r.delay_ms == 10, "delay should be 10ms");
    EXPECT(m.count("creek_mirror") == 1, "metadata should have creek_mirror");
    EXPECT(m["creek_mirror"] == "shadow:9000", "mirror target should be shadow:9000");
}

void test_wasm_filter_chain_canary() {
    int reroute_count = 0;
    int total = 500;
    for (int i = 0; i < total; ++i) {
        creek::JsonRuleFilterChain chain;
        chain.add_json(R"({"type":"canary","canary_endpoint":"v2","canary_percentage":25})");
        auto m = make_meta();
        auto r = chain.process_request("svc", "m", m);
        if (r.action == creek::JsonRuleFilter::Action::Reroute) ++reroute_count;
    }
    double rate = static_cast<double>(reroute_count) / total;
    EXPECT(rate > 0.15 && rate < 0.35, "canary 25% rate should be ~0.25");
    std::fprintf(stdout, "[wasm_test] canary: %d/%d (%.3f)\n", reroute_count, total, rate);
}

void test_wasm_filter_reject_short_circuits() {
    creek::JsonRuleFilterChain chain;
    chain.add_json(R"({"type":"reject","probability":1.0})");
    chain.add_json(R"({"type":"delay","probability":1.0,"delay_ms":9999})");
    auto m = make_meta();
    auto r = chain.process_request("svc", "m", m);
    EXPECT(r.action == creek::JsonRuleFilter::Action::Reject, "reject should short-circuit chain");
    EXPECT(r.status_code == 503, "reject status should be 503");
}

void test_wasm_filter_name() {
    creek::JsonRuleFilter f(R"({"type":"delay","probability":0.5,"delay_ms":100})");
    EXPECT(f.name() == "delay", "filter name should be delay");
}

void test_wasm_filter_empty_chain() {
    creek::JsonRuleFilterChain chain;
    EXPECT(chain.size() == 0, "empty chain");
    auto m = make_meta();
    auto r = chain.process_request("svc", "m", m);
    EXPECT(r.action == creek::JsonRuleFilter::Action::Passthrough, "empty chain should passthrough");
    chain.clear();
    EXPECT(chain.size() == 0, "after clear");
}

void test_wasm_filter_chain_move() {
    creek::JsonRuleFilterChain chain;
    chain.add_json(R"({"type":"delay","probability":1.0,"delay_ms":5})");
    creek::JsonRuleFilterChain chain2 = std::move(chain);
    EXPECT(chain.size() == 0, "moved-from chain should be empty");
    EXPECT(chain2.size() == 1, "moved-to chain should have 1 filter");
}

void test_wasm_runtime_singleton() {
    auto& rt = creek::WasmRuntime::instance();
    (void)rt;
    std::fprintf(stdout, "[wasm_test] wasm runtime singleton ok\n");
}

void test_wasm_filter_mirror_metadata() {
    creek::JsonRuleFilterChain chain;
    chain.add_json(R"({"type":"mirror","mirror_target":"bk-99:8000"})");
    chain.add_json(R"({"type":"mirror","mirror_target":"bk-88:9000"})");
    auto m = make_meta();
    chain.process_request("s", "m", m);
    EXPECT(m["creek_mirror"] == "bk-88:9000", "last mirror target wins");
}

void test_wasm_filter_delay_probability_bounds() {
    int delayed = 0;
    int total = 1000;
    for (int i = 0; i < total; ++i) {
        creek::JsonRuleFilterChain chain;
        chain.add_json(R"({"type":"delay","probability":0.10,"delay_ms":1})");
        auto m = make_meta();
        auto r = chain.process_request("s", "m", m);
        if (r.action == creek::JsonRuleFilter::Action::Delay) ++delayed;
    }
    double rate = static_cast<double>(delayed) / total;
    EXPECT(rate > 0.05 && rate < 0.20, "delay 10% rate in bounds");
    std::fprintf(stdout, "[wasm_test] delay_10pct: %d/%d (%.3f)\n", delayed, total, rate);
}

void test_wasm_filter_multiple_types() {
    int mirror = 0, delay = 0;
    for (int i = 0; i < 500; ++i) {
        creek::JsonRuleFilterChain chain;
        chain.add_json(R"({"type":"mirror","probability":1.0,"mirror_target":"m:1"})");
        chain.add_json(R"({"type":"delay","probability":1.0,"delay_ms":1})");
        chain.add_json(R"({"type":"canary","canary_endpoint":"c1","canary_percentage":50})");
        auto m = make_meta();
        auto r = chain.process_request("s", "m", m);
        if (r.mirror_target == "m:1") ++mirror;
        if (r.action == creek::JsonRuleFilter::Action::Delay) ++delay;
    }
    EXPECT(mirror == 500, "mirror target always set");
    EXPECT(delay == 500, "delay always triggered");
}

} // namespace

int main() {
    test_wasm_filter_chain_json();
    test_wasm_filter_chain_canary();
    test_wasm_filter_reject_short_circuits();
    test_wasm_filter_name();
    test_wasm_filter_empty_chain();
    test_wasm_filter_chain_move();
    test_wasm_runtime_singleton();
    test_wasm_filter_mirror_metadata();
    test_wasm_filter_delay_probability_bounds();
    test_wasm_filter_multiple_types();
    std::fprintf(stdout, "Wasm Tests: Passed=%d Failed=%d\n", g_passed, g_failed);
    return g_failed == 0 ? 0 : 1;
}
