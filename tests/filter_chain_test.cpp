#include "creek/filter_builtin.hpp"
#include "creek/filter_chain.hpp"
#include "creek/types.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
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

void test_delay_filter_probability() {
    constexpr int kTrials = 100;
    constexpr double kProbability = 0.3;

    creek::DelayFilter filter(10, kProbability);

    int delayed = 0;
    for (int i = 0; i < kTrials; ++i) {
        creek::RpcContext ctx;
        ctx.service = "TestService";
        ctx.method = "TestMethod";
        auto start = std::chrono::steady_clock::now();
        ctx = filter.on_request(std::move(ctx));
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed >= 8) {
            ++delayed;
        }
    }

    double actual = static_cast<double>(delayed) / kTrials;
    double error = std::abs(actual - kProbability);

    EXPECT(error < 0.2);
    EXPECT(filter.name() == "DelayFilter");

    std::fprintf(stdout, "[filter_chain] delay_filter: %d/%d delayed (rate=%.2f expected=%.2f error=%.2f)\n",
                 delayed, kTrials, actual, kProbability, error);
}

void test_mirror_filter_mark() {
    creek::MirrorFilter filter("backend-mirror:9000");

    creek::RpcContext ctx;
    ctx.service = "TestService";
    ctx.method = "MirrorMethod";

    ctx = filter.on_request(std::move(ctx));

    EXPECT(ctx.metadata.count("mirror_destination") == 1);
    EXPECT(ctx.metadata["mirror_destination"] == "backend-mirror:9000");
    EXPECT(filter.name() == "MirrorFilter");

    std::fprintf(stdout, "[filter_chain] mirror_filter: mirror_destination=%s\n",
                 ctx.metadata["mirror_destination"].c_str());
}

void test_canary_filter_split() {
    constexpr int kTrials = 1000;
    constexpr int kPercentage = 30;

    creek::CanaryFilter filter("canary-v2:9000", kPercentage);

    int canary_count = 0;
    for (int i = 0; i < kTrials; ++i) {
        creek::RpcContext ctx;
        ctx.service = "TestService";
        ctx.method = "TestMethod";
        ctx = filter.on_request(std::move(ctx));
        if (ctx.metadata.count("endpoint_override") && ctx.metadata["endpoint_override"] == "canary-v2:9000") {
            ++canary_count;
        }
    }

    double actual = static_cast<double>(canary_count) / kTrials;
    double expected = kPercentage / 100.0;
    double error = std::abs(actual - expected);

    EXPECT(error < 0.06);
    EXPECT(filter.name() == "CanaryFilter");

    std::fprintf(stdout, "[filter_chain] canary_filter: %d/%d routed to canary (rate=%.3f expected=%.2f error=%.3f)\n",
                 canary_count, kTrials, actual, expected, error);
}

void test_canary_filter_no_route() {
    creek::CanaryFilter filter("canary-v2:9000", 0);

    for (int i = 0; i < 50; ++i) {
        creek::RpcContext ctx;
        ctx.service = "Svc";
        ctx.method = "M";
        ctx = filter.on_request(std::move(ctx));
        EXPECT(ctx.metadata.count("endpoint_override") == 0);
    }
}

void test_canary_filter_all_routed() {
    creek::CanaryFilter filter("canary-v2:9000", 100);

    for (int i = 0; i < 50; ++i) {
        creek::RpcContext ctx;
        ctx.service = "Svc";
        ctx.method = "M";
        ctx = filter.on_request(std::move(ctx));
        EXPECT(ctx.metadata.count("endpoint_override") == 1);
        EXPECT(ctx.metadata["endpoint_override"] == "canary-v2:9000");
    }
}

struct OrderRec {
    std::string filter_name;
    bool is_request;
};

class OrderTrackingFilter : public creek::Filter {
public:
    explicit OrderTrackingFilter(std::string name, std::vector<OrderRec>* log)
        : name_(std::move(name)), log_(log) {}

    creek::RpcContext on_request(creek::RpcContext ctx) override {
        log_->push_back({name_, true});
        return ctx;
    }

    creek::RpcContext on_response(creek::RpcContext ctx) override {
        log_->push_back({name_, false});
        return ctx;
    }

    std::string name() const override {
        return name_;
    }

private:
    std::string name_;
    std::vector<OrderRec>* log_;
};

void test_filter_chain_order() {
    std::vector<OrderRec> log;

    creek::FilterChain chain;
    chain.add(std::make_shared<OrderTrackingFilter>("A", &log));
    chain.add(std::make_shared<OrderTrackingFilter>("B", &log));
    chain.add(std::make_shared<OrderTrackingFilter>("C", &log));

    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx = chain.process_request(std::move(ctx));

    EXPECT(log.size() == 3);
    EXPECT(log[0].filter_name == "A" && log[0].is_request);
    EXPECT(log[1].filter_name == "B" && log[1].is_request);
    EXPECT(log[2].filter_name == "C" && log[2].is_request);

    ctx = chain.process_response(std::move(ctx));

    EXPECT(log.size() == 6);
    EXPECT(log[3].filter_name == "C" && !log[3].is_request);
    EXPECT(log[4].filter_name == "B" && !log[4].is_request);
    EXPECT(log[5].filter_name == "A" && !log[5].is_request);

    std::fprintf(stdout, "[filter_chain] order: ");
    for (const auto& r : log) {
        std::fprintf(stdout, "%s(%s) ", r.filter_name.c_str(), r.is_request ? "req" : "resp");
    }
    std::fprintf(stdout, "\n");
}

void test_filter_chain_metadata_mutation() {
    class AppendFilter : public creek::Filter {
    public:
        creek::RpcContext on_request(creek::RpcContext ctx) override {
            ctx.metadata["pipeline"] += ":append";
            return ctx;
        }
        creek::RpcContext on_response(creek::RpcContext ctx) override {
            ctx.metadata["pipeline"] += ":response";
            return ctx;
        }
        std::string name() const override { return "AppendFilter"; }
    };

    class PrefixFilter : public creek::Filter {
    public:
        creek::RpcContext on_request(creek::RpcContext ctx) override {
            ctx.metadata["pipeline"] = "prefix" + ctx.metadata["pipeline"];
            return ctx;
        }
        creek::RpcContext on_response(creek::RpcContext ctx) override {
            ctx.metadata["pipeline"] = "resp_prefix" + ctx.metadata["pipeline"];
            return ctx;
        }
        std::string name() const override { return "PrefixFilter"; }
    };

    creek::FilterChain chain;
    chain.add(std::make_shared<PrefixFilter>());
    chain.add(std::make_shared<AppendFilter>());

    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx.metadata["pipeline"] = "start";

    ctx = chain.process_request(std::move(ctx));
    EXPECT(ctx.metadata["pipeline"] == "prefixstart:append");

    ctx.is_response = true;
    ctx = chain.process_response(std::move(ctx));
    EXPECT(ctx.metadata["pipeline"] == "resp_prefixprefixstart:append:response");

    std::fprintf(stdout, "[filter_chain] mutation: %s\n", ctx.metadata["pipeline"].c_str());
}

void test_delay_filter_zero_probability() {
    creek::DelayFilter filter(1000, 0.0);

    auto start = std::chrono::steady_clock::now();
    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx = filter.on_request(std::move(ctx));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    EXPECT(elapsed < 50);
}

void test_delay_filter_full_probability() {
    creek::DelayFilter filter(20, 1.0);

    auto start = std::chrono::steady_clock::now();
    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx = filter.on_request(std::move(ctx));
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    EXPECT(elapsed >= 15);
}

void test_mirror_filter_preserves_context() {
    creek::MirrorFilter filter("mirror:8080");

    creek::RpcContext ctx;
    ctx.service = "OriginalService";
    ctx.method = "OriginalMethod";
    ctx.status_code = 42;
    ctx.is_response = true;

    ctx = filter.on_request(std::move(ctx));

    EXPECT(ctx.service == "OriginalService");
    EXPECT(ctx.method == "OriginalMethod");
    EXPECT(ctx.status_code == 42);
    EXPECT(ctx.is_response == true);
}

void test_canary_filter_response_passthrough() {
    creek::CanaryFilter filter("canary-ep:9000", 50);

    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx.status_code = 200;
    ctx.is_response = true;

    auto before = ctx.metadata;
    ctx = filter.on_response(std::move(ctx));

    EXPECT(ctx.service == "Svc");
    EXPECT(ctx.method == "M");
    EXPECT(ctx.status_code == 200);
    EXPECT(ctx.is_response == true);
    EXPECT(ctx.metadata.size() == before.size());
}

void test_empty_filter_chain() {
    creek::FilterChain chain;

    creek::RpcContext ctx;
    ctx.service = "Svc";
    ctx.method = "M";
    ctx.metadata["key"] = "value";

    ctx = chain.process_request(std::move(ctx));
    EXPECT(ctx.service == "Svc");
    EXPECT(ctx.method == "M");
    EXPECT(ctx.metadata["key"] == "value");

    ctx = chain.process_response(std::move(ctx));
    EXPECT(ctx.service == "Svc");
    EXPECT(ctx.metadata["key"] == "value");
}

void test_combined_filters() {
    creek::FilterChain chain;
    chain.add(std::make_shared<creek::MirrorFilter>("mirror:9000"));
    chain.add(std::make_shared<creek::CanaryFilter>("canary:9000", 50));

    int mirror_set = 0;
    int canary_set = 0;
    int both_set = 0;

    for (int i = 0; i < 200; ++i) {
        creek::RpcContext ctx;
        ctx.service = "Svc";
        ctx.method = "M";

        ctx = chain.process_request(std::move(ctx));

        bool has_mirror = ctx.metadata.count("mirror_destination") > 0;
        bool has_canary = ctx.metadata.count("endpoint_override") > 0;

        if (has_mirror) ++mirror_set;
        if (has_canary) ++canary_set;
        if (has_mirror && has_canary) ++both_set;
    }

    EXPECT(mirror_set == 200);
    EXPECT(canary_set > 0);
    EXPECT(canary_set < 200);
    EXPECT(both_set == canary_set);

    std::fprintf(stdout, "[filter_chain] combined: mirror=%d canary=%d both=%d\n",
                 mirror_set, canary_set, both_set);
}

}

int main() {
    test_delay_filter_probability();
    test_delay_filter_zero_probability();
    test_delay_filter_full_probability();
    test_mirror_filter_mark();
    test_mirror_filter_preserves_context();
    test_canary_filter_split();
    test_canary_filter_no_route();
    test_canary_filter_all_routed();
    test_canary_filter_response_passthrough();
    test_filter_chain_order();
    test_filter_chain_metadata_mutation();
    test_empty_filter_chain();
    test_combined_filters();

    std::cout << "Passed: " << g_passed << " Failed: " << g_failed << std::endl;
    return g_failed == 0 ? 0 : 1;
}
