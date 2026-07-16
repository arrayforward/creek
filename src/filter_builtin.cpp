#include "creek/filter_builtin.hpp"

#include <chrono>
#include <random>
#include <thread>

namespace creek {

DelayFilter::DelayFilter(int delay_ms, double probability)
    : delay_(std::chrono::milliseconds(delay_ms))
    , probability_(probability)
    , rng_(std::random_device{}()) {}

RpcContext DelayFilter::on_request(RpcContext ctx) {
    std::bernoulli_distribution dist(probability_);
    if (dist(rng_)) {
        std::this_thread::sleep_for(delay_);
    }
    return ctx;
}

RpcContext DelayFilter::on_response(RpcContext ctx) {
    std::bernoulli_distribution dist(probability_);
    if (dist(rng_)) {
        std::this_thread::sleep_for(delay_);
    }
    return ctx;
}

std::string DelayFilter::name() const {
    return "DelayFilter";
}

MirrorFilter::MirrorFilter(std::string mirror_target)
    : mirror_target_(std::move(mirror_target)) {}

RpcContext MirrorFilter::on_request(RpcContext ctx) {
    ctx.metadata["mirror_destination"] = mirror_target_;
    return ctx;
}

RpcContext MirrorFilter::on_response(RpcContext ctx) {
    return ctx;
}

std::string MirrorFilter::name() const {
    return "MirrorFilter";
}

CanaryFilter::CanaryFilter(std::string canary_endpoint, int percentage)
    : canary_endpoint_(std::move(canary_endpoint))
    , percentage_(percentage)
    , rng_(std::random_device{}()) {}

RpcContext CanaryFilter::on_request(RpcContext ctx) {
    std::uniform_int_distribution<int> dist(0, 99);
    if (dist(rng_) < percentage_) {
        ctx.metadata["endpoint_override"] = canary_endpoint_;
    }
    return ctx;
}

RpcContext CanaryFilter::on_response(RpcContext ctx) {
    return ctx;
}

std::string CanaryFilter::name() const {
    return "CanaryFilter";
}

}
