#pragma once

#include "creek/filter_chain.hpp"

#include <chrono>
#include <random>
#include <string>

namespace creek {

class DelayFilter : public Filter {
public:
    DelayFilter(int delay_ms, double probability);

    RpcContext on_request(RpcContext ctx) override;
    RpcContext on_response(RpcContext ctx) override;
    std::string name() const override;

private:
    std::chrono::milliseconds delay_;
    double probability_;
    std::mt19937 rng_;
};

class MirrorFilter : public Filter {
public:
    explicit MirrorFilter(std::string mirror_target);

    RpcContext on_request(RpcContext ctx) override;
    RpcContext on_response(RpcContext ctx) override;
    std::string name() const override;

private:
    std::string mirror_target_;
};

class CanaryFilter : public Filter {
public:
    CanaryFilter(std::string canary_endpoint, int percentage);

    RpcContext on_request(RpcContext ctx) override;
    RpcContext on_response(RpcContext ctx) override;
    std::string name() const override;

private:
    std::string canary_endpoint_;
    int percentage_;
    std::mt19937 rng_;
};

}
