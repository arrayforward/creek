#include "creek/filter_chain.hpp"

#include <algorithm>

namespace creek {

Filter::~Filter() = default;

void FilterChain::add(std::shared_ptr<Filter> f) {
    filters_.push_back(std::move(f));
}

RpcContext FilterChain::process_request(RpcContext ctx) {
    for (auto& f : filters_) {
        ctx = f->on_request(std::move(ctx));
    }
    return ctx;
}

RpcContext FilterChain::process_response(RpcContext ctx) {
    for (auto it = filters_.rbegin(); it != filters_.rend(); ++it) {
        ctx = (*it)->on_response(std::move(ctx));
    }
    return ctx;
}

}
