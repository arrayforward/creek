#pragma once

#include "creek/types.hpp"

#include <memory>
#include <string>
#include <vector>

namespace creek {

struct RpcContext {
    std::string service;
    std::string method;
    Metadata metadata;
    bool is_response = false;
    int status_code = 0;
};

class Filter {
public:
    virtual ~Filter();
    virtual RpcContext on_request(RpcContext ctx) = 0;
    virtual RpcContext on_response(RpcContext ctx) = 0;
    virtual std::string name() const = 0;
};

class FilterChain {
public:
    void add(std::shared_ptr<Filter> f);
    RpcContext process_request(RpcContext ctx);
    RpcContext process_response(RpcContext ctx);

private:
    std::vector<std::shared_ptr<Filter>> filters_;
};

}
