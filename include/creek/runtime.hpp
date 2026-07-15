#pragma once

#include "creek/metrics.hpp"
#include "creek/redis.hpp"
#include "creek/routing.hpp"
#include "creek/tight.hpp"
#include "creek.grpc.pb.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace creek {

struct NodeConfig {
    std::string id;
    Address udp_bind;
    Address metrics_bind;
    std::string token;
    std::vector<RemotePeer> peers;
    std::chrono::milliseconds sync_interval{15000};
    std::chrono::milliseconds metric_period{60000};
    RedisOptions redis;
};

struct LeafConfig {
    std::string id;
    Address udp_bind;
    Address grpc_bind;
    Address json_bind;
    Address metrics_bind;
    std::string token;
    std::vector<RemotePeer> parents;
    std::chrono::milliseconds sync_interval{15000};
    std::chrono::milliseconds backend_timeout{3000};
    std::chrono::milliseconds rpc_timeout{15000};
    std::chrono::milliseconds metric_period{60000};
    RedisOptions redis;
};

class NodeRuntime {
public:
    class Impl;
    explicit NodeRuntime(NodeConfig config);
    ~NodeRuntime();
    bool start();
    void stop();

private:
    std::unique_ptr<Impl> impl_;
};

class LeafRuntime {
public:
    class Impl;
    explicit LeafRuntime(LeafConfig config);
    ~LeafRuntime();
    bool start();
    void stop();

private:
    std::unique_ptr<Impl> impl_;
};

}
