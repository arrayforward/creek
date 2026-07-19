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

namespace creek::framework { class Framework; struct ChangeSet; struct Message; }

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

class NodeRuntime {
public:
    class Impl;
    explicit NodeRuntime(NodeConfig config);
    ~NodeRuntime();
    bool start();
    void stop();
    void set_framework(framework::Framework* fw);
    framework::ChangeSet process_batch(const std::vector<framework::Message>& batch);

private:
    std::unique_ptr<Impl> m_impl;
};

}
