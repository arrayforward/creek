#pragma once

// Internal declaration of NodeRuntime::Impl. Not part of the public API.
// Method definitions live in node_runtime.cpp; the public NodeRuntime
// forwarding methods live in src/runtime.cpp.

#include "creek/node/node_runtime.hpp"

#include "creek/metrics.hpp"
#include "creek/redis.hpp"
#include "creek/routing.hpp"
#include "creek/tight.hpp"
#include "creek/types.hpp"
#include "creek/framework/framework.hpp"
#include "creek/framework/message.hpp"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace creek {

class NodeRuntime::Impl {
public:
    explicit Impl(NodeConfig config);
    ~Impl();

    bool start();
    void stop();

    void set_framework(framework::Framework* fw);
    framework::ChangeSet process_batch(const std::vector<framework::Message>& batch);

private:
    framework::Framework* m_framework{nullptr};

    void on_peer_event(const PeerEvent& ev);
    void on_message(const std::string& peer_id, Bytes payload);
    void handle_directory(const std::string& peer_id,
                          const creek::v1::DirectorySnapshot& snap,
                          std::size_t raw_size);
    void handle_request(const std::string& peer_id,
                        const creek::v1::RoutedRequest& req,
                        std::size_t raw_size);
    void handle_response(const std::string& peer_id,
                         const creek::v1::RoutedResponse& resp,
                         std::size_t raw_size);
    void route_request_locked(const std::string& from_peer,
                              const creek::v1::RoutedRequest& req);
    void route_response_locked(const std::string& from_peer,
                               const creek::v1::RoutedResponse& resp);
    void send_error_response_locked(const creek::v1::RoutedRequest& req,
                                    const std::string& error);
    void broadcast_snapshot_locked();
    void push_to_leaves_locked();
    void revoke_leaf_locked(const std::string& leaf_id);
    void do_sync_work();
    void do_redis_sync_work();
    void record_metric(const std::string& direction, const std::string& rpc_name,
                       std::uint64_t bytes, std::uint64_t latency_us, bool success,
                       const Metadata& metadata = Metadata{});

    NodeConfig m_config;
    std::unique_ptr<TightTransport> m_transport;
    std::shared_ptr<MetricsStore> m_metrics_store;
    std::unique_ptr<MetricsHttpServer> m_metrics_server;
    std::unique_ptr<RedisClient> m_redis;
    EndpointDirectory m_directory;
    std::unordered_map<std::string, SteadyClock::time_point> m_leaves;
    std::unordered_set<std::string> m_known_node_peers;
    std::unordered_map<std::string, std::unordered_set<std::string>> m_leaf_endpoints;
    std::uint64_t m_version_counter{};
    std::atomic<bool> m_running{false};
    std::mutex m_mutex;
    framework::TaskId m_sync_task_id{0};
    framework::TaskId m_redis_sync_task_id{0};
};

}
