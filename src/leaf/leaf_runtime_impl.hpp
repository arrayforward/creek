#pragma once

// Internal declaration of LeafRuntime::Impl. Not part of the public API.
// Method definitions live in leaf_runtime.cpp; the public LeafRuntime
// forwarding methods live in src/runtime.cpp. The gRPC service
// implementations (src/rpc/*.cpp) include this header to call the
// handle_* methods they are friends of.

#include "creek/leaf/leaf_runtime.hpp"

#include "creek/rpc/greeter_service.hpp"
#include "creek/rpc/leaf_control_service.hpp"
#include "creek/rpc/admin_service.hpp"

#include "creek/blocking_queue.hpp"
#include "creek/circuit_breaker.hpp"
#include "creek/json_rpc.hpp"
#include "creek/metrics.hpp"
#include "creek/redis.hpp"
#include "creek/routing.hpp"
#include "creek/tight.hpp"
#include "creek/types.hpp"
#include "creek/framework/framework.hpp"
#include "creek/framework/message.hpp"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace creek {

class LeafRuntime::Impl {
public:
    struct PendingResponse {
        std::mutex m;
        std::condition_variable cv;
        bool done{false};
        creek::v1::RoutedResponse response;
    };

    explicit Impl(LeafConfig config);
    ~Impl();

    bool start();
    void stop();

    void on_message(const std::string& peer_id, Bytes payload);
    void on_peer_event(const PeerEvent& ev);
    void handle_directory(const creek::v1::DirectorySnapshot& snap, std::size_t raw_size);
    void worker_loop();
    void handle_inbound_request(const creek::v1::RoutedRequest& req, std::size_t raw_size);
    void process_inbound_request(const creek::v1::RoutedRequest& req, std::size_t raw_size);
    void handle_inbound_response(const creek::v1::RoutedResponse& resp, std::size_t raw_size);
    creek::v1::RoutedResponse make_error_response(const creek::v1::RoutedRequest& req,
                                                  const std::string& error);
    void send_response_to_parent(const creek::v1::RoutedResponse& resp);
    grpc::Status call_backend(const creek::v1::Endpoint& ep,
                              const creek::v1::HelloRequest& req,
                              const Metadata& metadata,
                              creek::v1::HelloReply* reply,
                              std::uint64_t* latency_us,
                              std::size_t* out_bytes);
    std::unique_ptr<creek::v1::Greeter::Stub> get_stub(const std::string& target);
    grpc::Status send_routed_request(const creek::v1::Endpoint& ep,
                                     const creek::v1::HelloRequest& req,
                                     const Metadata& metadata,
                                     creek::v1::HelloReply* reply,
                                     std::uint64_t* latency_us);
    std::pair<int, std::string> handle_json_rpc(std::string body,
                                                const JsonRpcHttpServer::HeaderMap& headers);
    grpc::Status invoke_for_hello(const creek::v1::HelloRequest& request,
                                  const Metadata& metadata,
                                  creek::v1::HelloReply* response);
    grpc::Status handle_say_hello(::grpc::ServerContext* context,
                                  const ::creek::v1::HelloRequest* request,
                                  ::creek::v1::HelloReply* response);
    grpc::Status handle_register(::grpc::ServerContext* context,
                                 const ::creek::v1::RegisterRequest* request,
                                 ::creek::v1::RegisterReply* response);
    grpc::Status handle_heartbeat(::grpc::ServerContext* context,
                                  const ::creek::v1::HeartbeatRequest* request,
                                  ::creek::v1::HeartbeatReply* response);
    grpc::Status handle_deregister(::grpc::ServerContext* context,
                                   const ::creek::v1::DeregisterRequest* request,
                                   ::creek::v1::DeregisterReply* response);
    grpc::Status handle_metrics(::grpc::ServerContext* context,
                                const ::creek::v1::MetricRequest* request,
                                ::creek::v1::MetricReply* response);
    grpc::Status handle_set_sticky(::grpc::ServerContext* context,
                                   const ::creek::v1::StickyStrategyRequest* request,
                                   ::creek::v1::StickyStrategyReply* response);
    grpc::Status handle_set_breaker(::grpc::ServerContext* context,
                                    const ::creek::v1::BreakerConfigRequest* request,
                                    ::creek::v1::BreakerConfigReply* response);
    grpc::Status handle_push_wasm(::grpc::ServerContext* context,
                                  const ::creek::v1::PushWasmRequest* request,
                                  ::creek::v1::PushWasmReply* response);
    grpc::Status handle_list_wasm(::grpc::ServerContext* context,
                                  const ::creek::v1::ListWasmRequest* request,
                                  ::creek::v1::ListWasmReply* response);
    grpc::Status handle_unload_wasm(::grpc::ServerContext* context,
                                    const ::creek::v1::UnloadWasmRequest* request,
                                    ::creek::v1::UnloadWasmReply* response);
    void m_redissync_loop();
    void send_snapshot_to_parent();
    void heartbeat_loop();
    void sync_loop();
    void do_heartbeat_work();
    void do_sync_work();
    void do_redis_sync_work();
    void record_metric(const std::string& direction, const std::string& rpc_name,
                       std::uint64_t bytes, std::uint64_t latency_us, bool success,
                       const Metadata& metadata = Metadata{});
    void set_framework(framework::Framework* fw);
    framework::ChangeSet process_batch(const std::vector<framework::Message>& batch);

    friend class GreeterService;
    friend class LeafControlService;
    friend class AdminService;

    struct LocalEndpoint {
        creek::v1::Endpoint endpoint;
        SteadyClock::time_point last_heartbeat{};
    };

    LeafConfig m_config;
    std::unique_ptr<TightTransport> m_transport;
    std::shared_ptr<MetricsStore> m_metrics_store;
    std::unique_ptr<MetricsHttpServer> m_metrics_server;
    std::unique_ptr<RedisClient> m_redis;
    std::unique_ptr<JsonRpcHttpServer> m_json_rpc_server;
    EndpointDirectory m_directory;
    StickyBalancer m_balancer;
    CircuitBreaker m_breaker;
    std::unique_ptr<grpc::Server> m_grpc_server;
    std::unique_ptr<GreeterService> m_greeter_service;
    std::unique_ptr<LeafControlService> m_leaf_control_service;
    std::unique_ptr<AdminService> m_admin_service;
    std::thread m_grpc_wait_thread;
    std::thread m_heartbeat_thread;
    std::thread m_sync_thread;
    std::thread m_redissync_thread_;
    // Worker pool that executes inbound routed requests (backend gRPC calls)
    // off the tight transport's single receiver thread.
    std::vector<std::thread> m_worker_threads;
    BlockingQueue<std::function<void()>> m_request_queue{256};
    std::atomic<bool> m_running{false};
    std::mutex m_mutex;
    std::unordered_map<std::string, LocalEndpoint> m_local_endpoints;
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> m_channels;
    std::unordered_map<std::string, std::shared_ptr<PendingResponse>> m_pending;
    std::unordered_map<std::string, std::string> m_peer_targets;
    std::unordered_map<std::string, creek::Address> m_known_leaves;
    std::set<std::string> m_parent_ids;
    std::string m_active_parent;
    std::atomic<bool> m_all_parents_down{true};
    std::vector<uint32_t> m_loaded_wasm_ids;
    std::uint64_t m_version_counter{};
    framework::Framework* m_framework{nullptr};
    framework::TaskId m_heartbeat_task_id{0};
    framework::TaskId m_sync_task_id{0};
    framework::TaskId m_redis_sync_task_id{0};
};

}
