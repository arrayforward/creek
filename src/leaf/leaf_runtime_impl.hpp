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

#include "generic_proxy.hpp"

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

    // ---- generic gRPC proxy (any /pkg.Service/Method, unary + bidi) ----
    // Sends a wire payload towards a destination leaf/node using the same
    // parent-selection and direct-connect fallback as send_routed_request.
    bool send_to_mesh(const std::string& dest_node, const std::string& dest_leaf,
                      const Bytes& payload, int priority);
    std::shared_ptr<grpc::Channel> get_channel(const std::string& target);
    std::string current_node_id();
    std::shared_ptr<IngressStream> find_stream(const std::string& request_id);
    void backend_stream_finished(BackendStream* bs, const grpc::Status& status);
    // Destination leaf: generic unary call to the local backend.
    void process_inbound_generic(const creek::v1::RoutedRequest& req, std::size_t raw_size);
    // Ingress leaf: generic completion-queue pump and call lifecycle.
    void generic_pump();
    void prime_generic_call();
    void generic_on_new_call(const std::shared_ptr<IngressStream>& s);
    void generic_on_read(IngressStream* s, bool ok);
    void generic_on_write(IngressStream* s, bool ok);
    void generic_on_finish(IngressStream* s, bool ok);
    void generic_on_alarm(IngressStream* s, bool ok);
    void generic_begin_stream(std::shared_ptr<IngressStream> s);
    // Ingress stream plumbing.
    void ingress_queue_write(IngressStream* s, std::string bytes);
    void ingress_try_finish(IngressStream* s);
    void ingress_teardown(const std::shared_ptr<IngressStream>& s,
                          const grpc::Status& status);
    void ingress_forward_frame(IngressStream* s, std::string bytes, bool half_close);
    void ingress_backend_message(IngressStream* s, std::string bytes);
    void ingress_backend_closed(IngressStream* s, const grpc::Status& status);
    void ingress_backend_closed_raw(const std::string& request_id,
                                    const grpc::Status& status);
    // Destination-leaf stream side.
    void stream_worker_loop();
    void process_stream_open(const creek::v1::RoutedStreamOpen& open);
    void process_stream_frame(const creek::v1::RoutedStreamFrame& frame);
    void process_stream_close(const creek::v1::RoutedStreamClose& close);
    void send_stream_close_to(const std::string& request_id,
                              const std::string& dest_leaf, const std::string& dest_node,
                              bool from_origin, std::int32_t status,
                              const std::string& error);
    void stream_sweep_loop();

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
    grpc::Status handle_directory_query(::grpc::ServerContext* context,
                                        const ::creek::v1::DirectoryRequest* request,
                                        ::creek::v1::DirectoryReply* response);
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
    void send_snapshot_to_parent();
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
    // Generic proxy: catches every method not handled by the typed services.
    std::unique_ptr<grpc::AsyncGenericService> m_generic_service;
    std::unique_ptr<grpc::ServerCompletionQueue> m_generic_cq;
    std::thread m_generic_thread;
    std::thread m_sweep_thread;
    // Inbound stream wire messages (open/frame/close) are processed on this
    // dedicated single worker so per-stream ordering is preserved.
    std::thread m_stream_worker_thread;
    BlockingQueue<std::function<void()>> m_stream_queue{1024};
    std::vector<std::shared_ptr<IngressStream>> m_pending_calls;
    std::unordered_map<std::string, std::shared_ptr<IngressStream>> m_streams;
    std::unordered_map<std::string, BackendStream*> m_backend_streams;
    std::thread m_grpc_wait_thread;
    // Worker pool that executes inbound routed requests (backend gRPC calls)
    // off the tight transport's single receiver thread.
    std::vector<std::thread> m_worker_threads;
    BlockingQueue<std::function<void()>> m_request_queue{256};
    std::atomic<bool> m_running{false};
    std::mutex m_mutex;
    std::unordered_map<std::string, LocalEndpoint> m_local_endpoints;
    // Tombstones of locally-removed endpoints: id -> (shell, removed_ms).
    // Advertised in our snapshots until the TTL expires.
    std::unordered_map<std::string, std::pair<creek::v1::Endpoint, std::uint64_t>> m_local_tombstones;
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
