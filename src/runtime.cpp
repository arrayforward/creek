#include "creek/runtime.hpp"

#include "creek.pb.h"
#include "creek.grpc.pb.h"
#include "creek/circuit_breaker.hpp"
#include "creek/logger.hpp"
#include "creek/trace_context.hpp"
#include "creek/wasm_runtime.hpp"
#include "creek/json_rpc.hpp"
#include "creek/metrics.hpp"
#include "creek/redis.hpp"
#include "creek/routing.hpp"
#include "creek/tight.hpp"
#include "creek/types.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/client_context.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace creek {

namespace {

std::string format_address(const Address& addr) {
    return addr.host + ":" + std::to_string(addr.port);
}

Bytes serialize_wire(const creek::v1::WireMessage& msg) {
    Bytes out(static_cast<std::size_t>(msg.ByteSizeLong()));
    if (!out.empty()) {
        msg.SerializeToArray(out.data(), static_cast<int>(out.size()));
    }
    return out;
}

bool parse_wire(const Bytes& data, creek::v1::WireMessage& msg) {
    if (data.empty()) return false;
    return msg.ParseFromArray(data.data(), static_cast<int>(data.size()));
}

Metadata request_metadata(const creek::v1::RoutedRequest& request) {
    Metadata metadata;
    for (const auto& entry : request.metadata()) {
        metadata[entry.first] = entry.second;
    }
    return metadata;
}

constexpr std::uint32_t kDefaultHopLimit = 16;
constexpr std::uint32_t kDefaultBackendHopLimit = 16;

}

class NodeRuntime::Impl {
public:
    explicit Impl(NodeConfig config)
        : config_(std::move(config)),
          version_counter_(unix_millis()) {}

    ~Impl() { stop(); }

    bool start() {
        if (running_.load()) return true;

        CREEK_LOG_INFO(std::string("[runtime] node start id=") + config_.id);

        if (config_.redis.port != 0) {
            try {
                redis_ = std::make_unique<RedisClient>(config_.redis, config_.id);
                redis_->register_node(format_address(config_.udp_bind));
            } catch (const std::exception& e) {
                CREEK_LOG_ERROR(std::string("[runtime] redis init failed: ") + e.what());
                redis_.reset();
                return false;
            }
        }

        TightConfig tc;
        tc.id = config_.id;
        tc.role = LinkRole::Node;
        tc.bind = config_.udp_bind;
        tc.token = config_.token;
        tc.dead_timeout = std::chrono::milliseconds(30000);

        transport_ = std::make_unique<TightTransport>(tc);
        transport_->set_message_callback([this](const std::string& peer_id, Bytes payload) {
            on_message(peer_id, std::move(payload));
        });
        transport_->set_peer_callback([this](const PeerEvent& ev) {
            on_peer_event(ev);
        });
        if (!transport_->start()) {
            CREEK_LOG_ERROR("[runtime] tight transport start failed");
            transport_.reset();
            return false;
        }

        {
            std::lock_guard<std::mutex> lk(mutex_);
            for (const auto& peer : config_.peers) {
                if (peer.id.empty()) continue;
                known_node_peers_.insert(peer.id);
                transport_->connect(peer);
            }
        }

        metrics_store_ = std::make_shared<MetricsStore>(config_.metric_period);
        metrics_server_ = std::make_unique<MetricsHttpServer>(metrics_store_, config_.metrics_bind);
        if (!metrics_server_->start()) {
            CREEK_LOG_ERROR("[runtime] metrics http server start failed");
            metrics_server_.reset();
            transport_->stop();
            transport_.reset();
            return false;
        }

        running_.store(true);
        sync_thread_ = std::thread([this] { sync_loop(); });
        if (redis_) {
            redis_sync_thread_ = std::thread([this] { redis_sync_loop(); });
        }
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (sync_thread_.joinable()) sync_thread_.join();
        if (redis_sync_thread_.joinable()) redis_sync_thread_.join();
        if (metrics_server_) metrics_server_->stop();
        metrics_server_.reset();
        if (transport_) transport_->stop();
        transport_.reset();
        known_node_peers_.clear();
        leaves_.clear();
        leaf_endpoints_.clear();
        redis_.reset();
    }

private:
    void on_peer_event(const PeerEvent& ev) {
        std::lock_guard<std::mutex> lk(mutex_);
        CREEK_LOG_INFO(std::string("[runtime] on_peer_event id=") + ev.id + " state=" + std::to_string((int)ev.state));
        if (ev.role == LinkRole::Leaf) {
            if (ev.state == LinkState::Closed) {
                auto it = leaves_.find(ev.id);
                if (it != leaves_.end()) {
                    std::string leaf_id = it->first;
                    leaves_.erase(it);
                    revoke_leaf_locked(leaf_id);
                }
            } else {
                leaves_[ev.id] = SteadyClock::now();
            }
        } else {
            if (ev.state == LinkState::Closed) {
                known_node_peers_.erase(ev.id);
            } else {
                known_node_peers_.insert(ev.id);
            }
        }
    }

    void on_message(const std::string& peer_id, Bytes payload) {
        CREEK_LOG_DEBUG(std::string("[runtime] leaf on_message from=") + peer_id + " bytes=" + std::to_string(payload.size()));
        creek::v1::WireMessage msg;
        if (!parse_wire(payload, msg)) return;

        if (msg.has_directory()) {
            handle_directory(peer_id, msg.directory(), payload.size());
        } else if (msg.has_request()) {
            handle_request(peer_id, msg.request(), payload.size());
        } else if (msg.has_response()) {
            handle_response(peer_id, msg.response(), payload.size());
        }
    }

    void handle_directory(const std::string& peer_id,
                          const creek::v1::DirectorySnapshot& snap,
                          std::size_t raw_size) {
        bool from_leaf = false;
        bool from_node_peer = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto leaf_it = leaves_.find(peer_id);
            if (leaf_it != leaves_.end()) {
                from_leaf = true;
                leaf_it->second = SteadyClock::now();
            } else if (known_node_peers_.count(peer_id)) {
                from_node_peer = true;
            }
        }

        creek::v1::DirectorySnapshot to_merge = snap;
        std::unordered_set<std::string> new_leaf_eps;
        if (from_leaf) {
            std::lock_guard<std::mutex> lk(mutex_);
            for (auto& ep : *to_merge.mutable_endpoints()) {
                if (ep.owner_leaf() == peer_id) {
                    ep.set_owner_node(config_.id);
                    ep.set_version(++version_counter_);
                    ep.set_updated_ms(unix_millis());
                    new_leaf_eps.insert(ep.endpoint_id());
                }
            }
            leaf_endpoints_[peer_id] = std::move(new_leaf_eps);
        }

        {
            std::lock_guard<std::mutex> lk(mutex_);
            directory_.merge(to_merge);
            if (from_leaf) {
                broadcast_snapshot_locked();
            } else if (from_node_peer) {
                push_to_leaves_locked();
            }
        }

        if (from_leaf) {
            record_metric("node_to_node", "DirectorySnapshot", raw_size, 0, true);
        } else if (from_node_peer) {
            record_metric("node_to_node", "DirectorySnapshot", raw_size, 0, true);
        }
    }

    void handle_request(const std::string& peer_id,
                        const creek::v1::RoutedRequest& req,
                        std::size_t raw_size) {
        bool from_leaf = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto leaf_it = leaves_.find(peer_id);
            if (leaf_it != leaves_.end()) {
                leaf_it->second = SteadyClock::now();
                from_leaf = true;
            }
            route_request_locked(peer_id, req);
        }
        record_metric(from_leaf ? "leaf_to_node" : "node_to_node",
                      req.rpc_name(), raw_size, 0, true, request_metadata(req));
    }

    void handle_response(const std::string& peer_id,
                         const creek::v1::RoutedResponse& resp,
                         std::size_t raw_size) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            route_response_locked(peer_id, resp);
        }
        record_metric("node_to_node", "RoutedResponse", raw_size, 0, true);
    }

    void route_request_locked(const std::string& from_peer,
                               const creek::v1::RoutedRequest& req) {
        const std::string dest_node = req.destination_node();
        const std::string dest_leaf = req.destination_leaf();
        CREEK_LOG_INFO(std::string("[creek-node] route_request from=") + from_peer
                        + " dest=(node=" + dest_node + " leaf=" + dest_leaf
                        + ") ep=" + req.endpoint_id());
        if (dest_node.empty() || dest_node == config_.id) {
            auto leaf_it = leaves_.find(dest_leaf);
            if (leaf_it == leaves_.end()) {
                send_error_response_locked(req, "leaf_not_found");
                return;
            }
            creek::v1::RoutedRequest out = req;
            out.set_destination_node(config_.id);
            creek::v1::WireMessage wm;
            *wm.mutable_request() = out;
            Bytes payload = serialize_wire(wm);
            transport_->send(dest_leaf, payload);
            record_metric("node_to_leaf", "RoutedRequest", payload.size(), 0, true,
                          request_metadata(out));
            return;
        }

        if (req.hop_limit() == 0) {
            send_error_response_locked(req, "hop_limit_exceeded");
            return;
        }
        if (!known_node_peers_.count(dest_node)) {
            send_error_response_locked(req, "node_not_found");
            return;
        }
        creek::v1::RoutedRequest fwd = req;
        fwd.set_hop_limit(req.hop_limit() - 1);
        creek::v1::WireMessage wm;
        *wm.mutable_request() = fwd;
        Bytes payload = serialize_wire(wm);
        transport_->send(dest_node, payload);
        record_metric("node_to_node", "RoutedRequest", payload.size(), 0, true,
                      request_metadata(fwd));
    }

    void route_response_locked(const std::string& from_peer,
                                const creek::v1::RoutedResponse& resp) {
        const std::string dest_node = resp.destination_node();
        const std::string dest_leaf = resp.destination_leaf();
        if (dest_node.empty() || dest_node == config_.id) {
            auto leaf_it = leaves_.find(dest_leaf);
            if (leaf_it == leaves_.end()) return;
            creek::v1::RoutedResponse out = resp;
            out.set_destination_node(config_.id);
            creek::v1::WireMessage wm;
            *wm.mutable_response() = out;
            Bytes payload = serialize_wire(wm);
            transport_->send(dest_leaf, payload);
            record_metric("node_to_leaf", "RoutedResponse", payload.size(), 0, true);
            return;
        }
        if (resp.hop_limit() == 0) return;
        if (!known_node_peers_.count(dest_node)) return;
        creek::v1::RoutedResponse fwd = resp;
        fwd.set_hop_limit(resp.hop_limit() - 1);
        creek::v1::WireMessage wm;
        *wm.mutable_response() = fwd;
        Bytes payload = serialize_wire(wm);
        transport_->send(dest_node, payload);
        record_metric("node_to_node", "RoutedResponse", payload.size(), 0, true);
    }

    void send_error_response_locked(const creek::v1::RoutedRequest& req,
                                     const std::string& error) {
        creek::v1::RoutedResponse resp;
        resp.set_request_id(req.request_id());
        resp.set_origin_leaf(req.destination_leaf());
        resp.set_origin_node(req.destination_node());
        resp.set_destination_leaf(req.origin_leaf());
        resp.set_destination_node(req.origin_node());
        resp.set_status(-1);
        resp.set_error(error);
        if (req.hop_limit() > 0) {
            resp.set_hop_limit(req.hop_limit() - 1);
        } else {
            resp.set_hop_limit(0);
        }
        route_response_locked(std::string{}, resp);
    }

    void broadcast_snapshot_locked() {
        creek::v1::DirectorySnapshot snap = directory_.snapshot(config_.id);
        creek::v1::WireMessage wm;
        *wm.mutable_directory() = snap;
        Bytes payload = serialize_wire(wm);

        for (const auto& peer : config_.peers) {
            if (peer.id.empty()) continue;
            transport_->send(peer.id, payload);
            record_metric("node_to_node", "DirectorySnapshot", payload.size(), 0, true);
        }
        for (const auto& node_id : known_node_peers_) {
            if (node_id == config_.id) continue;
            bool skip = false;
            for (const auto& peer : config_.peers) {
                if (peer.id == node_id) { skip = true; break; }
            }
            if (!skip) {
                transport_->send(node_id, payload);
                record_metric("node_to_node", "DirectorySnapshot", payload.size(), 0, true);
            }
        }
        for (const auto& kv : leaves_) {
            transport_->send(kv.first, payload);
            record_metric("node_to_leaf", "DirectorySnapshot", payload.size(), 0, true);
        }
    }

    void push_to_leaves_locked() {
        creek::v1::DirectorySnapshot snap = directory_.snapshot(config_.id);
        creek::v1::WireMessage wm;
        *wm.mutable_directory() = snap;
        Bytes payload = serialize_wire(wm);
        for (const auto& kv : leaves_) {
            transport_->send(kv.first, payload);
            record_metric("node_to_leaf", "DirectorySnapshot", payload.size(), 0, true);
        }
    }

    void revoke_leaf_locked(const std::string& leaf_id) {
        creek::v1::DirectorySnapshot snap = directory_.snapshot(config_.id);
        bool changed = false;
        for (auto& ep : *snap.mutable_endpoints()) {
            if (ep.owner_leaf() == leaf_id) {
                ep.set_alive(false);
                ep.set_version(++version_counter_);
                ep.set_updated_ms(unix_millis());
                changed = true;
            }
        }
        if (changed) {
            directory_.merge(snap);
            leaf_endpoints_.erase(leaf_id);
            broadcast_snapshot_locked();
        } else {
            leaf_endpoints_.erase(leaf_id);
        }
    }

    void redis_sync_loop() {
        while (running_.load()) {
            if (redis_) {
                try {
                    auto nodes = redis_->fetch_nodes();
                    {
                        std::lock_guard<std::mutex> lk(mutex_);
                        for (const auto& [node_id, addr_str] : nodes) {
                            if (node_id == config_.id) continue;
                            if (known_node_peers_.count(node_id) == 0) {
                                auto addr = parse_address(addr_str);
                                if (addr) {
                                    known_node_peers_.insert(node_id);
                                    transport_->connect({node_id, *addr});
                                }
                            }
                        }
                    }
                    for (const auto& [node_id, _] : nodes) {
                        if (node_id == config_.id) continue;
                        auto leaves = redis_->fetch_leaves_for_node(node_id);
                        for (const auto& [leaf_id, leaf_addr] : leaves) {
                            std::lock_guard<std::mutex> lk(mutex_);
                            if (leaves_.count(leaf_id) == 0) {
                                auto addr = parse_address(leaf_addr);
                                if (addr) {
                                    leaves_[leaf_id] = SteadyClock::now();
                                    transport_->connect({leaf_id, *addr});
                                }
                            }
                        }
                    }
                } catch (...) {}
            }
            for (int i = 0; i < 10 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    void sync_loop() {
        auto interval = config_.sync_interval;
        if (interval.count() <= 0) interval = std::chrono::milliseconds(15000);
        auto next = SteadyClock::now() + interval;
        while (running_.load()) {
            auto now = SteadyClock::now();
            if (now < next) {
                auto sleep_for = std::min<std::chrono::milliseconds>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(next - now),
                    std::chrono::milliseconds(500));
                if (sleep_for.count() <= 0) sleep_for = std::chrono::milliseconds(10);
                std::this_thread::sleep_for(sleep_for);
                continue;
            }
            next = now + interval;
            {
                std::lock_guard<std::mutex> lk(mutex_);
                broadcast_snapshot_locked();
            }
        }
    }

    void record_metric(const std::string& direction, const std::string& rpc_name,
                       std::uint64_t bytes, std::uint64_t latency_us, bool success,
                       const Metadata& metadata = Metadata{}) {
        if (!metrics_store_) return;
        MetricEvent ev;
        ev.direction = direction;
        ev.rpc_name = rpc_name;
        ev.metadata = metadata;
        ev.bytes = bytes;
        ev.latency_us = latency_us;
        ev.success = success;
        metrics_store_->record(ev);
    }

    NodeConfig config_;
    std::unique_ptr<TightTransport> transport_;
    std::shared_ptr<MetricsStore> metrics_store_;
    std::unique_ptr<MetricsHttpServer> metrics_server_;
    std::unique_ptr<RedisClient> redis_;
    EndpointDirectory directory_;
    std::unordered_map<std::string, SteadyClock::time_point> leaves_;
    std::unordered_set<std::string> known_node_peers_;
    std::unordered_map<std::string, std::unordered_set<std::string>> leaf_endpoints_;
    std::uint64_t version_counter_{};
    std::atomic<bool> running_{false};
    std::thread sync_thread_;
    std::thread redis_sync_thread_;
    std::mutex mutex_;
};

class GreeterService final : public creek::v1::Greeter::Service {
public:
    explicit GreeterService(LeafRuntime::Impl* impl) : impl_(impl) {}
    ::grpc::Status SayHello(::grpc::ServerContext* context,
                            const ::creek::v1::HelloRequest* request,
                            ::creek::v1::HelloReply* response) override;
private:
    LeafRuntime::Impl* impl_;
};

class LeafControlService final : public creek::v1::LeafControl::Service {
public:
    explicit LeafControlService(LeafRuntime::Impl* impl) : impl_(impl) {}
    ::grpc::Status Register(::grpc::ServerContext* context,
                            const ::creek::v1::RegisterRequest* request,
                            ::creek::v1::RegisterReply* response) override;
    ::grpc::Status Heartbeat(::grpc::ServerContext* context,
                             const ::creek::v1::HeartbeatRequest* request,
                             ::creek::v1::HeartbeatReply* response) override;
    ::grpc::Status Deregister(::grpc::ServerContext* context,
                              const ::creek::v1::DeregisterRequest* request,
                              ::creek::v1::DeregisterReply* response) override;
private:
    LeafRuntime::Impl* impl_;
};

class AdminService final : public creek::v1::Admin::Service {
public:
    explicit AdminService(LeafRuntime::Impl* impl) : impl_(impl) {}
    ::grpc::Status Metrics(::grpc::ServerContext* context,
                           const ::creek::v1::MetricRequest* request,
                           ::creek::v1::MetricReply* response) override;
    ::grpc::Status SetStickyStrategy(::grpc::ServerContext* context,
                                     const ::creek::v1::StickyStrategyRequest* request,
                                     ::creek::v1::StickyStrategyReply* response) override;
    ::grpc::Status SetBreakerConfig(::grpc::ServerContext* context,
                                    const ::creek::v1::BreakerConfigRequest* request,
                                    ::creek::v1::BreakerConfigReply* response) override;
    ::grpc::Status PushWasmModule(::grpc::ServerContext* context,
                                  const ::creek::v1::PushWasmRequest* request,
                                  ::creek::v1::PushWasmReply* response) override;
    ::grpc::Status ListWasmModules(::grpc::ServerContext* context,
                                   const ::creek::v1::ListWasmRequest* request,
                                   ::creek::v1::ListWasmReply* response) override;
    ::grpc::Status UnloadWasmModule(::grpc::ServerContext* context,
                                    const ::creek::v1::UnloadWasmRequest* request,
                                    ::creek::v1::UnloadWasmReply* response) override;
private:
    LeafRuntime::Impl* impl_;
};

class LeafRuntime::Impl {
public:
    struct PendingResponse {
        std::mutex m;
        std::condition_variable cv;
        bool done{false};
        creek::v1::RoutedResponse response;
    };

    explicit Impl(LeafConfig config)
        : config_(std::move(config)),
          balancer_(4096, std::chrono::minutes(1)),
          version_counter_(unix_millis()) {}

    ~Impl() { stop(); }

    bool start() {
        if (running_.load()) return true;

        CREEK_LOG_INFO(std::string("[runtime] leaf start id=") + config_.id);

        if (config_.redis.port != 0) {
            try {
                redis_ = std::make_unique<RedisClient>(config_.redis, config_.id);
                std::unordered_map<std::string, std::string> nodes = redis_->fetch_nodes();
                if (nodes.empty()) {
                    throw std::runtime_error("redis: no nodes discovered for leaf registration");
                }
                std::string picked_node;
                if (!config_.parents.empty() && !config_.parents[0].id.empty()) {
                    auto it = nodes.find(config_.parents[0].id);
                    if (it == nodes.end()) {
                        throw std::runtime_error("redis: configured parent node " + config_.parents[0].id + " not registered");
                    }
                    picked_node = config_.parents[0].id;
                } else {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<std::size_t> dist(0, nodes.size() - 1);
                    auto it = nodes.begin();
                    std::advance(it, dist(gen));
                    picked_node = it->first;
                }
                redis_->register_leaf(format_address(config_.udp_bind), picked_node);
            } catch (const std::exception&) {
                redis_.reset();
                return false;
            }
        }

        TightConfig tc;
        tc.id = config_.id;
        tc.role = LinkRole::Leaf;
        tc.bind = config_.udp_bind;
        tc.token = config_.token;
        tc.dead_timeout = std::chrono::milliseconds(30000);

        transport_ = std::make_unique<TightTransport>(tc);
        transport_->set_message_callback([this](const std::string& peer_id, Bytes payload) {
            on_message(peer_id, std::move(payload));
        });
        transport_->set_peer_callback([this](const PeerEvent& ev) {
            on_peer_event(ev);
        });
        if (!transport_->start()) {
            CREEK_LOG_ERROR("[runtime] tight transport start failed");
            transport_.reset();
            return false;
        }
        CREEK_LOG_INFO("[runtime] tight transport started");
        for (const auto& parent : config_.parents) {
            if (!parent.id.empty()) {
                parent_ids_.insert(parent.id);
                transport_->connect(parent);
            }
        }

        metrics_store_ = std::make_shared<MetricsStore>(config_.metric_period);
        metrics_server_ = std::make_unique<MetricsHttpServer>(metrics_store_, config_.metrics_bind);
        CREEK_LOG_INFO(std::string("[runtime] starting metrics server on ") + format_address(config_.metrics_bind));
        if (!metrics_server_->start()) {
            CREEK_LOG_ERROR("[runtime] metrics server start failed");
            metrics_server_.reset();
            transport_->stop();
            transport_.reset();
            return false;
        }
        CREEK_LOG_INFO("[runtime] metrics server started");

        json_rpc_server_ = std::make_unique<JsonRpcHttpServer>(
            config_.json_bind,
            [this](std::string body, const JsonRpcHttpServer::HeaderMap& headers) {
                return handle_json_rpc(std::move(body), headers);
            });
        if (!json_rpc_server_->start()) {
            CREEK_LOG_ERROR("[runtime] json_rpc server start failed");
            json_rpc_server_.reset();
            metrics_server_->stop();
            metrics_server_.reset();
            transport_->stop();
            transport_.reset();
            return false;
        }
        CREEK_LOG_INFO("[runtime] json_rpc server started");

        greeter_service_ = std::make_unique<GreeterService>(this);
        leaf_control_service_ = std::make_unique<LeafControlService>(this);
        admin_service_ = std::make_unique<AdminService>(this);

        grpc::ServerBuilder builder;
        CREEK_LOG_INFO(std::string("[runtime] building gRPC server on ") + format_address(config_.grpc_bind));
        builder.AddListeningPort(format_address(config_.grpc_bind),
                                 grpc::InsecureServerCredentials());
        builder.RegisterService(greeter_service_.get());
        builder.RegisterService(leaf_control_service_.get());
        builder.RegisterService(admin_service_.get());
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        CREEK_LOG_INFO("[runtime] calling BuildAndStart...");
        grpc_server_ = builder.BuildAndStart();
        CREEK_LOG_INFO("[runtime] BuildAndStart returned");
        if (!grpc_server_) {
            CREEK_LOG_ERROR("[runtime] gRPC server BuildAndStart returned null");
            json_rpc_server_->stop();
            json_rpc_server_.reset();
            metrics_server_->stop();
            metrics_server_.reset();
            transport_->stop();
            transport_.reset();
            return false;
        }
        CREEK_LOG_INFO("[runtime] gRPC server started");

        running_.store(true);
        grpc_wait_thread_ = std::thread([this] {
            if (grpc_server_) grpc_server_->Wait();
        });
        heartbeat_thread_ = std::thread([this] { heartbeat_loop(); });
        sync_thread_ = std::thread([this] { sync_loop(); });
        if (redis_) {
            redis_sync_thread_ = std::thread([this] { redis_sync_loop(); });
        }
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (grpc_server_) {
            grpc_server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
        }
        if (grpc_wait_thread_.joinable()) grpc_wait_thread_.join();
        if (heartbeat_thread_.joinable()) heartbeat_thread_.join();
        if (sync_thread_.joinable()) sync_thread_.join();
        if (redis_sync_thread_.joinable()) redis_sync_thread_.join();
        if (json_rpc_server_) json_rpc_server_->stop();
        json_rpc_server_.reset();
        if (metrics_server_) metrics_server_->stop();
        metrics_server_.reset();
        if (transport_) transport_->stop();
        transport_.reset();
        grpc_server_.reset();
        greeter_service_.reset();
        leaf_control_service_.reset();
        admin_service_.reset();
        redis_.reset();

        std::lock_guard<std::mutex> lk(mutex_);
        for (auto& kv : pending_) {
            std::lock_guard<std::mutex> sl(kv.second->m);
            kv.second->done = true;
            kv.second->cv.notify_all();
        }
        pending_.clear();
        channels_.clear();
        local_endpoints_.clear();
    }

    void on_message(const std::string& peer_id, Bytes payload) {
        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (!parent_ids_.count(peer_id)) return;
        }
        creek::v1::WireMessage msg;
        if (!parse_wire(payload, msg)) return;

        if (msg.has_directory()) {
            handle_directory(msg.directory(), payload.size());
        } else if (msg.has_request()) {
            handle_inbound_request(msg.request(), payload.size());
        } else if (msg.has_response()) {
            handle_inbound_response(msg.response(), payload.size());
        }
    }

    void on_peer_event(const PeerEvent& ev) {
        std::lock_guard<std::mutex> lk(mutex_);
        CREEK_LOG_INFO(std::string("[runtime] leaf on_peer_event id=") + ev.id + " state=" + std::to_string((int)ev.state) +
                       " in_parents=" + std::to_string(parent_ids_.count(ev.id)));
        if (!parent_ids_.count(ev.id)) return;
        if (ev.state == LinkState::Online) {
            if (active_parent_.empty()) {
                active_parent_ = ev.id;
                all_parents_down_.store(false);
            }
        } else if (ev.state == LinkState::Closed) {
            if (ev.id == active_parent_) {
                active_parent_.clear();
                bool found = false;
                for (const auto& parent_id : parent_ids_) {
                    if (parent_id == ev.id) continue;
                    auto peers = transport_->peers();
                    for (const auto& p : peers) {
                        if (p.id == parent_id && p.state == LinkState::Online) {
                            active_parent_ = parent_id;
                            found = true;
                            break;
                        }
                    }
                    if (found) break;
                }
                if (!found) {
                    all_parents_down_.store(true);
                }
            }
        }
    }

    void handle_directory(const creek::v1::DirectorySnapshot& snap, std::size_t raw_size) {
        CREEK_LOG_DEBUG(std::string("[runtime] leaf handle_directory eps=") + std::to_string(snap.endpoints_size()));
        {
            std::lock_guard<std::mutex> lk(mutex_);
            directory_.merge(snap);
        }
        record_metric("leaf_to_node", "DirectorySnapshot", raw_size, 0, true);
    }

    void handle_inbound_request(const creek::v1::RoutedRequest& req, std::size_t raw_size) {
        CREEK_LOG_INFO(std::string("[creek-leaf] recv_routed rid=") + req.request_id()
                        + " from=(node=" + req.origin_node() + " leaf=" + req.origin_leaf()
                        + ") dest=(node=" + req.destination_node() + " leaf=" + req.destination_leaf()
                        + ") ep=" + req.endpoint_id());
        if (req.destination_leaf() != config_.id) {
            creek::v1::RoutedResponse resp = make_error_response(req, "wrong_destination_leaf");
            send_response_to_parent(resp);
            return;
        }

        auto start = SteadyClock::now();
        creek::v1::HelloRequest hello_req;
        if (!hello_req.ParseFromString(req.body())) {
            creek::v1::RoutedResponse resp = make_error_response(req, "bad_request_body");
            send_response_to_parent(resp);
            return;
        }

        std::optional<creek::v1::Endpoint> ep_opt;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            ep_opt = directory_.find(req.endpoint_id());
        }
        if (!ep_opt || !ep_opt->alive()) {
            creek::v1::RoutedResponse resp = make_error_response(req, "endpoint_not_found");
            send_response_to_parent(resp);
            return;
        }

        creek::v1::HelloReply hello_reply;
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + config_.backend_timeout);
        auto stub = get_stub(ep_opt->target());
        auto it_tp = req.metadata().find("traceparent");
        std::string tp = (it_tp != req.metadata().end()) ? it_tp->second : std::string();
        auto it_ts = req.metadata().find("tracestate");
        std::string ts = (it_ts != req.metadata().end()) ? it_ts->second : std::string();
        TraceSpan mesh_span = TraceContext::extract_or_create(tp, ts);
        TraceSpan backend_span = TraceContext::create_child(mesh_span);
        CREEK_LOG_DEBUG(std::string("[trace] leaf_backend: trace_id=")
                         + backend_span.trace_id + " span_id=" + backend_span.span_id
                         + " parent=" + backend_span.parent_span_id
                         + " ep=" + ep_opt->endpoint_id());

        grpc::Status status = stub->SayHello(&ctx, hello_req, &hello_reply);
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            SteadyClock::now() - start).count();
        record_metric("leaf_to_backend", req.rpc_name(),
                      static_cast<std::uint64_t>(req.body().size()),
                      static_cast<std::uint64_t>(latency), status.ok(),
                      request_metadata(req));

        creek::v1::RoutedResponse resp;
        resp.set_request_id(req.request_id());
        resp.set_origin_leaf(config_.id);
        {
            std::lock_guard<std::mutex> lk(mutex_);
            resp.set_origin_node(active_parent_.empty() ? (config_.parents.empty() ? std::string{} : config_.parents[0].id) : active_parent_);
        }
        resp.set_destination_leaf(req.origin_leaf());
        resp.set_destination_node(req.origin_node());
        resp.set_hop_limit(req.hop_limit() > 0 ? req.hop_limit() - 1 : 0);
        if (status.ok()) {
            resp.set_status(0);
            hello_reply.set_backend_id(ep_opt->endpoint_id());
            resp.set_body(hello_reply.SerializeAsString());
        } else {
            resp.set_status(static_cast<std::int32_t>(status.error_code()));
            resp.set_error(status.error_message());
        }
        send_response_to_parent(resp);
        (void)raw_size;
    }

    void handle_inbound_response(const creek::v1::RoutedResponse& resp, std::size_t raw_size) {
        std::shared_ptr<PendingResponse> slot;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = pending_.find(resp.request_id());
            if (it == pending_.end()) return;
            slot = it->second;
            pending_.erase(it);
        }
        {
            std::lock_guard<std::mutex> sl(slot->m);
            slot->response = resp;
            slot->done = true;
        }
        slot->cv.notify_all();
        (void)raw_size;
    }

    creek::v1::RoutedResponse make_error_response(const creek::v1::RoutedRequest& req,
                                                  const std::string& error) {
        creek::v1::RoutedResponse resp;
        resp.set_request_id(req.request_id());
        resp.set_origin_leaf(config_.id);
        {
            std::lock_guard<std::mutex> lk(mutex_);
            resp.set_origin_node(active_parent_.empty() ? (config_.parents.empty() ? std::string{} : config_.parents[0].id) : active_parent_);
        }
        resp.set_destination_leaf(req.origin_leaf());
        resp.set_destination_node(req.origin_node());
        resp.set_status(-1);
        resp.set_error(error);
        resp.set_hop_limit(req.hop_limit() > 0 ? req.hop_limit() - 1 : 0);
        return resp;
    }

    void send_response_to_parent(const creek::v1::RoutedResponse& resp) {
        creek::v1::WireMessage wm;
        *wm.mutable_response() = resp;
        Bytes payload = serialize_wire(wm);
        std::string target;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            target = active_parent_;
        }
        if (!target.empty()) {
            transport_->send(target, payload);
        }
        record_metric("leaf_to_node", "RoutedResponse", payload.size(), 0, true);
    }

    grpc::Status call_backend(const creek::v1::Endpoint& ep,
                              const creek::v1::HelloRequest& req,
                              const Metadata& metadata,
                              creek::v1::HelloReply* reply,
                              std::uint64_t* latency_us,
                              std::size_t* out_bytes) {
        auto stub = get_stub(ep.target());
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + config_.backend_timeout);
        auto start = SteadyClock::now();
        grpc::Status status = stub->SayHello(&ctx, req, reply);
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            SteadyClock::now() - start).count();
        if (latency_us) *latency_us = static_cast<std::uint64_t>(latency);
        if (out_bytes) *out_bytes = static_cast<std::size_t>(req.ByteSizeLong());
        reply->set_backend_id(ep.endpoint_id());
        record_metric("leaf_to_backend", "SayHello",
                      static_cast<std::uint64_t>(req.ByteSizeLong()),
                      static_cast<std::uint64_t>(latency), status.ok(), metadata);
        return status;
    }

    std::unique_ptr<creek::v1::Greeter::Stub> get_stub(const std::string& target) {
        std::shared_ptr<grpc::Channel> channel;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = channels_.find(target);
            if (it == channels_.end()) {
                channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
                channels_[target] = channel;
            } else {
                channel = it->second;
            }
        }
        return creek::v1::Greeter::NewStub(channel);
    }

    grpc::Status send_routed_request(const creek::v1::Endpoint& ep,
                                     const creek::v1::HelloRequest& req,
                                     const Metadata& metadata,
                                     creek::v1::HelloReply* reply,
                                     std::uint64_t* latency_us) {
        creek::v1::RoutedRequest out;
        out.set_request_id(random_id());
        out.set_origin_leaf(config_.id);
        {
            std::lock_guard<std::mutex> lk(mutex_);
            out.set_origin_node(active_parent_.empty() ? (config_.parents.empty() ? std::string{} : config_.parents[0].id) : active_parent_);
        }
        out.set_destination_leaf(ep.owner_leaf());
        out.set_destination_node(ep.owner_node());
        out.set_endpoint_id(ep.endpoint_id());
        out.set_rpc_name("SayHello");
        for (const auto& entry : metadata) {
            (*out.mutable_metadata())[entry.first] = entry.second;
        }
        out.set_body(req.SerializeAsString());
        out.set_deadline_ms(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()) +
            static_cast<std::uint64_t>(config_.rpc_timeout.count()));
        out.set_hop_limit(kDefaultBackendHopLimit);

        auto slot = std::make_shared<PendingResponse>();
        {
            std::lock_guard<std::mutex> lk(mutex_);
            pending_[out.request_id()] = slot;
        }

        creek::v1::WireMessage wm;
        *wm.mutable_request() = out;
        Bytes payload = serialize_wire(wm);
        auto send_start = SteadyClock::now();
        bool sent = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            if (!active_parent_.empty()) {
                sent = transport_->send_priority(active_parent_, payload, 1);
            }
            if (!sent) {
                for (const auto& pid : parent_ids_) {
                    if (pid == active_parent_) continue;
                    sent = transport_->send_priority(pid, payload, 1);
                    if (sent) break;
                }
            }
        }
        if (!sent) {
            std::string target_addr;
            std::string target_id;
            {
                std::lock_guard<std::mutex> lk(mutex_);
                if (peer_targets_.count(out.destination_node())) {
                    target_addr = peer_targets_[out.destination_node()];
                    target_id = out.destination_node();
                } else {
                    auto it = known_leaves_.find(out.destination_leaf());
                    if (it != known_leaves_.end()) {
                        target_addr = format_address(it->second);
                        target_id = out.destination_node();
                    }
                }
            }
            if (!target_addr.empty()) {
                auto parsed = parse_address(target_addr);
                if (parsed) {
                    transport_->connect({target_id, *parsed});
                    sent = transport_->send_priority(target_id, payload, 1);
                }
            }
        }
        CREEK_LOG_DEBUG(std::string("[creek-leaf] send_routed rid=")
                         + out.request_id()
                         + " dest_node=" + out.destination_node()
                         + " dest_leaf=" + out.destination_leaf()
                         + " ep=" + out.endpoint_id()
                         + (sent ? " sent=1" : " sent=0"));
        record_metric("leaf_to_node", "RoutedRequest", payload.size(), 0, true, metadata);

        std::unique_lock<std::mutex> sl(slot->m);
        bool ok = slot->cv.wait_for(sl, config_.rpc_timeout, [&] { return slot->done; });
        if (latency_us) {
            *latency_us = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    SteadyClock::now() - send_start).count());
        }
        if (!ok) {
            std::lock_guard<std::mutex> lk(mutex_);
            pending_.erase(out.request_id());
            return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "rpc_timeout");
        }

        const creek::v1::RoutedResponse& resp = slot->response;
        if (resp.status() == 0) {
            if (!reply->ParseFromString(resp.body())) {
                return grpc::Status(grpc::StatusCode::INTERNAL, "bad_reply_body");
            }
            reply->set_backend_id(ep.endpoint_id());
            return grpc::Status::OK;
        }
        grpc::StatusCode code = resp.status() > 0
            ? static_cast<grpc::StatusCode>(resp.status())
            : grpc::StatusCode::UNAVAILABLE;
        return grpc::Status(code, resp.error());
    }

    std::pair<int, std::string> handle_json_rpc(std::string body,
                                                  const JsonRpcHttpServer::HeaderMap& headers) {
        nlohmann::json request;
        try {
            request = nlohmann::json::parse(body);
        } catch (const std::exception& error) {
            nlohmann::json error_body = {
                {"jsonrpc", "2.0"},
                {"error", {{"code", -32700}, {"message", std::string("parse error: ") + error.what()}}},
            };
            return {200, error_body.dump()};
        }
        std::string id = request.contains("id") ? request["id"].dump() : std::string("null");
        if (!request.contains("method") || !request["method"].is_string()) {
            nlohmann::json error_body = {
                {"jsonrpc", "2.0"},
                {"id", id},
                {"error", {{"code", -32600}, {"message", "invalid request"}}},
            };
            return {200, error_body.dump()};
        }
        std::string method = request["method"].get<std::string>();
        if (method != "SayHello" && method != "creek.SayHello") {
            nlohmann::json error_body = {
                {"jsonrpc", "2.0"},
                {"id", id},
                {"error", {{"code", -32601}, {"message", "method not found"}}},
            };
            return {200, error_body.dump()};
        }

        nlohmann::json params = request.value("params", nlohmann::json::object());
        std::string name = params.value("name", std::string{});
        std::string sid_value = params.value("sid", std::string{});
        bool sticky = true;
        Metadata metadata;
        if (params.contains("sticky") && params["sticky"].is_boolean()) {
            sticky = params["sticky"].get<bool>();
        }
        if (request.contains("metadata") && request["metadata"].is_object()) {
            for (auto it = request["metadata"].begin(); it != request["metadata"].end(); ++it) {
                const std::string& key = it.key();
                if (key == "sticky" && it->is_string()) {
                    auto v = it->get<std::string>();
                    sticky = (v == "true" || v == "1");
                }
                if (key == "sid" && it->is_string()) {
                    sid_value = it->get<std::string>();
                }
                if ((key == "shard_key" || key == "tenant_id") && it->is_string()) {
                    metadata["shard_key"] = it->get<std::string>();
                }
            }
        }
        {
            auto hit = headers.find("x-creek-sticky");
            if (hit != headers.end()) {
                auto v = hit->second;
                sticky = (v == "true" || v == "1");
            }
            auto hit_sid = headers.find("x-creek-sid");
            if (hit_sid != headers.end() && !hit_sid->second.empty()) {
                sid_value = hit_sid->second;
            }
            auto hit_s = headers.find("sticky");
            if (hit_s != headers.end()) {
                auto v = hit_s->second;
                sticky = (v == "true" || v == "1");
            }
            auto hit_id = headers.find("sid");
            if (hit_id != headers.end() && !hit_id->second.empty()) {
                sid_value = hit_id->second;
            }
            auto hit_shard = headers.find("x-creek-shard");
            if (hit_shard == headers.end()) hit_shard = headers.find("shard_key");
            if (hit_shard == headers.end()) hit_shard = headers.find("tenant_id");
            if (hit_shard != headers.end() && !hit_shard->second.empty()) {
                metadata["shard_key"] = hit_shard->second;
            }
        }

        creek::v1::HelloRequest hello_req;
        hello_req.set_name(name);
        hello_req.set_sid(sid_value);
        hello_req.set_sticky(sticky);

        metadata["sid"] = sid_value;
        metadata["sticky"] = sticky ? "true" : "false";
        metadata["x-sid"] = sid_value;

        auto it_tp = headers.find("traceparent");
        std::string tp = (it_tp != headers.end()) ? it_tp->second : std::string();
        auto it_ts = headers.find("tracestate");
        std::string ts = (it_ts != headers.end()) ? it_ts->second : std::string();
        TraceSpan span = TraceContext::extract_or_create(tp, ts);
        TraceSpan child = TraceContext::create_child(span);
        metadata["traceparent"] = child.traceparent_swapped();
        if (!child.trace_state.empty()) metadata["tracestate"] = child.trace_state;

        creek::v1::HelloReply hello_reply;
        grpc::Status status = invoke_for_hello(hello_req, metadata, &hello_reply);
        nlohmann::json response_body;
        response_body["jsonrpc"] = "2.0";
        response_body["id"] = id;
        if (status.ok()) {
            nlohmann::json result = {
                {"message", hello_reply.message()},
                {"backend_id", hello_reply.backend_id()},
            };
            response_body["result"] = result;
        } else {
            response_body["error"] = {
                {"code", static_cast<int>(status.error_code())},
                {"message", status.error_message()},
            };
        }
        return {200, response_body.dump()};
    }

    grpc::Status invoke_for_hello(const creek::v1::HelloRequest& request,
                                  const Metadata& metadata,
                                  creek::v1::HelloReply* response) {
        auto start = SteadyClock::now();
        const std::string service_name = creek::v1::Greeter::service_full_name();
        std::vector<creek::v1::Endpoint> endpoints;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            endpoints = directory_.service(service_name);
        }
        if (endpoints.empty()) {
            record_metric("client_to_leaf", "SayHello",
                          static_cast<std::uint64_t>(request.ByteSizeLong()), 0, false, metadata);
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no_endpoint");
        }
        std::set<std::string> tried;
        grpc::Status last_status(grpc::StatusCode::UNAVAILABLE, "no_attempt");
        for (int attempt = 0; attempt < 2; ++attempt) {
            std::optional<creek::v1::Endpoint> ep_opt;
            {
                std::lock_guard<std::mutex> lk(mutex_);
                ep_opt = balancer_.pick(service_name, metadata, endpoints);
            }
            if (!ep_opt) break;
            if (!tried.insert(ep_opt->endpoint_id()).second) break;
            const creek::v1::Endpoint& ep = *ep_opt;
            if (!breaker_.allow(ep.endpoint_id())) {
                CREEK_LOG_WARN(std::string("[creek-leaf] circuit open ep=") + ep.endpoint_id());
                last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "circuit_open");
                continue;
            }
            grpc::Status status(grpc::StatusCode::UNAVAILABLE, "no_backend");
            std::uint64_t latency = 0;
            std::size_t out_bytes = 0;
            if (ep.owner_leaf() == config_.id) {
                status = call_backend(ep, request, metadata, response, &latency, &out_bytes);
            } else {
                status = send_routed_request(ep, request, metadata, response, &latency);
            }
            last_status = status;
            if (status.ok()) {
                breaker_.record_success(ep.endpoint_id(), latency);
                record_metric("client_to_leaf", "SayHello",
                              static_cast<std::uint64_t>(request.ByteSizeLong()), 0, true, metadata);
                return status;
            }
            if (status.error_code() != static_cast<int>(grpc::StatusCode::UNAVAILABLE) ||
                (status.error_message() != "no_endpoint" && status.error_message() != "no_backend" &&
                 status.error_message() != "leaf_not_found" && status.error_message() != "circuit_open")) {
                breaker_.record_failure(ep.endpoint_id());
            }
            {
                std::lock_guard<std::mutex> lk(mutex_);
                balancer_.invalidate(ep.endpoint_id());
            }
        }
        record_metric("client_to_leaf", "SayHello",
                      static_cast<std::uint64_t>(request.ByteSizeLong()), 0, false, metadata);
        return last_status;
    }

    grpc::Status handle_say_hello(::grpc::ServerContext* context,
                                   const ::creek::v1::HelloRequest* request,
                                   ::creek::v1::HelloReply* response) {
        Metadata metadata;
        for (auto it = context->client_metadata().begin();
             it != context->client_metadata().end(); ++it) {
            std::string key(it->first.data(), it->first.size());
            std::string val(it->second.data(), it->second.size());
            metadata[key] = val;
            if (key == "x-creek-shard" || key == "shard_key" || key == "tenant_id") {
                metadata["shard_key"] = val;
            }
        }
        metadata["sid"] = request->sid();
        metadata["sticky"] = request->sticky() ? "true" : "false";
        metadata["x-sid"] = request->sid();

        auto it_trace = metadata.find("traceparent");
        std::string tp = (it_trace != metadata.end()) ? it_trace->second : std::string();
        auto it_state = metadata.find("tracestate");
        std::string ts = (it_state != metadata.end()) ? it_state->second : std::string();
        TraceSpan span = TraceContext::extract_or_create(tp, ts);
        TraceSpan child = TraceContext::create_child(span);
        metadata["traceparent"] = child.traceparent_swapped();
        if (!child.trace_state.empty()) metadata["tracestate"] = child.trace_state;
        CREEK_LOG_DEBUG(std::string("[trace] gRPC entry: trace_id=")
                         + child.trace_id + " span_id=" + child.span_id
                         + " parent=" + child.parent_span_id);

        grpc::Status status = invoke_for_hello(*request, metadata, response);
        return status;
    }

    grpc::Status handle_register(::grpc::ServerContext* context,
                                  const ::creek::v1::RegisterRequest* request,
                                  ::creek::v1::RegisterReply* response) {
        (void)context;
        if (!request->has_endpoint() || request->endpoint().endpoint_id().empty() ||
            request->endpoint().service().empty() ||
            request->endpoint().target().empty()) {
            response->set_accepted(false);
            response->set_error("invalid_endpoint");
            return grpc::Status::OK;
        }

        creek::v1::Endpoint ep = request->endpoint();
        ep.set_owner_leaf(config_.id);
        {
            std::lock_guard<std::mutex> lk(mutex_);
            ep.set_owner_node(active_parent_.empty() ? (config_.parents.empty() ? std::string{} : config_.parents[0].id) : active_parent_);
        }
        if (ep.version() == 0) ep.set_version(++version_counter_);
        ep.set_alive(true);
        ep.set_updated_ms(unix_millis());

        {
            std::lock_guard<std::mutex> lk(mutex_);
            directory_.upsert_local(ep);
            local_endpoints_[ep.endpoint_id()] = LocalEndpoint{ep, SteadyClock::now()};
        }
        CREEK_LOG_DEBUG(std::string("[creek-leaf] register ep=")
                         + ep.endpoint_id() + " svc=" + ep.service()
                         + " target=" + ep.target());
        send_snapshot_to_parent();
        response->set_accepted(true);
        return grpc::Status::OK;
    }

    grpc::Status handle_heartbeat(::grpc::ServerContext* context,
                                   const ::creek::v1::HeartbeatRequest* request,
                                   ::creek::v1::HeartbeatReply* response) {
        (void)context;
        bool revived = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = local_endpoints_.find(request->endpoint_id());
            if (it == local_endpoints_.end()) {
                response->set_accepted(false);
                return grpc::Status::OK;
            }
            it->second.last_heartbeat = SteadyClock::now();
            if (!it->second.endpoint.alive()) {
                it->second.endpoint.set_alive(true);
                it->second.endpoint.set_version(++version_counter_);
                it->second.endpoint.set_updated_ms(unix_millis());
                directory_.upsert_local(it->second.endpoint);
                revived = true;
            }
        }
        if (revived) send_snapshot_to_parent();
        response->set_accepted(true);
        return grpc::Status::OK;
    }

    grpc::Status handle_deregister(::grpc::ServerContext* context,
                                    const ::creek::v1::DeregisterRequest* request,
                                    ::creek::v1::DeregisterReply* response) {
        (void)context;
        bool found = false;
        {
            std::lock_guard<std::mutex> lk(mutex_);
            auto it = local_endpoints_.find(request->endpoint_id());
            if (it != local_endpoints_.end()) {
                it->second.endpoint.set_alive(false);
                it->second.endpoint.set_version(++version_counter_);
                it->second.endpoint.set_updated_ms(unix_millis());
                directory_.upsert_local(it->second.endpoint);
                found = true;
            }
        }
        if (found) {
            send_snapshot_to_parent();
        }
        response->set_accepted(found);
        return grpc::Status::OK;
    }

    grpc::Status handle_metrics(::grpc::ServerContext* context,
                                 const ::creek::v1::MetricRequest* request,
                                 ::creek::v1::MetricReply* response) {
        (void)context;
        if (!metrics_store_) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no_store");
        *response = metrics_store_->protobuf_snapshot(request->previous_minute(), request->take());
        return grpc::Status::OK;
    }

    grpc::Status handle_set_sticky(::grpc::ServerContext* context,
                                   const ::creek::v1::StickyStrategyRequest* request,
                                   ::creek::v1::StickyStrategyReply* response) {
        (void)context;
        std::lock_guard<std::mutex> lk(mutex_);
        if (request->strategy() == 0) {
            balancer_.set_shard_key("");
        }
        if (request->ttl_ms() > 0) {
            balancer_.set_ttl(std::chrono::milliseconds(request->ttl_ms()));
        }
        response->set_accepted(true);
        return grpc::Status::OK;
    }

    grpc::Status handle_set_breaker(::grpc::ServerContext* context,
                                    const ::creek::v1::BreakerConfigRequest* request,
                                    ::creek::v1::BreakerConfigReply* response) {
        (void)context;
        std::lock_guard<std::mutex> lk(mutex_);
        if (!request->endpoint_id().empty()) {
            breaker_.reset(request->endpoint_id());
        } else {
            breaker_.reset_all();
        }
        response->set_accepted(true);
        return grpc::Status::OK;
    }

    grpc::Status handle_push_wasm(::grpc::ServerContext* context,
                                  const ::creek::v1::PushWasmRequest* request,
                                  ::creek::v1::PushWasmReply* response) {
        (void)context;
        try {
            Bytes wasm_bytes(request->wasm_bytes().begin(), request->wasm_bytes().end());
            uint32_t id = WasmRuntime::instance().load_module(wasm_bytes);
            std::lock_guard<std::mutex> lk(mutex_);
            loaded_wasm_ids_.push_back(id);
            response->set_accepted(true);
            response->set_module_id(id);
        } catch (const std::exception& e) {
            response->set_accepted(false);
            response->set_error(e.what());
        }
        return grpc::Status::OK;
    }

    grpc::Status handle_list_wasm(::grpc::ServerContext* context,
                                  const ::creek::v1::ListWasmRequest* request,
                                  ::creek::v1::ListWasmReply* response) {
        (void)context;
        (void)request;
        std::lock_guard<std::mutex> lk(mutex_);
        for (auto id : loaded_wasm_ids_) {
            auto* info = response->add_modules();
            info->set_module_id(id);
            info->set_name("wasm_" + std::to_string(id));
            info->set_size(0);
        }
        return grpc::Status::OK;
    }

    grpc::Status handle_unload_wasm(::grpc::ServerContext* context,
                                    const ::creek::v1::UnloadWasmRequest* request,
                                    ::creek::v1::UnloadWasmReply* response) {
        (void)context;
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = std::find(loaded_wasm_ids_.begin(), loaded_wasm_ids_.end(), request->module_id());
        if (it != loaded_wasm_ids_.end()) {
            loaded_wasm_ids_.erase(it);
            response->set_accepted(true);
        } else {
            response->set_accepted(false);
            response->set_error("module not found");
        }
        return grpc::Status::OK;
    }

    void redis_sync_loop() {
        while (running_.load()) {
            try {
                std::unordered_map<std::string, std::string> nodes = redis_->fetch_nodes();
                std::unordered_map<std::string, std::string> all_leaves;
                for (const auto& parent : config_.parents) {
                    if (parent.id.empty()) continue;
                    auto leaves = redis_->fetch_leaves_for_node(parent.id);
                    for (const auto& entry : leaves) {
                        all_leaves[entry.first] = entry.second;
                    }
                }
                for (const auto& [node_id, _] : nodes) {
                    auto per_node = redis_->fetch_leaves_for_node(node_id);
                    for (const auto& entry : per_node) {
                        all_leaves[entry.first] = entry.second;
                    }
                }
                {
                    std::lock_guard<std::mutex> lk(mutex_);
                    for (const auto& [node_id, addr] : nodes) {
                        if (node_id == config_.id) continue;
                        if (peer_targets_.count(node_id) == 0) {
                            transport_->connect({node_id, parse_address(addr).value()});
                        }
                        peer_targets_[node_id] = addr;
                    }
                    for (const auto& [leaf_id, addr] : all_leaves) {
                        if (leaf_id == config_.id) continue;
                        if (known_leaves_.count(leaf_id) == 0) {
                            auto parsed = parse_address(addr);
                            if (parsed) known_leaves_[leaf_id] = *parsed;
                        }
                    }
                }
            } catch (...) {
            }
            for (int i = 0; i < 10 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
    }

    void send_snapshot_to_parent() {
        creek::v1::DirectorySnapshot snap;
        snap.set_source_id(config_.id);
        snap.set_generated_ms(unix_millis());
        {
            std::lock_guard<std::mutex> lk(mutex_);
            for (auto& kv : local_endpoints_) {
                *snap.add_endpoints() = kv.second.endpoint;
            }
        }
        creek::v1::WireMessage wm;
        *wm.mutable_directory() = snap;
        Bytes payload = serialize_wire(wm);
        if (transport_) {
            for (const auto& pid : parent_ids_) {
                transport_->send(pid, payload);
            }
        }
        record_metric("leaf_to_node", "DirectorySnapshot", payload.size(), 0, true);
    }

    void heartbeat_loop() {
        auto next = SteadyClock::now() + std::chrono::seconds(1);
        while (running_.load()) {
            auto now = SteadyClock::now();
            if (now < next) {
                auto sleep_for = std::min<std::chrono::milliseconds>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(next - now),
                    std::chrono::milliseconds(200));
                if (sleep_for.count() <= 0) sleep_for = std::chrono::milliseconds(10);
                std::this_thread::sleep_for(sleep_for);
                continue;
            }
            next = now + std::chrono::seconds(1);
            std::vector<creek::v1::Endpoint> dead;
            {
                std::lock_guard<std::mutex> lk(mutex_);
                for (auto& kv : local_endpoints_) {
                    auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - kv.second.last_heartbeat).count();
                    if (age >= 3000 && kv.second.endpoint.alive()) {
                        kv.second.endpoint.set_alive(false);
                        kv.second.endpoint.set_version(++version_counter_);
                        kv.second.endpoint.set_updated_ms(unix_millis());
                        dead.push_back(kv.second.endpoint);
                    }
                }
                for (auto& ep : dead) {
                    directory_.upsert_local(ep);
                }
            }
            if (!dead.empty()) {
                send_snapshot_to_parent();
            }
        }
    }

    void sync_loop() {
        auto interval = config_.sync_interval;
        if (interval.count() <= 0) interval = std::chrono::milliseconds(15000);
        while (running_.load()) {
            auto remaining = interval;
            while (running_.load() && remaining.count() > 0) {
                auto slice = std::min(remaining, std::chrono::milliseconds(200));
                std::this_thread::sleep_for(slice);
                remaining -= slice;
            }
            if (running_.load()) {
                {
                    std::lock_guard<std::mutex> lk(mutex_);
                    if (active_parent_.empty()) {
                        auto peers = transport_->peers();
                        for (const auto& parent_id : parent_ids_) {
                            for (const auto& p : peers) {
                                if (p.id == parent_id && p.state == LinkState::Online) {
                                    active_parent_ = parent_id;
                                    all_parents_down_.store(false);
                                    break;
                                }
                            }
                            if (!active_parent_.empty()) break;
                        }
                    }
                }
                send_snapshot_to_parent();
            }
        }
    }

    void record_metric(const std::string& direction, const std::string& rpc_name,
                       std::uint64_t bytes, std::uint64_t latency_us, bool success,
                       const Metadata& metadata = Metadata{}) {
        if (!metrics_store_) return;
        MetricEvent ev;
        ev.direction = direction;
        ev.rpc_name = rpc_name;
        ev.metadata = metadata;
        ev.bytes = bytes;
        ev.latency_us = latency_us;
        ev.success = success;
        metrics_store_->record(ev);
    }

    friend class GreeterService;
    friend class LeafControlService;
    friend class AdminService;

    struct LocalEndpoint {
        creek::v1::Endpoint endpoint;
        SteadyClock::time_point last_heartbeat{};
    };

    LeafConfig config_;
    std::unique_ptr<TightTransport> transport_;
    std::shared_ptr<MetricsStore> metrics_store_;
    std::unique_ptr<MetricsHttpServer> metrics_server_;
    std::unique_ptr<RedisClient> redis_;
    std::unique_ptr<JsonRpcHttpServer> json_rpc_server_;
    EndpointDirectory directory_;
    StickyBalancer balancer_;
    CircuitBreaker breaker_;
    std::unique_ptr<grpc::Server> grpc_server_;
    std::unique_ptr<GreeterService> greeter_service_;
    std::unique_ptr<LeafControlService> leaf_control_service_;
    std::unique_ptr<AdminService> admin_service_;
    std::thread grpc_wait_thread_;
    std::thread heartbeat_thread_;
    std::thread sync_thread_;
    std::thread redis_sync_thread_;
    std::atomic<bool> running_{false};
    std::mutex mutex_;
    std::unordered_map<std::string, LocalEndpoint> local_endpoints_;
    std::unordered_map<std::string, std::shared_ptr<grpc::Channel>> channels_;
    std::unordered_map<std::string, std::shared_ptr<PendingResponse>> pending_;
    std::unordered_map<std::string, std::string> peer_targets_;
    std::unordered_map<std::string, creek::Address> known_leaves_;
    std::set<std::string> parent_ids_;
    std::string active_parent_;
    std::atomic<bool> all_parents_down_{true};
    std::vector<uint32_t> loaded_wasm_ids_;
    std::uint64_t version_counter_{};
};

::grpc::Status GreeterService::SayHello(::grpc::ServerContext* context,
                                        const ::creek::v1::HelloRequest* request,
                                        ::creek::v1::HelloReply* response) {
    return impl_->handle_say_hello(context, request, response);
}

::grpc::Status LeafControlService::Register(::grpc::ServerContext* context,
                                            const ::creek::v1::RegisterRequest* request,
                                            ::creek::v1::RegisterReply* response) {
    return impl_->handle_register(context, request, response);
}

::grpc::Status LeafControlService::Heartbeat(::grpc::ServerContext* context,
                                             const ::creek::v1::HeartbeatRequest* request,
                                             ::creek::v1::HeartbeatReply* response) {
    return impl_->handle_heartbeat(context, request, response);
}

::grpc::Status LeafControlService::Deregister(::grpc::ServerContext* context,
                                              const ::creek::v1::DeregisterRequest* request,
                                              ::creek::v1::DeregisterReply* response) {
    return impl_->handle_deregister(context, request, response);
}

::grpc::Status AdminService::Metrics(::grpc::ServerContext* context,
                                     const ::creek::v1::MetricRequest* request,
                                     ::creek::v1::MetricReply* response) {
    return impl_->handle_metrics(context, request, response);
}

::grpc::Status AdminService::SetStickyStrategy(::grpc::ServerContext* context,
                                               const ::creek::v1::StickyStrategyRequest* request,
                                               ::creek::v1::StickyStrategyReply* response) {
    return impl_->handle_set_sticky(context, request, response);
}

::grpc::Status AdminService::SetBreakerConfig(::grpc::ServerContext* context,
                                              const ::creek::v1::BreakerConfigRequest* request,
                                              ::creek::v1::BreakerConfigReply* response) {
    return impl_->handle_set_breaker(context, request, response);
}

::grpc::Status AdminService::PushWasmModule(::grpc::ServerContext* context,
                                            const ::creek::v1::PushWasmRequest* request,
                                            ::creek::v1::PushWasmReply* response) {
    return impl_->handle_push_wasm(context, request, response);
}

::grpc::Status AdminService::ListWasmModules(::grpc::ServerContext* context,
                                             const ::creek::v1::ListWasmRequest* request,
                                             ::creek::v1::ListWasmReply* response) {
    return impl_->handle_list_wasm(context, request, response);
}

::grpc::Status AdminService::UnloadWasmModule(::grpc::ServerContext* context,
                                              const ::creek::v1::UnloadWasmRequest* request,
                                              ::creek::v1::UnloadWasmReply* response) {
    return impl_->handle_unload_wasm(context, request, response);
}

NodeRuntime::NodeRuntime(NodeConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}
NodeRuntime::~NodeRuntime() = default;
bool NodeRuntime::start() { return impl_->start(); }
void NodeRuntime::stop() { impl_->stop(); }

LeafRuntime::LeafRuntime(LeafConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}
LeafRuntime::~LeafRuntime() = default;
bool LeafRuntime::start() { return impl_->start(); }
void LeafRuntime::stop() { impl_->stop(); }

}
