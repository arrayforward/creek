#include "leaf_runtime_impl.hpp"
#include "../runtime_wire.hpp"

#include "creek/logger.hpp"
#include "creek/trace_context.hpp"
#include "creek/wasm_runtime.hpp"

#include <grpcpp/client_context.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <optional>
#include <random>
#include <utility>

namespace creek {

LeafRuntime::Impl::Impl(LeafConfig config)
    : m_config(std::move(config)),
      m_balancer(4096, std::chrono::minutes(1)),
      m_version_counter(unix_millis()) {}

LeafRuntime::Impl::~Impl() { stop(); }

bool LeafRuntime::Impl::start() {
    if (m_running.load()) return true;

    CREEK_LOG_INFO(std::string("[runtime] leaf start id=") + m_config.id);

    if (m_config.redis.port != 0) {
        try {
            m_redis = std::make_unique<RedisClient>(m_config.redis, m_config.id);
            std::unordered_map<std::string, std::string> nodes = m_redis->fetch_nodes();
            if (nodes.empty()) {
                throw std::runtime_error("redis: no nodes discovered for leaf registration");
            }
            std::string picked_node;
            if (!m_config.parents.empty() && !m_config.parents[0].id.empty()) {
                auto it = nodes.find(m_config.parents[0].id);
                if (it == nodes.end()) {
                    throw std::runtime_error("redis: configured parent node " + m_config.parents[0].id + " not registered");
                }
                picked_node = m_config.parents[0].id;
            } else {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<std::size_t> dist(0, nodes.size() - 1);
                auto it = nodes.begin();
                std::advance(it, dist(gen));
                picked_node = it->first;
            }
            m_redis->register_leaf(format_address(m_config.udp_bind), picked_node);
        } catch (const std::exception&) {
            m_redis.reset();
            return false;
        }
    }

    TightConfig tc;
    tc.id = m_config.id;
    tc.role = LinkRole::Leaf;
    tc.bind = to_tight_address(m_config.udp_bind);
    tc.token = m_config.token;
    tc.heartbeat = std::chrono::milliseconds(100);
    tc.report_interval = std::chrono::milliseconds(100);
    tc.retransmit_timeout = std::chrono::milliseconds(100);
    tc.flush_interval = std::chrono::milliseconds(10);
    tc.dead_timeout = std::chrono::milliseconds(30000);
    tc.mtu = 1200;
    tc.queue_limit = 4096;
    tc.initial_bandwidth_bytes = 10ULL * 1024 * 1024;
    tc.max_message_bytes = 1024 * 1024;

    m_transport = std::make_unique<TightTransport>(tc);
    m_transport->set_message_callback([this](const std::string& peer_id, Bytes payload) {
        on_message(peer_id, std::move(payload));
    });
    m_transport->set_peer_callback([this](const PeerEvent& ev) {
        on_peer_event(ev);
    });
    if (!m_transport->start()) {
        CREEK_LOG_ERROR("[runtime] tight transport start failed");
        m_transport.reset();
        return false;
    }
    CREEK_LOG_INFO("[runtime] tight transport started");
    for (const auto& parent : m_config.parents) {
        if (!parent.id.empty()) {
            m_parent_ids.insert(parent.id);
            m_transport->connect(to_tight_peer(parent));
        }
    }

    m_metrics_store = std::make_shared<MetricsStore>(m_config.metric_period);
    m_metrics_server = std::make_unique<MetricsHttpServer>(m_metrics_store, m_config.metrics_bind);
    CREEK_LOG_INFO(std::string("[runtime] starting metrics server on ") + format_address(m_config.metrics_bind));
    if (!m_metrics_server->start()) {
        CREEK_LOG_ERROR("[runtime] metrics server start failed");
        m_metrics_server.reset();
        m_transport->stop();
        m_transport.reset();
        return false;
    }
    CREEK_LOG_INFO("[runtime] metrics server started");

    m_json_rpc_server = std::make_unique<JsonRpcHttpServer>(
        m_config.json_bind,
        [this](std::string body, const JsonRpcHttpServer::HeaderMap& headers) {
            return handle_json_rpc(std::move(body), headers);
        });
    if (!m_json_rpc_server->start()) {
        CREEK_LOG_ERROR("[runtime] json_rpc server start failed");
        m_json_rpc_server.reset();
        m_metrics_server->stop();
        m_metrics_server.reset();
        m_transport->stop();
        m_transport.reset();
        return false;
    }
    CREEK_LOG_INFO("[runtime] json_rpc server started");

    m_greeter_service = std::make_unique<GreeterService>(this);
    m_leaf_control_service = std::make_unique<LeafControlService>(this);
    m_admin_service = std::make_unique<AdminService>(this);

    grpc::ServerBuilder builder;
    CREEK_LOG_INFO(std::string("[runtime] building gRPC server on ") + format_address(m_config.grpc_bind));
    builder.AddListeningPort(format_address(m_config.grpc_bind),
                             grpc::InsecureServerCredentials());
    builder.RegisterService(m_greeter_service.get());
    builder.RegisterService(m_leaf_control_service.get());
    builder.RegisterService(m_admin_service.get());
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    CREEK_LOG_INFO("[runtime] calling BuildAndStart...");
    m_grpc_server = builder.BuildAndStart();
    CREEK_LOG_INFO("[runtime] BuildAndStart returned");
    if (!m_grpc_server) {
        CREEK_LOG_ERROR("[runtime] gRPC server BuildAndStart returned null");
        m_json_rpc_server->stop();
        m_json_rpc_server.reset();
        m_metrics_server->stop();
        m_metrics_server.reset();
        m_transport->stop();
        m_transport.reset();
        return false;
    }
    CREEK_LOG_INFO("[runtime] gRPC server started");

    m_running.store(true);
    m_grpc_wait_thread = std::thread([this] {
        if (m_grpc_server) m_grpc_server->Wait();
    });
    // A handful of workers is plenty: routed calls are infrequent and the
    // queue absorbs bursts. Each worker blocks on backend gRPC calls so
    // the transport receiver thread never has to.
    constexpr std::size_t kWorkerCount = 4;
    for (std::size_t i = 0; i < kWorkerCount; ++i) {
        m_worker_threads.emplace_back([this] { worker_loop(); });
    }
    if (m_framework) {
        m_heartbeat_task_id = m_framework->reactor().schedule_periodic(
            "leaf_heartbeat", [this] { do_heartbeat_work(); },
            std::chrono::seconds(1),
            framework::TaskPriority::High, false);
        auto interval = m_config.sync_interval;
        if (interval.count() <= 0) interval = std::chrono::milliseconds(15000);
        m_sync_task_id = m_framework->reactor().schedule_periodic(
            "leaf_sync", [this] { do_sync_work(); }, interval,
            framework::TaskPriority::Normal, false);
        if (m_redis) {
            m_redis_sync_task_id = m_framework->reactor().schedule_periodic(
                "leaf_redis_sync", [this] { do_redis_sync_work(); },
                std::chrono::milliseconds(1000),
                framework::TaskPriority::Normal, false);
        }
    }
    return true;
}

void LeafRuntime::Impl::stop() {
    if (!m_running.exchange(false)) return;
    m_request_queue.close();
    for (auto& t : m_worker_threads) {
        if (t.joinable()) t.join();
    }
    m_worker_threads.clear();
    if (m_grpc_server) {
        m_grpc_server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    }
    if (m_grpc_wait_thread.joinable()) m_grpc_wait_thread.join();
    if (m_framework) {
        if (m_heartbeat_task_id != 0) {
            m_framework->reactor().cancel_periodic(m_heartbeat_task_id);
            m_heartbeat_task_id = 0;
        }
        if (m_sync_task_id != 0) {
            m_framework->reactor().cancel_periodic(m_sync_task_id);
            m_sync_task_id = 0;
        }
        if (m_redis_sync_task_id != 0) {
            m_framework->reactor().cancel_periodic(m_redis_sync_task_id);
            m_redis_sync_task_id = 0;
        }
    }
    if (m_json_rpc_server) m_json_rpc_server->stop();
    m_json_rpc_server.reset();
    if (m_metrics_server) m_metrics_server->stop();
    m_metrics_server.reset();
    if (m_transport) m_transport->stop();
    m_transport.reset();
    m_grpc_server.reset();
    m_greeter_service.reset();
    m_leaf_control_service.reset();
    m_admin_service.reset();
    m_redis.reset();

    std::lock_guard<std::mutex> lk(m_mutex);
    for (auto& kv : m_pending) {
        std::lock_guard<std::mutex> sl(kv.second->m);
        kv.second->done = true;
        kv.second->cv.notify_all();
    }
    m_pending.clear();
    m_channels.clear();
    m_local_endpoints.clear();
}

void LeafRuntime::Impl::on_message(const std::string& peer_id, Bytes payload) {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (!m_parent_ids.count(peer_id)) return;
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

void LeafRuntime::Impl::on_peer_event(const PeerEvent& ev) {
    std::lock_guard<std::mutex> lk(m_mutex);
    CREEK_LOG_INFO(std::string("[runtime] leaf on_peer_event id=") + ev.id + " state=" + std::to_string((int)ev.state) +
                   " in_parents=" + std::to_string(m_parent_ids.count(ev.id)));
    if (!m_parent_ids.count(ev.id)) return;
    if (ev.state == LinkState::Online) {
        if (m_active_parent.empty()) {
            m_active_parent = ev.id;
            m_all_parents_down.store(false);
        }
    } else if (ev.state == LinkState::Closed) {
        if (ev.id == m_active_parent) {
            m_active_parent.clear();
            bool found = false;
            for (const auto& parent_id : m_parent_ids) {
                if (parent_id == ev.id) continue;
                auto peers = m_transport->peers();
                for (const auto& p : peers) {
                    if (p.id == parent_id && p.state == LinkState::Online) {
                        m_active_parent = parent_id;
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
            if (!found) {
                m_all_parents_down.store(true);
            }
        }
    }
}

void LeafRuntime::Impl::handle_directory(const creek::v1::DirectorySnapshot& snap, std::size_t raw_size) {
    CREEK_LOG_DEBUG(std::string("[runtime] leaf handle_directory eps=") + std::to_string(snap.endpoints_size()));
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_directory.merge(snap);
    }
    record_metric("leaf_to_node", "DirectorySnapshot", raw_size, 0, true);
}

void LeafRuntime::Impl::worker_loop() {
    while (m_running.load()) {
        auto task = m_request_queue.take_for(std::chrono::milliseconds(100));
        if (!task) continue;
        try {
            (*task)();
        } catch (...) {}
    }
    // Drain remaining tasks on shutdown.
    while (auto task = m_request_queue.poll()) {
        try { (*task)(); } catch (...) {}
    }
}

void LeafRuntime::Impl::handle_inbound_request(const creek::v1::RoutedRequest& req, std::size_t raw_size) {
    // This runs on the tight transport's single receiver thread. The
    // backend SayHello call in process_inbound_request blocks (up to
    // backend_timeout), so doing it here would stall every peer's
    // acks/heartbeats/directories and cause mesh-wide UDP loss. Offload it
    // to a worker and return immediately.
    if (!m_request_queue.try_push([this, req, raw_size] {
            process_inbound_request(req, raw_size);
        })) {
        creek::v1::RoutedResponse resp = make_error_response(req, "overloaded");
        send_response_to_parent(resp);
    }
}

void LeafRuntime::Impl::process_inbound_request(const creek::v1::RoutedRequest& req, std::size_t raw_size) {
    CREEK_LOG_INFO(std::string("[creek-leaf] recv_routed rid=") + req.request_id()
                    + " from=(node=" + req.origin_node() + " leaf=" + req.origin_leaf()
                    + ") dest=(node=" + req.destination_node() + " leaf=" + req.destination_leaf()
                    + ") ep=" + req.endpoint_id());
    if (req.destination_leaf() != m_config.id) {
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
        std::lock_guard<std::mutex> lk(m_mutex);
        ep_opt = m_directory.find(req.endpoint_id());
    }
    if (!ep_opt || !ep_opt->alive()) {
        creek::v1::RoutedResponse resp = make_error_response(req, "endpoint_not_found");
        send_response_to_parent(resp);
        return;
    }

    creek::v1::HelloReply hello_reply;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + m_config.backend_timeout);
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
    resp.set_origin_leaf(m_config.id);
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        resp.set_origin_node(m_active_parent.empty() ? (m_config.parents.empty() ? std::string{} : m_config.parents[0].id) : m_active_parent);
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

void LeafRuntime::Impl::handle_inbound_response(const creek::v1::RoutedResponse& resp, std::size_t raw_size) {
    std::shared_ptr<PendingResponse> slot;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_pending.find(resp.request_id());
        if (it == m_pending.end()) return;
        slot = it->second;
        m_pending.erase(it);
    }
    {
        std::lock_guard<std::mutex> sl(slot->m);
        slot->response = resp;
        slot->done = true;
    }
    slot->cv.notify_all();
    (void)raw_size;
}

creek::v1::RoutedResponse LeafRuntime::Impl::make_error_response(const creek::v1::RoutedRequest& req,
                                                                 const std::string& error) {
    creek::v1::RoutedResponse resp;
    resp.set_request_id(req.request_id());
    resp.set_origin_leaf(m_config.id);
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        resp.set_origin_node(m_active_parent.empty() ? (m_config.parents.empty() ? std::string{} : m_config.parents[0].id) : m_active_parent);
    }
    resp.set_destination_leaf(req.origin_leaf());
    resp.set_destination_node(req.origin_node());
    resp.set_status(-1);
    resp.set_error(error);
    resp.set_hop_limit(req.hop_limit() > 0 ? req.hop_limit() - 1 : 0);
    return resp;
}

void LeafRuntime::Impl::send_response_to_parent(const creek::v1::RoutedResponse& resp) {
    creek::v1::WireMessage wm;
    *wm.mutable_response() = resp;
    Bytes payload = serialize_wire(wm);
    std::string target;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        target = m_active_parent;
    }
    if (!target.empty()) {
        m_transport->send(target, payload);
    }
    record_metric("leaf_to_node", "RoutedResponse", payload.size(), 0, true);
}

grpc::Status LeafRuntime::Impl::call_backend(const creek::v1::Endpoint& ep,
                                             const creek::v1::HelloRequest& req,
                                             const Metadata& metadata,
                                             creek::v1::HelloReply* reply,
                                             std::uint64_t* latency_us,
                                             std::size_t* out_bytes) {
    auto stub = get_stub(ep.target());
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + m_config.backend_timeout);
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

std::unique_ptr<creek::v1::Greeter::Stub> LeafRuntime::Impl::get_stub(const std::string& target) {
    std::shared_ptr<grpc::Channel> channel;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_channels.find(target);
        if (it == m_channels.end()) {
            channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
            m_channels[target] = channel;
        } else {
            channel = it->second;
        }
    }
    return creek::v1::Greeter::NewStub(channel);
}

grpc::Status LeafRuntime::Impl::send_routed_request(const creek::v1::Endpoint& ep,
                                                    const creek::v1::HelloRequest& req,
                                                    const Metadata& metadata,
                                                    creek::v1::HelloReply* reply,
                                                    std::uint64_t* latency_us) {
    creek::v1::RoutedRequest out;
    out.set_request_id(random_id());
    out.set_origin_leaf(m_config.id);
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        out.set_origin_node(m_active_parent.empty() ? (m_config.parents.empty() ? std::string{} : m_config.parents[0].id) : m_active_parent);
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
        static_cast<std::uint64_t>(m_config.rpc_timeout.count()));
    out.set_hop_limit(kDefaultBackendHopLimit);

    auto slot = std::make_shared<PendingResponse>();
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_pending[out.request_id()] = slot;
    }

    creek::v1::WireMessage wm;
    *wm.mutable_request() = out;
    Bytes payload = serialize_wire(wm);
    auto send_start = SteadyClock::now();
    bool sent = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (!m_active_parent.empty()) {
            sent = m_transport->send_priority(m_active_parent, payload, 1);
        }
        if (!sent) {
            for (const auto& pid : m_parent_ids) {
                if (pid == m_active_parent) continue;
                sent = m_transport->send_priority(pid, payload, 1);
                if (sent) break;
            }
        }
    }
    if (!sent) {
        std::string target_addr;
        std::string target_id;
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            if (m_peer_targets.count(out.destination_node())) {
                target_addr = m_peer_targets[out.destination_node()];
                target_id = out.destination_node();
            } else {
                auto it = m_known_leaves.find(out.destination_leaf());
                if (it != m_known_leaves.end()) {
                    target_addr = format_address(it->second);
                    target_id = out.destination_node();
                }
            }
        }
        if (!target_addr.empty()) {
            auto parsed = parse_address(target_addr);
            if (parsed) {
                m_transport->connect({target_id, to_tight_address(*parsed)});
                sent = m_transport->send_priority(target_id, payload, 1);
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
    bool ok = slot->cv.wait_for(sl, m_config.rpc_timeout, [&] { return slot->done; });
    if (latency_us) {
        *latency_us = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                SteadyClock::now() - send_start).count());
    }
    if (!ok) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_pending.erase(out.request_id());
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

std::pair<int, std::string> LeafRuntime::Impl::handle_json_rpc(std::string body,
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

grpc::Status LeafRuntime::Impl::invoke_for_hello(const creek::v1::HelloRequest& request,
                                                 const Metadata& metadata,
                                                 creek::v1::HelloReply* response) {
    auto start = SteadyClock::now();
    const std::string service_name = creek::v1::Greeter::service_full_name();
    std::vector<creek::v1::Endpoint> endpoints;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        endpoints = m_directory.service(service_name);
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
            std::lock_guard<std::mutex> lk(m_mutex);
            ep_opt = m_balancer.pick(service_name, metadata, endpoints);
        }
        if (!ep_opt) break;
        if (!tried.insert(ep_opt->endpoint_id()).second) break;
        const creek::v1::Endpoint& ep = *ep_opt;
        if (!m_breaker.allow(ep.endpoint_id())) {
            CREEK_LOG_WARN(std::string("[creek-leaf] circuit open ep=") + ep.endpoint_id());
            last_status = grpc::Status(grpc::StatusCode::UNAVAILABLE, "circuit_open");
            continue;
        }
        grpc::Status status(grpc::StatusCode::UNAVAILABLE, "no_backend");
        std::uint64_t latency = 0;
        std::size_t out_bytes = 0;
        if (ep.owner_leaf() == m_config.id) {
            status = call_backend(ep, request, metadata, response, &latency, &out_bytes);
        } else {
            status = send_routed_request(ep, request, metadata, response, &latency);
        }
        last_status = status;
        if (status.ok()) {
            m_breaker.record_success(ep.endpoint_id(), latency);
            record_metric("client_to_leaf", "SayHello",
                          static_cast<std::uint64_t>(request.ByteSizeLong()), 0, true, metadata);
            return status;
        }
        if (status.error_code() != static_cast<int>(grpc::StatusCode::UNAVAILABLE) ||
            (status.error_message() != "no_endpoint" && status.error_message() != "no_backend" &&
             status.error_message() != "leaf_not_found" && status.error_message() != "circuit_open")) {
            m_breaker.record_failure(ep.endpoint_id());
        }
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_balancer.invalidate(ep.endpoint_id());
        }
    }
    record_metric("client_to_leaf", "SayHello",
                  static_cast<std::uint64_t>(request.ByteSizeLong()), 0, false, metadata);
    return last_status;
}

grpc::Status LeafRuntime::Impl::handle_say_hello(::grpc::ServerContext* context,
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

grpc::Status LeafRuntime::Impl::handle_register(::grpc::ServerContext* context,
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
    ep.set_owner_leaf(m_config.id);
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        ep.set_owner_node(m_active_parent.empty() ? (m_config.parents.empty() ? std::string{} : m_config.parents[0].id) : m_active_parent);
    }
    if (ep.version() == 0) ep.set_version(++m_version_counter);
    ep.set_alive(true);
    ep.set_updated_ms(unix_millis());

    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_directory.upsert_local(ep);
        m_local_endpoints[ep.endpoint_id()] = LocalEndpoint{ep, SteadyClock::now()};
        CREEK_LOG_DEBUG(std::string("[creek-leaf] register local_eps_count=") + std::to_string(m_local_endpoints.size()));
    }
    CREEK_LOG_DEBUG(std::string("[creek-leaf] register ep=")
                     + ep.endpoint_id() + " svc=" + ep.service()
                     + " target=" + ep.target());
    send_snapshot_to_parent();
    response->set_accepted(true);
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_heartbeat(::grpc::ServerContext* context,
                                                 const ::creek::v1::HeartbeatRequest* request,
                                                 ::creek::v1::HeartbeatReply* response) {
    (void)context;
    bool revived = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_local_endpoints.find(request->endpoint_id());
        if (it == m_local_endpoints.end()) {
            response->set_accepted(false);
            return grpc::Status::OK;
        }
        it->second.last_heartbeat = SteadyClock::now();
        if (!it->second.endpoint.alive()) {
            it->second.endpoint.set_alive(true);
            it->second.endpoint.set_version(++m_version_counter);
            it->second.endpoint.set_updated_ms(unix_millis());
            m_directory.upsert_local(it->second.endpoint);
            revived = true;
        }
    }
    if (revived) send_snapshot_to_parent();
    response->set_accepted(true);
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_deregister(::grpc::ServerContext* context,
                                                  const ::creek::v1::DeregisterRequest* request,
                                                  ::creek::v1::DeregisterReply* response) {
    (void)context;
    bool found = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_local_endpoints.find(request->endpoint_id());
        if (it != m_local_endpoints.end()) {
            it->second.endpoint.set_alive(false);
            it->second.endpoint.set_version(++m_version_counter);
            it->second.endpoint.set_updated_ms(unix_millis());
            m_directory.upsert_local(it->second.endpoint);
            found = true;
        }
    }
    if (found) {
        send_snapshot_to_parent();
    }
    response->set_accepted(found);
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_metrics(::grpc::ServerContext* context,
                                               const ::creek::v1::MetricRequest* request,
                                               ::creek::v1::MetricReply* response) {
    (void)context;
    if (!m_metrics_store) return grpc::Status(grpc::StatusCode::UNAVAILABLE, "no_store");
    *response = m_metrics_store->protobuf_snapshot(request->previous_minute(), request->take());
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_set_sticky(::grpc::ServerContext* context,
                                                  const ::creek::v1::StickyStrategyRequest* request,
                                                  ::creek::v1::StickyStrategyReply* response) {
    (void)context;
    std::lock_guard<std::mutex> lk(m_mutex);
    if (request->strategy() == 0) {
        m_balancer.set_shard_key("");
    }
    if (request->ttl_ms() > 0) {
        m_balancer.set_ttl(std::chrono::milliseconds(request->ttl_ms()));
    }
    response->set_accepted(true);
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_set_breaker(::grpc::ServerContext* context,
                                                   const ::creek::v1::BreakerConfigRequest* request,
                                                   ::creek::v1::BreakerConfigReply* response) {
    (void)context;
    std::lock_guard<std::mutex> lk(m_mutex);
    if (!request->endpoint_id().empty()) {
        m_breaker.reset(request->endpoint_id());
    } else {
        m_breaker.reset_all();
    }
    response->set_accepted(true);
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_push_wasm(::grpc::ServerContext* context,
                                                 const ::creek::v1::PushWasmRequest* request,
                                                 ::creek::v1::PushWasmReply* response) {
    (void)context;
    try {
        Bytes wasm_bytes(request->wasm_bytes().begin(), request->wasm_bytes().end());
        uint32_t id = WasmRuntime::instance().load_module(wasm_bytes);
        std::lock_guard<std::mutex> lk(m_mutex);
        m_loaded_wasm_ids.push_back(id);
        response->set_accepted(true);
        response->set_module_id(id);
    } catch (const std::exception& e) {
        response->set_accepted(false);
        response->set_error(e.what());
    }
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_list_wasm(::grpc::ServerContext* context,
                                                 const ::creek::v1::ListWasmRequest* request,
                                                 ::creek::v1::ListWasmReply* response) {
    (void)context;
    (void)request;
    std::lock_guard<std::mutex> lk(m_mutex);
    for (auto id : m_loaded_wasm_ids) {
        auto* info = response->add_modules();
        info->set_module_id(id);
        info->set_name("wasm_" + std::to_string(id));
        info->set_size(0);
    }
    return grpc::Status::OK;
}

grpc::Status LeafRuntime::Impl::handle_unload_wasm(::grpc::ServerContext* context,
                                                   const ::creek::v1::UnloadWasmRequest* request,
                                                   ::creek::v1::UnloadWasmReply* response) {
    (void)context;
    std::lock_guard<std::mutex> lk(m_mutex);
    auto it = std::find(m_loaded_wasm_ids.begin(), m_loaded_wasm_ids.end(), request->module_id());
    if (it != m_loaded_wasm_ids.end()) {
        m_loaded_wasm_ids.erase(it);
        response->set_accepted(true);
    } else {
        response->set_accepted(false);
        response->set_error("module not found");
    }
    return grpc::Status::OK;
}

void LeafRuntime::Impl::send_snapshot_to_parent() {
    creek::v1::DirectorySnapshot snap;
    snap.set_source_id(m_config.id);
    snap.set_generated_ms(unix_millis());
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        for (auto& kv : m_local_endpoints) {
            *snap.add_endpoints() = kv.second.endpoint;
        }
    }
    creek::v1::WireMessage wm;
    *wm.mutable_directory() = snap;
    Bytes payload = serialize_wire(wm);
    CREEK_LOG_DEBUG(std::string("[runtime] send_snapshot_to_parent eps=") + std::to_string(snap.endpoints_size()) + " parents=" + std::to_string(m_parent_ids.size()));
    if (m_transport) {
        for (const auto& pid : m_parent_ids) {
            m_transport->send(pid, payload);
        }
    }
    record_metric("leaf_to_node", "DirectorySnapshot", payload.size(), 0, true);
}

void LeafRuntime::Impl::do_heartbeat_work() {
    auto now = SteadyClock::now();
    std::vector<creek::v1::Endpoint> dead;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        for (auto& kv : m_local_endpoints) {
            auto age = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - kv.second.last_heartbeat).count();
            if (age >= 3000 && kv.second.endpoint.alive()) {
                kv.second.endpoint.set_alive(false);
                kv.second.endpoint.set_version(++m_version_counter);
                kv.second.endpoint.set_updated_ms(unix_millis());
                dead.push_back(kv.second.endpoint);
            }
        }
        for (auto& ep : dead) {
            m_directory.upsert_local(ep);
        }
    }
    if (!dead.empty()) {
        send_snapshot_to_parent();
    }
}

void LeafRuntime::Impl::do_sync_work() {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (m_active_parent.empty()) {
            auto peers = m_transport->peers();
            for (const auto& parent_id : m_parent_ids) {
                for (const auto& p : peers) {
                    if (p.id == parent_id && p.state == LinkState::Online) {
                        m_active_parent = parent_id;
                        m_all_parents_down.store(false);
                        break;
                    }
                }
                if (!m_active_parent.empty()) break;
            }
        }
    }
    send_snapshot_to_parent();
}

void LeafRuntime::Impl::do_redis_sync_work() {
    if (!m_redis) return;
    try {
        auto nodes = m_redis->fetch_nodes();
        for (const auto& [node_id, addr_str] : nodes) {
            if (node_id == m_config.id) continue;
            auto leaves = m_redis->fetch_leaves_for_node(node_id);
            for (const auto& [leaf_id, leaf_addr] : leaves) {
                std::lock_guard<std::mutex> lk(m_mutex);
                if (m_known_leaves.count(leaf_id) == 0) {
                    auto parsed = parse_address(leaf_addr);
                    if (parsed) m_known_leaves[leaf_id] = *parsed;
                }
            }
        }
    } catch (...) {}
}

void LeafRuntime::Impl::record_metric(const std::string& direction, const std::string& rpc_name,
                                      std::uint64_t bytes, std::uint64_t latency_us, bool success,
                                      const Metadata& metadata) {
    if (!m_metrics_store) return;
    MetricEvent ev;
    ev.direction = direction;
    ev.rpc_name = rpc_name;
    ev.metadata = metadata;
    ev.bytes = bytes;
    ev.latency_us = latency_us;
    ev.success = success;
    m_metrics_store->record(ev);
}

void LeafRuntime::Impl::set_framework(framework::Framework* fw) {
    m_framework = fw;
    if (fw) {
        fw->set_batch_processor([this](const std::vector<framework::Message>& batch) {
            return process_batch(batch);
        });
    }
}

framework::ChangeSet LeafRuntime::Impl::process_batch(const std::vector<framework::Message>& batch) {
    framework::ChangeSet cs;
    for (const auto& msg : batch) {
        if (msg.kind == framework::MessageKind::UdpDatagram) {
            creek::v1::WireMessage wm;
            if (wm.ParseFromArray(msg.payload.data(), static_cast<int>(msg.payload.size()))) {
                if (wm.has_directory()) {
                    handle_directory(wm.directory(), msg.payload.size());
                } else if (wm.has_request()) {
                    handle_inbound_request(wm.request(), msg.payload.size());
                } else if (wm.has_response()) {
                    handle_inbound_response(wm.response(), msg.payload.size());
                }
            }
        } else if (msg.kind == framework::MessageKind::PeerEvent) {
            if (msg.payload.size() >= sizeof(PeerEvent)) {
                PeerEvent ev;
                std::memcpy(&ev, msg.payload.data(), sizeof(PeerEvent));
                on_peer_event(ev);
            }
        }
    }
    return cs;
}

}
