#include "node_runtime_impl.hpp"
#include "../runtime_wire.hpp"

#include "creek/logger.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <utility>

namespace creek {

NodeRuntime::Impl::Impl(NodeConfig config)
    : m_config(std::move(config)),
      m_version_counter(unix_millis()) {}

NodeRuntime::Impl::~Impl() { stop(); }

bool NodeRuntime::Impl::start() {
    if (m_running.load()) return true;

    CREEK_LOG_INFO(std::string("[runtime] node start id=") + m_config.id);

    if (m_config.redis.port != 0) {
        try {
            m_redis = std::make_unique<RedisClient>(m_config.redis, m_config.id);
            m_redis->register_node(format_address(m_config.udp_bind));
        } catch (const std::exception& e) {
            CREEK_LOG_ERROR(std::string("[runtime] redis init failed: ") + e.what());
            m_redis.reset();
            return false;
        }
    }

    TightConfig tc;
    tc.id = m_config.id;
    tc.role = LinkRole::Node;
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

    {
        std::lock_guard<std::mutex> lk(m_mutex);
        for (const auto& peer : m_config.peers) {
            if (peer.id.empty()) continue;
            m_known_node_peers.insert(peer.id);
            m_transport->connect(to_tight_peer(peer));
        }
    }

    m_metrics_store = std::make_shared<MetricsStore>(m_config.metric_period);
    m_metrics_server = std::make_unique<MetricsHttpServer>(m_metrics_store, m_config.metrics_bind);
    if (!m_metrics_server->start()) {
        CREEK_LOG_ERROR("[runtime] metrics http server start failed");
        m_metrics_server.reset();
        m_transport->stop();
        m_transport.reset();
        return false;
    }

    m_running.store(true);
    auto interval = m_config.sync_interval;
    if (interval.count() <= 0) interval = std::chrono::milliseconds(15000);
    m_sync_task_id = m_framework->reactor().schedule_periodic(
        "node_sync", [this] { do_sync_work(); }, interval,
        framework::TaskPriority::Normal, false);
    if (m_redis) {
        m_redis_sync_task_id = m_framework->reactor().schedule_periodic(
            "node_redis_sync", [this] { do_redis_sync_work(); },
            std::chrono::milliseconds(1000),
            framework::TaskPriority::Normal, false);
    }
    return true;
}

void NodeRuntime::Impl::stop() {
    if (!m_running.exchange(false)) return;
    if (m_sync_task_id != 0) {
        m_framework->reactor().cancel_periodic(m_sync_task_id);
        m_sync_task_id = 0;
    }
    if (m_redis_sync_task_id != 0) {
        m_framework->reactor().cancel_periodic(m_redis_sync_task_id);
        m_redis_sync_task_id = 0;
    }
    if (m_metrics_server) m_metrics_server->stop();
    m_metrics_server.reset();
    if (m_transport) m_transport->stop();
    m_transport.reset();
    m_known_node_peers.clear();
    m_leaves.clear();
    m_leaf_endpoints.clear();
    m_redis.reset();
}

void NodeRuntime::Impl::set_framework(framework::Framework* fw) {
    m_framework = fw;
    if (fw) {
        fw->set_batch_processor([this](const std::vector<framework::Message>& batch) {
            return process_batch(batch);
        });
    }
}

framework::ChangeSet NodeRuntime::Impl::process_batch(const std::vector<framework::Message>& batch) {
    framework::ChangeSet cs;
    for (const auto& msg : batch) {
        if (msg.kind == framework::MessageKind::UdpDatagram) {
            creek::v1::WireMessage wm;
            if (wm.ParseFromArray(msg.payload.data(), static_cast<int>(msg.payload.size()))) {
                if (wm.has_directory()) {
                    handle_directory(msg.source, wm.directory(), msg.payload.size());
                } else if (wm.has_request()) {
                    handle_request(msg.source, wm.request(), msg.payload.size());
                } else if (wm.has_response()) {
                    handle_response(msg.source, wm.response(), msg.payload.size());
                } else if (wm.has_stream_open()) {
                    handle_stream_open(msg.source, wm.stream_open(), msg.payload.size());
                } else if (wm.has_stream_frame()) {
                    handle_stream_frame(msg.source, wm.stream_frame(), msg.payload.size());
                } else if (wm.has_stream_close()) {
                    handle_stream_close(msg.source, wm.stream_close(), msg.payload.size());
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

void NodeRuntime::Impl::on_peer_event(const PeerEvent& ev) {
    std::lock_guard<std::mutex> lk(m_mutex);
    CREEK_LOG_INFO(std::string("[runtime] on_peer_event id=") + ev.id + " state=" + std::to_string((int)ev.state));
    if (ev.role == LinkRole::Leaf) {
        if (ev.state == LinkState::Closed) {
            auto it = m_leaves.find(ev.id);
            if (it != m_leaves.end()) {
                std::string leaf_id = it->first;
                m_leaves.erase(it);
                revoke_leaf_locked(leaf_id);
            }
        } else {
            m_leaves[ev.id] = SteadyClock::now();
        }
    } else {
        if (ev.state == LinkState::Closed) {
            m_known_node_peers.erase(ev.id);
        } else {
            m_known_node_peers.insert(ev.id);
        }
    }
}

void NodeRuntime::Impl::on_message(const std::string& peer_id, Bytes payload) {
    CREEK_LOG_DEBUG(std::string("[runtime] leaf on_message from=") + peer_id + " bytes=" + std::to_string(payload.size()));
    creek::v1::WireMessage msg;
    if (!parse_wire(payload, msg)) return;

    if (msg.has_directory()) {
        handle_directory(peer_id, msg.directory(), payload.size());
    } else if (msg.has_request()) {
        handle_request(peer_id, msg.request(), payload.size());
    } else if (msg.has_response()) {
        handle_response(peer_id, msg.response(), payload.size());
    } else if (msg.has_stream_open()) {
        handle_stream_open(peer_id, msg.stream_open(), payload.size());
    } else if (msg.has_stream_frame()) {
        handle_stream_frame(peer_id, msg.stream_frame(), payload.size());
    } else if (msg.has_stream_close()) {
        handle_stream_close(peer_id, msg.stream_close(), payload.size());
    }
}

void NodeRuntime::Impl::handle_directory(const std::string& peer_id,
                                         const creek::v1::DirectorySnapshot& snap,
                                         std::size_t raw_size) {
    bool from_leaf = false;
    bool from_node_peer = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto leaf_it = m_leaves.find(peer_id);
        if (leaf_it != m_leaves.end()) {
            from_leaf = true;
            leaf_it->second = SteadyClock::now();
        } else if (m_known_node_peers.count(peer_id)) {
            from_node_peer = true;
        }
        CREEK_LOG_DEBUG(std::string("[runtime] node handle_directory peer=") + peer_id +
                        " from_leaf=" + std::to_string(from_leaf) +
                        " from_node=" + std::to_string(from_node_peer) +
                        " eps=" + std::to_string(snap.endpoints_size()));
    }

    creek::v1::DirectorySnapshot to_merge = snap;
    std::unordered_set<std::string> new_leaf_eps;
    if (from_leaf) {
        std::lock_guard<std::mutex> lk(m_mutex);
        for (auto& ep : *to_merge.mutable_endpoints()) {
            if (ep.owner_leaf() == peer_id) {
                ep.set_owner_node(m_config.id);
                ep.set_version(++m_version_counter);
                ep.set_updated_ms(unix_millis());
                new_leaf_eps.insert(ep.endpoint_id());
            }
        }
        // The leaf's snapshot is authoritative for its own endpoints: ids
        // that vanished since the previous snapshot were removed on the
        // leaf (dead past the grace period / deregistered). Erase them here
        // with a fresh tombstone so our broadcasts propagate the removal
        // and delayed older snapshots cannot resurrect them.
        auto prev_it = m_leaf_endpoints.find(peer_id);
        if (prev_it != m_leaf_endpoints.end()) {
            for (const auto& old_id : prev_it->second) {
                if (new_leaf_eps.count(old_id) == 0) {
                    auto* removed = to_merge.add_removed_endpoints();
                    removed->set_endpoint_id(old_id);
                    removed->set_version(++m_version_counter);
                    removed->set_updated_ms(unix_millis());
                }
            }
        }
        m_leaf_endpoints[peer_id] = std::move(new_leaf_eps);
    }

    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_directory.merge(to_merge);
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

void NodeRuntime::Impl::handle_request(const std::string& peer_id,
                                       const creek::v1::RoutedRequest& req,
                                       std::size_t raw_size) {
    bool from_leaf = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto leaf_it = m_leaves.find(peer_id);
        if (leaf_it != m_leaves.end()) {
            leaf_it->second = SteadyClock::now();
            from_leaf = true;
        }
        route_request_locked(peer_id, req);
    }
    record_metric(from_leaf ? "leaf_to_node" : "node_to_node",
                  req.rpc_name(), raw_size, 0, true, request_metadata(req));
}

void NodeRuntime::Impl::handle_response(const std::string& peer_id,
                                        const creek::v1::RoutedResponse& resp,
                                        std::size_t raw_size) {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        route_response_locked(peer_id, resp);
    }
    record_metric("node_to_node", "RoutedResponse", raw_size, 0, true);
}

void NodeRuntime::Impl::route_request_locked(const std::string& from_peer,
                                             const creek::v1::RoutedRequest& req) {
    const std::string dest_node = req.destination_node();
    const std::string dest_leaf = req.destination_leaf();
    CREEK_LOG_INFO(std::string("[creek-node] route_request from=") + from_peer
                    + " dest=(node=" + dest_node + " leaf=" + dest_leaf
                    + ") ep=" + req.endpoint_id());
    if (dest_node.empty() || dest_node == m_config.id) {
        auto leaf_it = m_leaves.find(dest_leaf);
        if (leaf_it == m_leaves.end()) {
            send_error_response_locked(req, "leaf_not_found");
            return;
        }
        creek::v1::RoutedRequest out = req;
        out.set_destination_node(m_config.id);
        creek::v1::WireMessage wm;
        *wm.mutable_request() = out;
        Bytes payload = serialize_wire(wm);
        m_transport->send(dest_leaf, payload);
        record_metric("node_to_leaf", "RoutedRequest", payload.size(), 0, true,
                      request_metadata(out));
        return;
    }

    if (req.hop_limit() == 0) {
        send_error_response_locked(req, "hop_limit_exceeded");
        return;
    }
    if (!m_known_node_peers.count(dest_node)) {
        send_error_response_locked(req, "node_not_found");
        return;
    }
    creek::v1::RoutedRequest fwd = req;
    fwd.set_hop_limit(req.hop_limit() - 1);
    creek::v1::WireMessage wm;
    *wm.mutable_request() = fwd;
    Bytes payload = serialize_wire(wm);
    m_transport->send(dest_node, payload);
    record_metric("node_to_node", "RoutedRequest", payload.size(), 0, true,
                  request_metadata(fwd));
}

void NodeRuntime::Impl::handle_stream_open(const std::string& peer_id,
                                           const creek::v1::RoutedStreamOpen& open,
                                           std::size_t raw_size) {
    bool from_leaf = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto leaf_it = m_leaves.find(peer_id);
        if (leaf_it != m_leaves.end()) {
            leaf_it->second = SteadyClock::now();
            from_leaf = true;
        }
        const std::string dest_node = open.destination_node();
        const std::string dest_leaf = open.destination_leaf();
        if (dest_node.empty() || dest_node == m_config.id) {
            auto dest_it = m_leaves.find(dest_leaf);
            if (dest_it == m_leaves.end()) {
                send_stream_close_locked(open, "leaf_not_found");
            } else {
                creek::v1::RoutedStreamOpen fwd = open;
                fwd.set_destination_node(m_config.id);
                creek::v1::WireMessage wm;
                *wm.mutable_stream_open() = fwd;
                Bytes payload = serialize_wire(wm);
                m_transport->send(dest_leaf, payload);
                record_metric("node_to_leaf", "RoutedStreamOpen", payload.size(), 0, true);
            }
        } else {
            if (open.hop_limit() == 0) {
                send_stream_close_locked(open, "hop_limit_exceeded");
            } else if (!m_known_node_peers.count(dest_node)) {
                send_stream_close_locked(open, "node_not_found");
            } else {
                creek::v1::RoutedStreamOpen fwd = open;
                fwd.set_hop_limit(open.hop_limit() - 1);
                creek::v1::WireMessage wm;
                *wm.mutable_stream_open() = fwd;
                Bytes payload = serialize_wire(wm);
                m_transport->send(dest_node, payload);
                record_metric("node_to_node", "RoutedStreamOpen", payload.size(), 0, true);
            }
        }
    }
    record_metric(from_leaf ? "leaf_to_node" : "node_to_node",
                  open.rpc_name(), raw_size, 0, true);
}

void NodeRuntime::Impl::handle_stream_frame(const std::string& peer_id,
                                            const creek::v1::RoutedStreamFrame& frame,
                                            std::size_t raw_size) {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto leaf_it = m_leaves.find(peer_id);
        if (leaf_it != m_leaves.end()) {
            leaf_it->second = SteadyClock::now();
        }
        route_stream_frame_locked(frame);
    }
    record_metric("node_to_node", "RoutedStreamFrame", raw_size, 0, true);
}

void NodeRuntime::Impl::handle_stream_close(const std::string& peer_id,
                                            const creek::v1::RoutedStreamClose& close,
                                            std::size_t raw_size) {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto leaf_it = m_leaves.find(peer_id);
        if (leaf_it != m_leaves.end()) {
            leaf_it->second = SteadyClock::now();
        }
        route_stream_close_locked(close);
    }
    record_metric("node_to_node", "RoutedStreamClose", raw_size, 0, true);
}

void NodeRuntime::Impl::route_stream_frame_locked(const creek::v1::RoutedStreamFrame& frame) {
    const std::string dest_node = frame.destination_node();
    const std::string dest_leaf = frame.destination_leaf();
    if (dest_node.empty() || dest_node == m_config.id) {
        auto leaf_it = m_leaves.find(dest_leaf);
        if (leaf_it == m_leaves.end()) return;
        creek::v1::RoutedStreamFrame out = frame;
        out.set_destination_node(m_config.id);
        creek::v1::WireMessage wm;
        *wm.mutable_stream_frame() = out;
        Bytes payload = serialize_wire(wm);
        m_transport->send(dest_leaf, payload);
        record_metric("node_to_leaf", "RoutedStreamFrame", payload.size(), 0, true);
        return;
    }
    if (frame.hop_limit() == 0) return;
    if (!m_known_node_peers.count(dest_node)) return;
    creek::v1::RoutedStreamFrame fwd = frame;
    fwd.set_hop_limit(frame.hop_limit() - 1);
    creek::v1::WireMessage wm;
    *wm.mutable_stream_frame() = fwd;
    Bytes payload = serialize_wire(wm);
    m_transport->send(dest_node, payload);
    record_metric("node_to_node", "RoutedStreamFrame", payload.size(), 0, true);
}

void NodeRuntime::Impl::route_stream_close_locked(const creek::v1::RoutedStreamClose& close) {
    const std::string dest_node = close.destination_node();
    const std::string dest_leaf = close.destination_leaf();
    if (dest_node.empty() || dest_node == m_config.id) {
        auto leaf_it = m_leaves.find(dest_leaf);
        if (leaf_it == m_leaves.end()) return;
        creek::v1::RoutedStreamClose out = close;
        out.set_destination_node(m_config.id);
        creek::v1::WireMessage wm;
        *wm.mutable_stream_close() = out;
        Bytes payload = serialize_wire(wm);
        m_transport->send(dest_leaf, payload);
        record_metric("node_to_leaf", "RoutedStreamClose", payload.size(), 0, true);
        return;
    }
    if (close.hop_limit() == 0) return;
    if (!m_known_node_peers.count(dest_node)) return;
    creek::v1::RoutedStreamClose fwd = close;
    fwd.set_hop_limit(close.hop_limit() - 1);
    creek::v1::WireMessage wm;
    *wm.mutable_stream_close() = fwd;
    Bytes payload = serialize_wire(wm);
    m_transport->send(dest_node, payload);
    record_metric("node_to_node", "RoutedStreamClose", payload.size(), 0, true);
}

void NodeRuntime::Impl::send_stream_close_locked(const creek::v1::RoutedStreamOpen& open,
                                                 const std::string& error) {
    creek::v1::RoutedStreamClose close;
    close.set_request_id(open.request_id());
    close.set_status(-1);
    close.set_error(error);
    close.set_from_origin(false);
    close.set_origin_leaf(open.destination_leaf());
    close.set_origin_node(open.destination_node());
    close.set_destination_leaf(open.origin_leaf());
    close.set_destination_node(open.origin_node());
    close.set_hop_limit(open.hop_limit() > 0 ? open.hop_limit() - 1 : 0);
    route_stream_close_locked(close);
}

void NodeRuntime::Impl::route_response_locked(const std::string& from_peer,
                                              const creek::v1::RoutedResponse& resp) {
    (void)from_peer;
    const std::string dest_node = resp.destination_node();
    const std::string dest_leaf = resp.destination_leaf();
    if (dest_node.empty() || dest_node == m_config.id) {
        auto leaf_it = m_leaves.find(dest_leaf);
        if (leaf_it == m_leaves.end()) return;
        creek::v1::RoutedResponse out = resp;
        out.set_destination_node(m_config.id);
        creek::v1::WireMessage wm;
        *wm.mutable_response() = out;
        Bytes payload = serialize_wire(wm);
        m_transport->send(dest_leaf, payload);
        record_metric("node_to_leaf", "RoutedResponse", payload.size(), 0, true);
        return;
    }
    if (resp.hop_limit() == 0) return;
    if (!m_known_node_peers.count(dest_node)) return;
    creek::v1::RoutedResponse fwd = resp;
    fwd.set_hop_limit(resp.hop_limit() - 1);
    creek::v1::WireMessage wm;
    *wm.mutable_response() = fwd;
    Bytes payload = serialize_wire(wm);
    m_transport->send(dest_node, payload);
    record_metric("node_to_node", "RoutedResponse", payload.size(), 0, true);
}

void NodeRuntime::Impl::send_error_response_locked(const creek::v1::RoutedRequest& req,
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

void NodeRuntime::Impl::broadcast_snapshot_locked() {
    creek::v1::DirectorySnapshot snap = m_directory.snapshot(m_config.id);
    creek::v1::WireMessage wm;
    *wm.mutable_directory() = snap;
    Bytes payload = serialize_wire(wm);

    for (const auto& peer : m_config.peers) {
        if (peer.id.empty()) continue;
        m_transport->send(peer.id, payload);
        record_metric("node_to_node", "DirectorySnapshot", payload.size(), 0, true);
    }
    for (const auto& node_id : m_known_node_peers) {
        if (node_id == m_config.id) continue;
        bool skip = false;
        for (const auto& peer : m_config.peers) {
            if (peer.id == node_id) { skip = true; break; }
        }
        if (!skip) {
            m_transport->send(node_id, payload);
            record_metric("node_to_node", "DirectorySnapshot", payload.size(), 0, true);
        }
    }
    for (const auto& kv : m_leaves) {
        m_transport->send(kv.first, payload);
        record_metric("node_to_leaf", "DirectorySnapshot", payload.size(), 0, true);
    }
}

void NodeRuntime::Impl::push_to_leaves_locked() {
    creek::v1::DirectorySnapshot snap = m_directory.snapshot(m_config.id);
    creek::v1::WireMessage wm;
    *wm.mutable_directory() = snap;
    Bytes payload = serialize_wire(wm);
    for (const auto& kv : m_leaves) {
        m_transport->send(kv.first, payload);
        record_metric("node_to_leaf", "DirectorySnapshot", payload.size(), 0, true);
    }
}

void NodeRuntime::Impl::revoke_leaf_locked(const std::string& leaf_id) {
    creek::v1::DirectorySnapshot snap = m_directory.snapshot(m_config.id);
    bool changed = false;
    for (auto& ep : *snap.mutable_endpoints()) {
        if (ep.owner_leaf() == leaf_id) {
            ep.set_alive(false);
            ep.set_version(++m_version_counter);
            ep.set_updated_ms(unix_millis());
            changed = true;
        }
    }
    if (changed) {
        m_directory.merge(snap);
        m_leaf_endpoints.erase(leaf_id);
        broadcast_snapshot_locked();
    } else {
        m_leaf_endpoints.erase(leaf_id);
    }
}

void NodeRuntime::Impl::do_sync_work() {
    std::lock_guard<std::mutex> lk(m_mutex);
    broadcast_snapshot_locked();
}

void NodeRuntime::Impl::do_redis_sync_work() {
    if (!m_redis) return;
    try {
        auto nodes = m_redis->fetch_nodes();
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            for (const auto& [node_id, addr_str] : nodes) {
                if (node_id == m_config.id) continue;
                if (m_known_node_peers.count(node_id) == 0) {
                    auto addr = parse_address(addr_str);
                    if (addr) {
                        m_known_node_peers.insert(node_id);
                        m_transport->connect({node_id, to_tight_address(*addr)});
                    }
                }
            }
        }
        for (const auto& [node_id, _] : nodes) {
            if (node_id == m_config.id) continue;
            auto leaves = m_redis->fetch_leaves_for_node(node_id);
            for (const auto& [leaf_id, leaf_addr] : leaves) {
                std::lock_guard<std::mutex> lk(m_mutex);
                if (m_leaves.count(leaf_id) == 0) {
                    auto addr = parse_address(leaf_addr);
                    if (addr) {
                        m_leaves[leaf_id] = SteadyClock::now();
                            m_transport->connect({leaf_id, to_tight_address(*addr)});
                    }
                }
            }
        }
    } catch (...) {}
}

void NodeRuntime::Impl::record_metric(const std::string& direction, const std::string& rpc_name,
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

}
