// Generic gRPC proxy for the leaf runtime: unary and bidi streaming
// forwarding for any /pkg.Service/Method, implemented on top of
// grpc::AsyncGenericService (ingress) and grpc::GenericStub
// (backend side). Wire-protocol messages (RoutedStreamOpen/Frame/Close)
// are declared in proto/creek.proto.

#include "leaf_runtime_impl.hpp"
#include "../runtime_wire.hpp"

#include "creek/logger.hpp"
#include "creek/trace_context.hpp"

#include <chrono>
#include <set>
#include <utility>

namespace creek {

namespace {

// How long the classifier waits for a second client message before
// committing the call to the stream path. A fast half-close decides
// immediately; this window only delays slow writers.
constexpr int kClassifyWindowMs = 50;
constexpr std::size_t kGenericCallSlots = 2;

bool is_bin_key(const std::string& key) {
    return key.size() >= 4 && key.compare(key.size() - 4, 4, "-bin") == 0;
}

// Blocking generic unary call to a backend (used for local endpoints and on
// the destination leaf for routed generic requests).
grpc::Status blocking_generic_unary(const std::shared_ptr<grpc::Channel>& channel,
                                    const std::string& method,
                                    const Metadata& metadata,
                                    const std::string& body,
                                    std::chrono::milliseconds timeout,
                                    std::string* resp_body) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + timeout);
    for (const auto& kv : metadata) {
        if (forwardable_metadata_key(kv.first)) ctx.AddMetadata(kv.first, kv.second);
    }
    grpc::CompletionQueue cq;
    grpc::GenericStub stub(channel);
    grpc::ByteBuffer req_bb = string_to_byte_buffer(body);
    grpc::ByteBuffer resp_bb;
    grpc::Status status;
    auto reader = stub.PrepareUnaryCall(&ctx, method, req_bb, &cq);
    if (!reader) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "generic_call_create_failed");
    }
    reader->StartCall();
    reader->Finish(&resp_bb, &status, reinterpret_cast<void*>(1));
    void* tag = nullptr;
    bool ok = false;
    cq.Next(&tag, &ok);
    cq.Shutdown();
    if (status.ok() && resp_body) *resp_body = byte_buffer_to_string(resp_bb);
    return status;
}

}  // namespace

std::string byte_buffer_to_string(grpc::ByteBuffer& bb) {
    std::vector<grpc::Slice> slices;
    std::string out;
    if (bb.Dump(&slices).ok()) {
        for (const auto& slice : slices) {
            out.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
        }
    }
    return out;
}

grpc::ByteBuffer string_to_byte_buffer(const std::string& s) {
    grpc::Slice slice(s);
    return grpc::ByteBuffer(&slice, 1);
}

bool forwardable_metadata_key(const std::string& key) {
    if (key.empty() || key[0] == ':') return false;
    if (key.rfind("grpc-", 0) == 0) return false;
    if (is_bin_key(key)) return false;
    static const std::set<std::string> kDeny = {
        "user-agent", "content-type", "te", "authority", "host",
    };
    return kDeny.count(key) == 0;
}

std::string service_from_method(const std::string& method) {
    if (method.empty() || method[0] != '/') return {};
    const auto slash = method.rfind('/');
    if (slash == 0 || slash == std::string::npos) return {};
    return method.substr(1, slash - 1);
}

// ---------------------------------------------------------------------------
// BackendStream
// ---------------------------------------------------------------------------

BackendStream* BackendStream::start(const std::shared_ptr<grpc::Channel>& channel,
                                    const std::string& method,
                                    const Metadata& metadata) {
    auto* bs = new BackendStream();
    for (const auto& kv : metadata) {
        if (forwardable_metadata_key(kv.first)) bs->ctx.AddMetadata(kv.first, kv.second);
    }
    grpc::GenericStub stub(channel);
    bs->rw = stub.PrepareCall(&bs->ctx, method, &bs->cq);
    if (!bs->rw) {
        delete bs;
        return nullptr;
    }
    return bs;
}

void BackendStream::begin() {
    rw->StartCall(&t_start);
    pump = std::thread([this] { run(); });
    pump.detach();
}

void BackendStream::run() {
    void* tag = nullptr;
    bool ok = false;
    bool done = false;
    grpc::Status status;
    while (!done && cq.Next(&tag, &ok)) {
        auto* t = static_cast<OpTag*>(tag);
        switch (t->op) {
        case Op::kStart:
            if (ok) {
                {
                    std::lock_guard<std::mutex> lk(m);
                    started = true;
                    drive_writes_locked();
                }
                rw->Read(&read_bb, &t_read);
            } else {
                rw->Finish(&status, &t_finish);
            }
            break;
        case Op::kRead:
            if (ok) {
                std::string bytes = byte_buffer_to_string(read_bb);
                read_bb.Clear();
                if (on_message) on_message(bytes);
                rw->Read(&read_bb, &t_read);
            } else {
                rw->Finish(&status, &t_finish);
            }
            break;
        case Op::kWrite: {
            std::lock_guard<std::mutex> lk(m);
            write_outstanding = false;
            drive_writes_locked();
            break;
        }
        case Op::kWritesDone:
            break;
        case Op::kFinish:
            done = true;
            break;
        }
    }
    {
        std::lock_guard<std::mutex> lk(m);
        finished = true;
    }
    cq.Shutdown();
    while (cq.Next(&tag, &ok)) {
    }
    // Erases this stream from the impl registry and then invokes on_close.
    // No user of the registry can touch this object afterwards.
    impl->backend_stream_finished(this, status);
    delete this;
}

void BackendStream::drive_writes_locked() {
    if (!started || write_outstanding || finished) return;
    if (!write_queue.empty()) {
        write_bb = string_to_byte_buffer(write_queue.front());
        write_queue.pop_front();
        write_outstanding = true;
        rw->Write(write_bb, &t_write);
        return;
    }
    if (half_close_requested && !writes_done_sent) {
        writes_done_sent = true;
        rw->WritesDone(&t_writes_done);
    }
}

void BackendStream::client_message(std::string bytes) {
    std::lock_guard<std::mutex> lk(m);
    if (finished) return;
    // Back-pressure bound: a backend that never starts draining (hung or
    // unreachable without refusing the connection) would grow this queue
    // without limit. Cancel the call; on_close propagates the failure.
    if (write_queue.size() >= kMaxBackendWriteQueue) {
        if (!overflow_logged) {
            overflow_logged = true;
            CREEK_LOG_WARN(std::string("[creek-leaf] backend write queue overflow, cancel stream rid=")
                           + request_id);
        }
        ctx.TryCancel();
        return;
    }
    write_queue.push_back(std::move(bytes));
    drive_writes_locked();
}

void BackendStream::client_half_close() {
    std::lock_guard<std::mutex> lk(m);
    if (finished) return;
    half_close_requested = true;
    drive_writes_locked();
}

void BackendStream::cancel() { ctx.TryCancel(); }

// ---------------------------------------------------------------------------
// LeafRuntime::Impl generic proxy
// ---------------------------------------------------------------------------

void LeafRuntime::Impl::backend_stream_finished(BackendStream* bs, const grpc::Status& status) {
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_backend_streams.find(bs->request_id);
        if (it != m_backend_streams.end() && it->second == bs) {
            m_backend_streams.erase(it);
        }
    }
    if (bs->on_close) bs->on_close(status);
}

bool LeafRuntime::Impl::send_to_mesh(const std::string& dest_node, const std::string& dest_leaf,
                                     const Bytes& payload, int priority) {
    bool sent = false;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        if (!m_active_parent.empty()) {
            sent = priority > 0 ? m_transport->send_priority(m_active_parent, payload, priority)
                                : m_transport->send(m_active_parent, payload);
        }
        if (!sent) {
            for (const auto& pid : m_parent_ids) {
                if (pid == m_active_parent) continue;
                sent = priority > 0 ? m_transport->send_priority(pid, payload, priority)
                                    : m_transport->send(pid, payload);
                if (sent) break;
            }
        }
    }
    if (!sent) {
        std::string target_addr;
        std::string target_id;
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            if (m_peer_targets.count(dest_node)) {
                target_addr = m_peer_targets[dest_node];
                target_id = dest_node;
            } else {
                auto it = m_known_leaves.find(dest_leaf);
                if (it != m_known_leaves.end()) {
                    target_addr = format_address(it->second);
                    target_id = dest_node;
                }
            }
        }
        if (!target_addr.empty()) {
            auto parsed = parse_address(target_addr);
            if (parsed) {
                m_transport->connect({target_id, to_tight_address(*parsed)});
                sent = priority > 0 ? m_transport->send_priority(target_id, payload, priority)
                                    : m_transport->send(target_id, payload);
            }
        }
    }
    return sent;
}

std::shared_ptr<grpc::Channel> LeafRuntime::Impl::get_channel(const std::string& target) {
    std::lock_guard<std::mutex> lk(m_mutex);
    auto it = m_channels.find(target);
    if (it == m_channels.end()) {
        auto channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
        m_channels[target] = channel;
        return channel;
    }
    return it->second;
}

std::string LeafRuntime::Impl::current_node_id() {
    std::lock_guard<std::mutex> lk(m_mutex);
    return m_active_parent.empty()
               ? (m_config.parents.empty() ? std::string{} : m_config.parents[0].id)
               : m_active_parent;
}

// ---- generic unary, destination leaf --------------------------------------

void LeafRuntime::Impl::process_inbound_generic(const creek::v1::RoutedRequest& req,
                                                std::size_t raw_size) {
    auto start = SteadyClock::now();
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

    Metadata metadata = request_metadata(req);
    auto it_tp = metadata.find("traceparent");
    std::string tp = (it_tp != metadata.end()) ? it_tp->second : std::string();
    auto it_ts = metadata.find("tracestate");
    std::string ts = (it_ts != metadata.end()) ? it_ts->second : std::string();
    TraceSpan mesh_span = TraceContext::extract_or_create(tp, ts);
    TraceSpan backend_span = TraceContext::create_child(mesh_span);
    metadata["traceparent"] = backend_span.traceparent_swapped();
    if (!backend_span.trace_state.empty()) metadata["tracestate"] = backend_span.trace_state;

    std::string resp_body;
    grpc::Status status = blocking_generic_unary(get_channel(ep_opt->target()),
                                                 req.rpc_name(), metadata, req.body(),
                                                 m_config.backend_timeout, &resp_body);
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        SteadyClock::now() - start).count();
    record_metric("leaf_to_backend", req.rpc_name(),
                  static_cast<std::uint64_t>(req.body().size()),
                  static_cast<std::uint64_t>(latency), status.ok(), metadata);

    creek::v1::RoutedResponse resp;
    resp.set_request_id(req.request_id());
    resp.set_origin_leaf(m_config.id);
    resp.set_origin_node(current_node_id());
    resp.set_destination_leaf(req.origin_leaf());
    resp.set_destination_node(req.origin_node());
    resp.set_hop_limit(req.hop_limit() > 0 ? req.hop_limit() - 1 : 0);
    if (status.ok()) {
        resp.set_status(0);
        resp.set_body(resp_body);
    } else {
        resp.set_status(static_cast<std::int32_t>(status.error_code()));
        resp.set_error(status.error_message());
    }
    send_response_to_parent(resp);
    (void)raw_size;
}

// ---- generic ingress: completion queue pump --------------------------------

void LeafRuntime::Impl::prime_generic_call() {
    if (!m_running.load() || !m_generic_service || !m_generic_cq) return;
    auto s = std::make_shared<IngressStream>();
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_pending_calls.push_back(s);
    }
    m_generic_service->RequestCall(&s->ctx, &s->rw, m_generic_cq.get(), m_generic_cq.get(),
                                   &s->t_new);
}

void LeafRuntime::Impl::generic_pump() {
    for (std::size_t i = 0; i < kGenericCallSlots; ++i) prime_generic_call();
    void* tag = nullptr;
    bool ok = false;
    while (m_generic_cq->Next(&tag, &ok)) {
        auto* t = static_cast<IngressStream::OpTag*>(tag);
        IngressStream* s = t->self;
        switch (t->op) {
        case IngressStream::Op::kNewCall: {
            std::shared_ptr<IngressStream> hold;
            {
                std::lock_guard<std::mutex> lk(m_mutex);
                for (auto it = m_pending_calls.begin(); it != m_pending_calls.end(); ++it) {
                    if (it->get() == s) {
                        hold = std::move(*it);
                        m_pending_calls.erase(it);
                        break;
                    }
                }
            }
            if (ok && hold && m_running.load()) {
                prime_generic_call();
                generic_on_new_call(hold);
            }
            break;
        }
        case IngressStream::Op::kRead:
            generic_on_read(s, ok);
            break;
        case IngressStream::Op::kWrite:
            generic_on_write(s, ok);
            break;
        case IngressStream::Op::kFinish:
            generic_on_finish(s, ok);
            break;
        case IngressStream::Op::kAlarm:
            generic_on_alarm(s, ok);
            break;
        }
    }
}

std::shared_ptr<IngressStream> LeafRuntime::Impl::find_stream(const std::string& request_id) {
    std::lock_guard<std::mutex> lk(m_mutex);
    auto it = m_streams.find(request_id);
    if (it == m_streams.end()) return nullptr;
    return it->second;
}

void LeafRuntime::Impl::generic_on_new_call(const std::shared_ptr<IngressStream>& s) {
    s->method.assign(s->ctx.method().data(), s->ctx.method().size());
    s->request_id = random_id();
    for (auto it = s->ctx.client_metadata().begin(); it != s->ctx.client_metadata().end(); ++it) {
        std::string key(it->first.data(), it->first.size());
        std::string val(it->second.data(), it->second.size());
        if (forwardable_metadata_key(key)) s->metadata[key] = val;
    }
    // Sticky key: x-session-id wins over sid; a present sid enables the
    // 1-minute sticky cache (same semantics as the Greeter path).
    std::string sid;
    auto itx = s->metadata.find("x-session-id");
    if (itx != s->metadata.end() && !itx->second.empty()) sid = itx->second;
    if (sid.empty()) {
        auto its = s->metadata.find("sid");
        if (its != s->metadata.end()) sid = its->second;
    }
    if (!sid.empty()) {
        s->metadata["sid"] = sid;
        s->metadata["x-sid"] = sid;
        if (s->metadata.find("sticky") == s->metadata.end()) {
            s->metadata["sticky"] = "true";
        }
    }
    auto it_tp = s->metadata.find("traceparent");
    std::string tp = (it_tp != s->metadata.end()) ? it_tp->second : std::string();
    auto it_ts = s->metadata.find("tracestate");
    std::string ts = (it_ts != s->metadata.end()) ? it_ts->second : std::string();
    TraceSpan span = TraceContext::extract_or_create(tp, ts);
    TraceSpan child = TraceContext::create_child(span);
    s->metadata["traceparent"] = child.traceparent_swapped();
    if (!child.trace_state.empty()) s->metadata["tracestate"] = child.trace_state;

    CREEK_LOG_INFO(std::string("[creek-leaf] generic_call rid=") + s->request_id
                   + " method=" + s->method);
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_streams[s->request_id] = s;
    }
    {
        std::lock_guard<std::mutex> lk(s->m);
        s->phase = 1;
        s->read_outstanding = true;
        s->rw.Read(&s->read_bb, &s->t_read);
    }
}

void LeafRuntime::Impl::generic_on_read(IngressStream* s, bool ok) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    std::unique_lock<std::mutex> lk(s->m);
    if (s->dead) return;
    s->last_activity = SteadyClock::now();
    s->read_outstanding = false;
    if (s->finish_pending || s->finishing) {
        lk.unlock();
        ingress_try_finish(s);
        return;
    }
    if (s->phase == 1) {
        if (ok) {
            s->buffered.push_back(byte_buffer_to_string(s->read_bb));
            s->read_bb.Clear();
            s->phase = 2;
            s->read_outstanding = true;
            s->rw.Read(&s->read_bb, &s->t_read);
            s->alarm.Set(m_generic_cq.get(),
                         std::chrono::system_clock::now() +
                             std::chrono::milliseconds(kClassifyWindowMs),
                         &s->t_alarm);
        } else {
            // No message before the read completed. Either the client
            // half-closed an empty stream or the call was cancelled; start
            // the stream path with an immediate half-close — a cancelled
            // call will fail on the first write/finish anyway (avoids
            // touching ctx state that may already be torn down).
            s->phase = 3;
            lk.unlock();
            generic_begin_stream(hold);
            ingress_forward_frame(s, "", true);
        }
        return;
    }
    if (s->phase == 2) {
        s->alarm.Cancel();
        if (ok) {
            s->buffered.push_back(byte_buffer_to_string(s->read_bb));
            s->read_bb.Clear();
            s->phase = 3;
            s->read_outstanding = true;
            s->rw.Read(&s->read_bb, &s->t_read);
            lk.unlock();
            generic_begin_stream(hold);
        } else {
            // One request frame followed by half-close: this is either a
            // unary call or a server-streaming call — indistinguishable at
            // this layer. Both go through the stream path (Open + one frame
            // + half_close); a unary backend simply answers with a single
            // frame followed by Close, a server-streaming backend with N.
            s->phase = 3;
            lk.unlock();
            generic_begin_stream(hold);
            ingress_forward_frame(s, "", true);
        }
        return;
    }
    // phase 3: stream mode (also covers unary / server-streaming, which
    // are one-request + half-close special cases).
    if (s->phase == 3) {
        if (s->read_stopped) return;
        if (ok) {
            std::string bytes = byte_buffer_to_string(s->read_bb);
            s->read_bb.Clear();
            s->read_outstanding = true;
            s->rw.Read(&s->read_bb, &s->t_read);
            lk.unlock();
            ingress_forward_frame(s, std::move(bytes), false);
        } else {
            s->read_stopped = true;
            lk.unlock();
            ingress_forward_frame(s, "", true);
        }
        return;
    }
}

void LeafRuntime::Impl::generic_on_write(IngressStream* s, bool ok) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    std::unique_lock<std::mutex> lk(s->m);
    if (s->dead) return;
    s->last_activity = SteadyClock::now();
    s->write_outstanding = false;
    if (!ok) {
        lk.unlock();
        ingress_teardown(hold, grpc::Status(grpc::StatusCode::CANCELLED, "write_failed"));
        return;
    }
    if (!s->write_queue.empty() && !s->finishing && !s->finish_pending) {
        s->write_bb = string_to_byte_buffer(s->write_queue.front());
        s->write_queue.pop_front();
        s->write_outstanding = true;
        s->rw.Write(s->write_bb, &s->t_write);
        return;
    }
    // Only drive towards Finish when a finish was actually requested
    // (backend closed / teardown / unary reply). Draining the write queue
    // of a live stream must NOT complete the call.
    if (s->finish_pending) {
        lk.unlock();
        ingress_try_finish(s);
    }
}

void LeafRuntime::Impl::generic_on_finish(IngressStream* s, bool ok) {
    (void)ok;
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    std::lock_guard<std::mutex> lk(s->m);
    // Defer erasure to the sweeper: the cancelled classification alarm may
    // still deliver an event that carries a raw pointer to this stream.
    s->dead = true;
    s->dead_since = SteadyClock::now();
}

void LeafRuntime::Impl::generic_on_alarm(IngressStream* s, bool ok) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    std::unique_lock<std::mutex> lk(s->m);
    if (s->dead || !ok) return;
    if (s->phase != 2) return;
    s->phase = 3;
    lk.unlock();
    generic_begin_stream(hold);
}

// ---- generic ingress: stream -----------------------------------------------

void LeafRuntime::Impl::generic_begin_stream(std::shared_ptr<IngressStream> s) {
    IngressStream* sp = s.get();
    const std::string service = service_from_method(sp->method);
    std::vector<creek::v1::Endpoint> endpoints;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        endpoints = m_directory.service(service);
    }
    std::optional<creek::v1::Endpoint> ep_opt;
    if (!endpoints.empty()) {
        std::lock_guard<std::mutex> lk(m_mutex);
        ep_opt = m_balancer.pick(service, sp->metadata, endpoints);
    }
    grpc::Status pick_error;
    if (!ep_opt) {
        pick_error = grpc::Status(grpc::StatusCode::UNAVAILABLE, "no_endpoint");
    } else if (!m_breaker.allow(ep_opt->endpoint_id())) {
        pick_error = grpc::Status(grpc::StatusCode::UNAVAILABLE, "circuit_open");
    }
    if (!pick_error.ok()) {
        record_metric("client_to_leaf", sp->method, 0, 0, false, sp->metadata);
        {
            std::lock_guard<std::mutex> lk(sp->m);
            if (sp->dead || sp->finishing) return;
            sp->finish_status = pick_error;
        }
        ingress_try_finish(sp);
        return;
    }
    const creek::v1::Endpoint& ep = *ep_opt;
    CREEK_LOG_INFO(std::string("[creek-leaf] generic_stream rid=") + sp->request_id
                   + " method=" + sp->method + " ep=" + ep.endpoint_id());
    if (ep.owner_leaf() == m_config.id) {
        auto weak = std::weak_ptr<IngressStream>(s);
        auto* bs = BackendStream::start(get_channel(ep.target()), sp->method, sp->metadata);
        if (!bs) {
            std::lock_guard<std::mutex> lk(sp->m);
            if (!sp->dead && !sp->finishing) {
                sp->finish_status = grpc::Status(grpc::StatusCode::INTERNAL,
                                                 "backend_call_create_failed");
            }
            ingress_try_finish(sp);
            return;
        }
        bs->impl = this;
        bs->request_id = sp->request_id;
        bs->on_message = [this, weak](std::string bytes) {
            if (auto p = weak.lock()) ingress_backend_message(p.get(), std::move(bytes));
        };
        bs->on_close = [this, weak](grpc::Status st) {
            if (auto p = weak.lock()) ingress_backend_closed(p.get(), st);
        };
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_backend_streams[sp->request_id] = bs;
        }
        {
            std::lock_guard<std::mutex> lk(sp->m);
            sp->remote = false;
            sp->backend = bs;
            sp->open_sent_at = SteadyClock::now();
        }
        bs->begin();
    } else {
        {
            std::lock_guard<std::mutex> lk(sp->m);
            sp->remote = true;
            sp->dest_leaf = ep.owner_leaf();
            sp->dest_node = ep.owner_node();
            sp->open_sent_at = SteadyClock::now();
        }
        creek::v1::RoutedStreamOpen open;
        open.set_request_id(sp->request_id);
        open.set_origin_leaf(m_config.id);
        open.set_origin_node(current_node_id());
        open.set_destination_leaf(ep.owner_leaf());
        open.set_destination_node(ep.owner_node());
        open.set_endpoint_id(ep.endpoint_id());
        open.set_rpc_name(sp->method);
        for (const auto& entry : sp->metadata) {
            (*open.mutable_metadata())[entry.first] = entry.second;
        }
        open.set_deadline_ms(static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()) +
            static_cast<std::uint64_t>(kStreamIdleTimeout.count()) * 1000);
        open.set_hop_limit(kDefaultBackendHopLimit);
        creek::v1::WireMessage wm;
        *wm.mutable_stream_open() = open;
        Bytes payload = serialize_wire(wm);
        send_to_mesh(open.destination_node(), open.destination_leaf(), payload, 1);
        record_metric("leaf_to_node", "RoutedStreamOpen", payload.size(), 0, true,
                      sp->metadata);
    }
    record_metric("client_to_leaf", sp->method, 0, 0, true, sp->metadata);
    // Flush messages that arrived before the mode was decided.
    std::vector<std::string> pending;
    {
        std::lock_guard<std::mutex> lk(sp->m);
        pending.swap(sp->buffered);
    }
    for (auto& bytes : pending) {
        ingress_forward_frame(sp, std::move(bytes), false);
    }
}

void LeafRuntime::Impl::ingress_forward_frame(IngressStream* s, std::string bytes,
                                              bool half_close) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    BackendStream* bs = nullptr;
    bool remote = false;
    std::string dest_leaf, dest_node;
    {
        std::lock_guard<std::mutex> lk(s->m);
        if (s->dead || s->finishing) return;
        s->last_activity = SteadyClock::now();
        remote = s->remote;
        dest_leaf = s->dest_leaf;
        dest_node = s->dest_node;
    }
    if (remote) {
        creek::v1::RoutedStreamFrame frame;
        frame.set_request_id(s->request_id);
        frame.set_payload(bytes);
        frame.set_half_close(half_close);
        frame.set_from_origin(true);
        frame.set_origin_leaf(m_config.id);
        frame.set_origin_node(current_node_id());
        frame.set_destination_leaf(dest_leaf);
        frame.set_destination_node(dest_node);
        frame.set_hop_limit(kDefaultBackendHopLimit);
        creek::v1::WireMessage wm;
        *wm.mutable_stream_frame() = frame;
        Bytes payload = serialize_wire(wm);
        send_to_mesh(dest_node, dest_leaf, payload, 1);
        return;
    }
    {
        // bs is kept alive by the registry: deletion requires m_mutex, which
        // is held across the call.
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_backend_streams.find(s->request_id);
        if (it == m_backend_streams.end()) return;
        bs = it->second;
        if (half_close) {
            bs->client_half_close();
        } else {
            bs->client_message(std::move(bytes));
        }
    }
}

void LeafRuntime::Impl::ingress_queue_write(IngressStream* s, std::string bytes) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    {
        std::lock_guard<std::mutex> lk(s->m);
        if (s->dead || s->finishing) return;
        s->last_activity = SteadyClock::now();
        if (s->write_outstanding) {
            // Back-pressure bound: a client that stops reading would grow
            // this queue without limit. Fail the stream instead.
            if (s->write_queue.size() >= kMaxIngressWriteQueue) {
                s->finish_status = grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                                "client_read_stall");
                s->read_stopped = true;
            } else {
                s->write_queue.push_back(std::move(bytes));
                return;
            }
        } else {
            s->write_bb = string_to_byte_buffer(bytes);
            s->write_outstanding = true;
            s->rw.Write(s->write_bb, &s->t_write);
            return;
        }
    }
    ingress_teardown(hold, grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                        "client_read_stall"));
}

void LeafRuntime::Impl::ingress_backend_message(IngressStream* s, std::string bytes) {
    {
        std::lock_guard<std::mutex> lk(s->m);
        s->backend_confirmed = true;
    }
    ingress_queue_write(s, std::move(bytes));
}

void LeafRuntime::Impl::ingress_backend_closed(IngressStream* s, const grpc::Status& status) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    {
        std::lock_guard<std::mutex> lk(s->m);
        if (s->dead || s->finishing) return;
        s->last_activity = SteadyClock::now();
        s->backend_confirmed = true;
        s->finish_status = status;
        s->read_stopped = true;
        s->backend = nullptr;
    }
    ingress_try_finish(s);
}

void LeafRuntime::Impl::ingress_try_finish(IngressStream* s) {
    auto hold = find_stream(s->request_id);
    if (!hold) return;
    std::unique_lock<std::mutex> lk(s->m);
    if (s->dead || s->finishing) return;
    if (s->write_outstanding || !s->write_queue.empty()) {
        s->finish_pending = true;
        return;
    }
    if (s->read_outstanding) {
        s->finish_pending = true;
        s->read_stopped = true;
        return;
    }
    s->finishing = true;
    const grpc::Status status = s->finish_status;
    s->alarm.Cancel();
    lk.unlock();
    s->rw.Finish(status, &s->t_finish);
}

void LeafRuntime::Impl::ingress_teardown(const std::shared_ptr<IngressStream>& s,
                                         const grpc::Status& status) {
    BackendStream* bs = nullptr;
    bool remote = false;
    std::string dest_leaf, dest_node;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_backend_streams.find(s->request_id);
        if (it != m_backend_streams.end()) {
            bs = it->second;
            bs->cancel();
        }
    }
    {
        std::lock_guard<std::mutex> lk(s->m);
        if (s->dead || s->finishing) return;
        s->finish_status = status;
        s->read_stopped = true;
        s->backend = nullptr;
        remote = s->remote;
        dest_leaf = s->dest_leaf;
        dest_node = s->dest_node;
    }
    if (remote && !dest_leaf.empty()) {
        send_stream_close_to(s->request_id, dest_leaf, dest_node, true,
                             static_cast<std::int32_t>(status.error_code()),
                             status.error_message());
    }
    ingress_try_finish(s.get());
}

// ---- stream wire handlers (destination leaf + ingress leaf) -----------------

void LeafRuntime::Impl::stream_worker_loop() {
    while (m_running.load()) {
        auto task = m_stream_queue.take_for(std::chrono::milliseconds(100));
        if (!task) continue;
        try {
            (*task)();
        } catch (...) {}
    }
    while (auto task = m_stream_queue.poll()) {
        try { (*task)(); } catch (...) {}
    }
}

void LeafRuntime::Impl::send_stream_close_to(const std::string& request_id,
                                             const std::string& dest_leaf,
                                             const std::string& dest_node,
                                             bool from_origin, std::int32_t status,
                                             const std::string& error) {
    creek::v1::RoutedStreamClose close;
    close.set_request_id(request_id);
    close.set_status(status);
    close.set_error(error);
    close.set_from_origin(from_origin);
    close.set_origin_leaf(m_config.id);
    close.set_origin_node(current_node_id());
    close.set_destination_leaf(dest_leaf);
    close.set_destination_node(dest_node);
    close.set_hop_limit(kDefaultBackendHopLimit);
    creek::v1::WireMessage wm;
    *wm.mutable_stream_close() = close;
    Bytes payload = serialize_wire(wm);
    send_to_mesh(dest_node, dest_leaf, payload, 1);
}

void LeafRuntime::Impl::process_stream_open(const creek::v1::RoutedStreamOpen& open) {
    if (open.destination_leaf() != m_config.id) {
        send_stream_close_to(open.request_id(), open.origin_leaf(), open.origin_node(),
                             false, -1, "wrong_destination_leaf");
        return;
    }
    std::optional<creek::v1::Endpoint> ep_opt;
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        ep_opt = m_directory.find(open.endpoint_id());
    }
    if (!ep_opt || !ep_opt->alive()) {
        send_stream_close_to(open.request_id(), open.origin_leaf(), open.origin_node(),
                             false, -1, "endpoint_not_found");
        return;
    }
    CREEK_LOG_INFO(std::string("[creek-leaf] stream_open rid=") + open.request_id()
                   + " method=" + open.rpc_name() + " ep=" + open.endpoint_id());
    Metadata metadata;
    for (const auto& entry : open.metadata()) {
        metadata[entry.first] = entry.second;
    }
    const std::string dest_leaf = open.origin_leaf();
    const std::string dest_node = open.origin_node();
    const std::string rid = open.request_id();
    auto* bs = BackendStream::start(get_channel(ep_opt->target()), open.rpc_name(), metadata);
    if (!bs) {
        send_stream_close_to(rid, dest_leaf, dest_node, false, -1,
                             "backend_call_create_failed");
        return;
    }
    bs->impl = this;
    bs->request_id = rid;
    bs->on_message = [this, rid, dest_leaf, dest_node](std::string bytes) {
        creek::v1::RoutedStreamFrame frame;
        frame.set_request_id(rid);
        frame.set_payload(bytes);
        frame.set_half_close(false);
        frame.set_from_origin(false);
        frame.set_origin_leaf(m_config.id);
        frame.set_origin_node(current_node_id());
        frame.set_destination_leaf(dest_leaf);
        frame.set_destination_node(dest_node);
        frame.set_hop_limit(kDefaultBackendHopLimit);
        creek::v1::WireMessage wm;
        *wm.mutable_stream_frame() = frame;
        Bytes payload = serialize_wire(wm);
        send_to_mesh(dest_node, dest_leaf, payload, 1);
    };
    bs->on_close = [this, rid, dest_leaf, dest_node](grpc::Status st) {
        send_stream_close_to(rid, dest_leaf, dest_node, false,
                             static_cast<std::int32_t>(st.error_code()),
                             st.error_message());
    };
    {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_backend_streams[rid] = bs;
    }
    bs->begin();
    record_metric("leaf_to_backend", open.rpc_name(), 0, 0, true, metadata);
}

void LeafRuntime::Impl::process_stream_frame(const creek::v1::RoutedStreamFrame& frame) {
    if (frame.from_origin()) {
        // Destination leaf: client -> backend direction.
        bool known = false;
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            auto it = m_backend_streams.find(frame.request_id());
            if (it != m_backend_streams.end()) {
                known = true;
                if (frame.half_close()) {
                    it->second->client_half_close();
                } else if (!frame.payload().empty()) {
                    it->second->client_message(frame.payload());
                }
            }
        }
        if (!known) {
            // Frame for a stream we never saw open (its RoutedStreamOpen was
            // lost to a mesh partition) or that is already gone. Tell the
            // ingress to fail it so the client reconnects instead of hanging
            // forever. Rate-limited: a flooding client would otherwise turn
            // every frame into a close packet.
            auto now = SteadyClock::now();
            bool reply = false;
            {
                std::lock_guard<std::mutex> lk(m_mutex);
                auto it = m_unknown_stream_replies.find(frame.request_id());
                if (it == m_unknown_stream_replies.end() ||
                    now - it->second > std::chrono::seconds(5)) {
                    m_unknown_stream_replies[frame.request_id()] = now;
                    reply = true;
                }
            }
            if (reply) {
                send_stream_close_to(frame.request_id(), frame.origin_leaf(),
                                     frame.origin_node(), false, -1, "unknown_stream");
            }
        }
        return;
    }
    // Ingress leaf: backend -> client direction.
    auto hold = find_stream(frame.request_id());
    if (!hold) {
        // Our side is gone; make sure the destination side is too.
        send_stream_close_to(frame.request_id(), frame.origin_leaf(),
                             frame.origin_node(), true, 1, "unknown_stream");
        return;
    }
    if (frame.payload().empty()) return;
    ingress_backend_message(hold.get(), frame.payload());
}

void LeafRuntime::Impl::process_stream_close(const creek::v1::RoutedStreamClose& close) {
    CREEK_LOG_DEBUG(std::string("[creek-leaf] stream_close rid=") + close.request_id()
                    + " from_origin=" + std::to_string(close.from_origin())
                    + " status=" + std::to_string(close.status())
                    + " err=" + close.error());
    if (close.from_origin()) {
        // Destination leaf: ingress cancelled the stream.
        std::lock_guard<std::mutex> lk(m_mutex);
        auto it = m_backend_streams.find(close.request_id());
        if (it == m_backend_streams.end()) return;
        it->second->cancel();
        return;
    }
    // Ingress leaf: backend finished (or a node reported a routing error).
    grpc::StatusCode code = close.status() > 0
        ? static_cast<grpc::StatusCode>(close.status())
        : (close.status() == 0 ? grpc::StatusCode::OK : grpc::StatusCode::UNAVAILABLE);
    ingress_backend_closed_raw(close.request_id(),
                               grpc::Status(code, close.error()));
}

void LeafRuntime::Impl::ingress_backend_closed_raw(const std::string& request_id,
                                                   const grpc::Status& status) {
    auto hold = find_stream(request_id);
    if (!hold) return;
    ingress_backend_closed(hold.get(), status);
}

// ---- stream sweeper ----------------------------------------------------------

void LeafRuntime::Impl::stream_sweep_loop() {
    while (m_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (!m_running.load()) break;
        const auto now = SteadyClock::now();
        std::vector<std::shared_ptr<IngressStream>> victims;
        std::vector<std::shared_ptr<IngressStream>> open_timeout_victims;
        std::vector<std::string> dead_ids;
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            for (auto& kv : m_streams) {
                auto& s = kv.second;
                std::unique_lock<std::mutex> sl(s->m, std::try_to_lock);
                if (!sl.owns_lock()) continue;
                if (s->dead) {
                    if (now - s->dead_since > std::chrono::seconds(2)) {
                        dead_ids.push_back(kv.first);
                    }
                    continue;
                }
                // Never touch the grpc context here: a stream that is
                // mid-Finish no longer owns a live call object. Cancellation
                // is observed via read/write completions; the idle timeout
                // is the fallback cleanup.
                if (s->finishing) continue;
                if (now - s->last_activity > kStreamIdleTimeout) {
                    victims.push_back(s);
                    continue;
                }
                // Open-ack timeout: the RoutedStreamOpen may have been lost
                // to a mesh partition (or the backend call never started).
                // Without any backend-side sign of life the stream would
                // hang forever — fail it so the client can reconnect.
                if (!s->backend_confirmed &&
                    s->open_sent_at.time_since_epoch().count() != 0 &&
                    now - s->open_sent_at > kStreamOpenTimeout) {
                    open_timeout_victims.push_back(s);
                }
            }
            for (const auto& id : dead_ids) {
                m_streams.erase(id);
            }
            for (auto it = m_unknown_stream_replies.begin();
                 it != m_unknown_stream_replies.end();) {
                if (now - it->second > std::chrono::seconds(60)) {
                    it = m_unknown_stream_replies.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto& s : victims) {
            ingress_teardown(s, grpc::Status(grpc::StatusCode::CANCELLED,
                                             "stream_cancelled_or_idle"));
        }
        for (auto& s : open_timeout_victims) {
            ingress_teardown(s, grpc::Status(grpc::StatusCode::UNAVAILABLE,
                                             "stream_open_timeout"));
        }
    }
}

}  // namespace creek
