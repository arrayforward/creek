#pragma once

// Internal generic gRPC proxy machinery for the leaf runtime. Not part of
// the public API. IngressStream models one client-facing call on the entry
// leaf (unary or bidi, driven by grpc::AsyncGenericService events);
// BackendStream models one outgoing bidi call to a real backend via
// grpc::generic::GenericStub (used both for local endpoints and on the
// destination leaf for remote endpoints).

#include "creek/types.hpp"
#include "creek/leaf/leaf_runtime.hpp"

#include <grpcpp/grpcpp.h>
#include <grpcpp/alarm.h>
#include <grpcpp/generic/async_generic_service.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/support/byte_buffer.h>

#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace creek {

class LeafRuntime;

// How long a proxied stream may sit idle (no frames either way) before the
// sweeper tears it down.
inline constexpr std::chrono::seconds kStreamIdleTimeout{120};
// How long the ingress waits for the first backend-side sign of life (a
// frame or a close) after opening a stream. Covers a RoutedStreamOpen lost
// to a mesh outage: without it the stream would hang forever.
inline constexpr std::chrono::seconds kStreamOpenTimeout{15};
// Back-pressure bounds. When a client stops reading, the ingress write
// queue would otherwise grow without limit; likewise for a backend that
// never starts draining.
inline constexpr std::size_t kMaxIngressWriteQueue = 256;
inline constexpr std::size_t kMaxBackendWriteQueue = 1024;

std::string byte_buffer_to_string(grpc::ByteBuffer& bb);
grpc::ByteBuffer string_to_byte_buffer(const std::string& s);
// True when a client metadata key may be forwarded to a backend / over the
// wire protocol (skips HTTP/2 pseudo headers, grpc-internal keys, transport
// keys and -bin binary values which cannot ride proto string maps safely).
bool forwardable_metadata_key(const std::string& key);
// "/pkg.Service/Method" -> "pkg.Service" ("" when malformed).
std::string service_from_method(const std::string& method);

// One proxied backend bidi call. Owns its completion queue and pump thread;
// the pump thread self-deletes the object after the call finishes, so users
// must treat the pointer as dead once on_close has been invoked.
struct BackendStream {
    enum class Op { kStart, kRead, kWrite, kWritesDone, kFinish };
    struct OpTag {
        BackendStream* self;
        Op op;
    };

    grpc::ClientContext ctx;
    std::unique_ptr<grpc::GenericClientAsyncReaderWriter> rw;
    grpc::CompletionQueue cq;
    std::thread pump;
    OpTag t_start{this, Op::kStart};
    OpTag t_read{this, Op::kRead};
    OpTag t_write{this, Op::kWrite};
    OpTag t_writes_done{this, Op::kWritesDone};
    OpTag t_finish{this, Op::kFinish};

    std::mutex m;
    std::deque<std::string> write_queue;
    bool started{false};
    bool write_outstanding{false};
    bool half_close_requested{false};
    bool writes_done_sent{false};
    bool finished{false};
    bool overflow_logged{false};
    grpc::ByteBuffer read_bb;
    grpc::ByteBuffer write_bb;

    std::function<void(std::string bytes)> on_message;
    std::function<void(grpc::Status status)> on_close;

    // Back-pointer for registry removal at end of life, set by the creator
    // together with request_id before begin() is called.
    LeafRuntime::Impl* impl{nullptr};
    std::string request_id;

    // Creates the call object (no I/O yet); callbacks must be assigned and
    // the object registered before begin(). Returns nullptr on failure.
    static BackendStream* start(const std::shared_ptr<grpc::Channel>& channel,
                                const std::string& method,
                                const Metadata& metadata);
    // Starts the call and the detached pump thread. The pump thread
    // self-deletes the object after the call finishes, so users must treat
    // the pointer as dead once it has been removed from the registry.
    void begin();

    // Client -> backend direction. Thread-safe; drops input once finished.
    void client_message(std::string bytes);
    void client_half_close();
    // Cancels the backend call (client went away / teardown). Thread-safe.
    void cancel();

    void run();
    void drive_writes_locked();
};

// One client-facing generic call on the ingress leaf. Owned by
// LeafRuntime::Impl::m_streams (shared_ptr); raw pointers are used as CQ
// tags and are valid until the kFinish event, which erases the map entry.
struct IngressStream : std::enable_shared_from_this<IngressStream> {
    enum class Op { kNewCall, kRead, kWrite, kFinish, kAlarm };
    struct OpTag {
        IngressStream* self;
        Op op;
    };

    // phase: 0 = fresh, 1 = first read pending, 2 = classifying
    // (multi-message vs single-shot), 3 = stream mode. Unary and
    // server-streaming calls (one request + half-close) are stream-mode
    // special cases: they ride Open + one frame + half_close.
    int phase{0};

    grpc::GenericServerContext ctx;
    grpc::GenericServerAsyncReaderWriter rw{&ctx};
    grpc::Alarm alarm;
    OpTag t_new{this, Op::kNewCall};
    OpTag t_read{this, Op::kRead};
    OpTag t_write{this, Op::kWrite};
    OpTag t_finish{this, Op::kFinish};
    OpTag t_alarm{this, Op::kAlarm};

    std::mutex m;
    bool read_outstanding{false};
    bool read_stopped{false};
    bool write_outstanding{false};
    bool finishing{false};
    bool finish_pending{false};
    grpc::Status finish_status{grpc::Status::OK};
    grpc::ByteBuffer read_bb;
    grpc::ByteBuffer write_bb;
    std::vector<std::string> buffered;  // messages read before/while classifying
    std::deque<std::string> write_queue;

    std::string method;
    std::string request_id;
    Metadata metadata;  // filtered client metadata + normalized sid/sticky
    bool remote{false};
    std::string dest_leaf;
    std::string dest_node;
    BackendStream* backend{nullptr};  // local endpoints only
    SteadyClock::time_point last_activity{SteadyClock::now()};
    // Open-ack tracking: set when the stream is opened (local or remote);
    // backend_confirmed flips on the first backend-side frame or close. The
    // sweeper fails streams that stay unconfirmed past kStreamOpenTimeout
    // (e.g. the RoutedStreamOpen was lost to a mesh partition).
    SteadyClock::time_point open_sent_at{};
    bool backend_confirmed{false};
    // Set when the kFinish event arrived; actual map erasure is deferred to
    // the sweeper so late alarm events never dereference freed memory.
    bool dead{false};
    SteadyClock::time_point dead_since{};
};

}  // namespace creek
