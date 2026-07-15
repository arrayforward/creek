#pragma once

#include "creek/types.hpp"
#include "creek.pb.h"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace creek {

struct MetricEvent {
    std::string direction;
    std::string rpc_name;
    Metadata metadata;
    std::uint64_t bytes{};
    std::uint64_t latency_us{};
    bool success{true};
};

struct MetricValue {
    std::uint64_t calls{};
    std::uint64_t errors{};
    std::uint64_t bytes{};
    std::uint64_t latency_us{};
};

using MetricMap = std::map<std::string, MetricValue>;

class MetricsStore {
public:
    explicit MetricsStore(std::chrono::milliseconds period = std::chrono::minutes(1));
    void record(const MetricEvent& event);
    MetricMap previous();
    MetricMap current();
    MetricMap take();
    creek::v1::MetricReply protobuf_snapshot(bool previous_minute, bool take_values);
    std::string openmetrics();
    std::string json(bool previous_minute, bool take_values);

private:
    void rotate_locked(SteadyClock::time_point now);
    static std::string key(const MetricEvent& event);

    std::mutex mutex_;
    std::chrono::milliseconds period_;
    SteadyClock::time_point period_start_;
    MetricMap current_;
    MetricMap previous_;
    MetricMap cumulative_;
};

class MetricsHttpServer {
public:
    MetricsHttpServer(std::shared_ptr<MetricsStore> store, Address bind);
    ~MetricsHttpServer();
    MetricsHttpServer(const MetricsHttpServer&) = delete;
    MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;
    bool start();
    void stop();
    std::uint16_t local_port() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
