#pragma once

#include "creek/types.hpp"
#include "creek.pb.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace creek {

class EndpointDirectory {
public:
    bool merge(const creek::v1::Endpoint& endpoint);
    bool merge(const creek::v1::DirectorySnapshot& snapshot);
    void upsert_local(creek::v1::Endpoint endpoint);
    // Erase a locally-owned endpoint and leave a tombstone so delayed
    // snapshots carrying older versions cannot resurrect it. Returns true
    // when an entry was actually removed.
    bool remove_local(const std::string& endpoint_id);
    // Apply a removal tombstone learned from a snapshot: erases the entry
    // when its version is not newer than the tombstone and records the
    // tombstone to suppress resurrection by older in-flight snapshots.
    bool apply_removal(const std::string& endpoint_id, std::uint64_t version,
                       std::uint64_t updated_ms);
    std::optional<creek::v1::Endpoint> find(std::string_view endpoint_id) const;
    std::vector<creek::v1::Endpoint> service(std::string_view name) const;
    creek::v1::DirectorySnapshot snapshot(std::string_view source_id) const;
    // Live (unexpired) tombstones as Endpoint shells (endpoint_id/version/
    // updated_ms), ready for a snapshot's removed_endpoints field.
    std::vector<creek::v1::Endpoint> tombstones() const;
    std::uint64_t version() const;
    std::size_t size() const;

private:
    struct Tombstone {
        std::uint64_t version{};
        std::uint64_t updated_ms{};
        std::uint64_t removed_ms{};
    };
    // Tombstones ride snapshots and suppress resurrection for this long.
    // Far beyond any sync interval, short enough to be forgotten.
    static constexpr std::int64_t kTombstoneTtlMs = 60000;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, creek::v1::Endpoint> endpoints_;
    mutable std::unordered_map<std::string, Tombstone> tombstones_;
    std::uint64_t version_{};
};

class StickyBalancer {
public:
    explicit StickyBalancer(std::size_t lru_capacity = 4096,
                            std::chrono::milliseconds ttl = std::chrono::minutes(1));
    std::optional<creek::v1::Endpoint> pick(
        std::string_view service, const Metadata& metadata,
        const std::vector<creek::v1::Endpoint>& endpoints,
        SteadyClock::time_point now = SteadyClock::now());
    void invalidate(std::string_view endpoint_id);
    void set_ttl(std::chrono::milliseconds ttl);
    std::size_t active_size() const;
    std::size_t lru_size() const;
    void set_shard_key(const std::string& key);

private:
    struct Entry {
        std::string endpoint_id;
        SteadyClock::time_point last_used;
        SteadyClock::time_point lru_since;
        bool lru{};
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
    std::size_t capacity_;
    std::chrono::milliseconds ttl_;
    std::uint64_t round_robin_{};
};

}
