#include "creek/routing.hpp"
#include "creek/types.hpp"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace creek {

bool EndpointDirectory::merge(const creek::v1::Endpoint& endpoint) {
    if (endpoint.endpoint_id().empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    auto ts = tombstones_.find(endpoint.endpoint_id());
    if (ts != tombstones_.end() && endpoint.version() <= ts->second.version) {
        return false;
    }
    auto it = endpoints_.find(endpoint.endpoint_id());
    if (it == endpoints_.end()) {
        endpoints_.emplace(endpoint.endpoint_id(), endpoint);
        ++version_;
        return true;
    }
    const auto& existing = it->second;
    if (endpoint.version() > existing.version()) {
        it->second = endpoint;
        ++version_;
        return true;
    }
    if (endpoint.version() == existing.version() &&
        endpoint.updated_ms() > existing.updated_ms()) {
        it->second = endpoint;
        ++version_;
        return true;
    }
    return false;
}

bool EndpointDirectory::merge(const creek::v1::DirectorySnapshot& snapshot) {
    bool changed = false;
    for (const auto& ep : snapshot.endpoints()) {
        if (merge(ep)) changed = true;
    }
    for (const auto& removed : snapshot.removed_endpoints()) {
        if (apply_removal(removed.endpoint_id(), removed.version(),
                          removed.updated_ms())) {
            changed = true;
        }
    }
    return changed;
}

void EndpointDirectory::upsert_local(creek::v1::Endpoint endpoint) {
    if (endpoint.endpoint_id().empty()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    // A local upsert is authoritative: clear any tombstone for this id.
    tombstones_.erase(endpoint.endpoint_id());
    auto it = endpoints_.find(endpoint.endpoint_id());
    if (it == endpoints_.end()) {
        if (endpoint.version() == 0) endpoint.set_version(1);
        if (endpoint.updated_ms() == 0) {
            endpoint.set_updated_ms(creek::unix_millis());
        }
    } else {
        endpoint.set_version(it->second.version() + 1);
        if (endpoint.updated_ms() == 0) {
            endpoint.set_updated_ms(creek::unix_millis());
        }
    }
    endpoints_[endpoint.endpoint_id()] = std::move(endpoint);
    ++version_;
}

bool EndpointDirectory::remove_local(const std::string& endpoint_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = endpoints_.find(endpoint_id);
    if (it == endpoints_.end()) return false;
    Tombstone ts{it->second.version(), it->second.updated_ms(), creek::unix_millis()};
    endpoints_.erase(it);
    tombstones_[endpoint_id] = ts;
    ++version_;
    return true;
}

bool EndpointDirectory::apply_removal(const std::string& endpoint_id,
                                      std::uint64_t version,
                                      std::uint64_t updated_ms) {
    if (endpoint_id.empty()) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    bool changed = false;
    auto it = endpoints_.find(endpoint_id);
    if (it != endpoints_.end() && it->second.version() <= version) {
        endpoints_.erase(it);
        ++version_;
        changed = true;
    }
    auto ts = tombstones_.find(endpoint_id);
    if (ts == tombstones_.end() || ts->second.version < version) {
        tombstones_[endpoint_id] = Tombstone{version, updated_ms, creek::unix_millis()};
    }
    return changed;
}

std::optional<creek::v1::Endpoint> EndpointDirectory::find(std::string_view endpoint_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = endpoints_.find(std::string(endpoint_id));
    if (it == endpoints_.end()) return std::nullopt;
    return it->second;
}

std::vector<creek::v1::Endpoint> EndpointDirectory::service(std::string_view name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<creek::v1::Endpoint> result;
    for (const auto& kv : endpoints_) {
        if (kv.second.service() == name) {
            result.push_back(kv.second);
        }
    }
    return result;
}

creek::v1::DirectorySnapshot EndpointDirectory::snapshot(std::string_view source_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    creek::v1::DirectorySnapshot snap;
    snap.set_source_id(std::string(source_id));
    snap.set_version(version_);
    snap.set_generated_ms(creek::unix_millis());
    for (const auto& kv : endpoints_) {
        *snap.add_endpoints() = kv.second;
    }
    const auto now = creek::unix_millis();
    for (auto it = tombstones_.begin(); it != tombstones_.end();) {
        if (now - it->second.removed_ms >= static_cast<std::uint64_t>(kTombstoneTtlMs)) {
            it = tombstones_.erase(it);
            continue;
        }
        auto* removed = snap.add_removed_endpoints();
        removed->set_endpoint_id(it->first);
        removed->set_version(it->second.version);
        removed->set_updated_ms(it->second.updated_ms);
        ++it;
    }
    return snap;
}

std::vector<creek::v1::Endpoint> EndpointDirectory::tombstones() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<creek::v1::Endpoint> out;
    const auto now = creek::unix_millis();
    for (const auto& kv : tombstones_) {
        if (now - kv.second.removed_ms >= static_cast<std::uint64_t>(kTombstoneTtlMs)) {
            continue;
        }
        creek::v1::Endpoint removed;
        removed.set_endpoint_id(kv.first);
        removed.set_version(kv.second.version);
        removed.set_updated_ms(kv.second.updated_ms);
        out.push_back(std::move(removed));
    }
    return out;
}

std::uint64_t EndpointDirectory::version() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return version_;
}

std::size_t EndpointDirectory::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return endpoints_.size();
}

StickyBalancer::StickyBalancer(std::size_t lru_capacity, std::chrono::milliseconds ttl)
    : capacity_(lru_capacity), ttl_(ttl) {}

namespace {

bool is_sticky_value(const std::string& v) {
    return v == "true" || v == "1";
}

bool metadata_sticky(const Metadata& metadata) {
    auto it = metadata.find("sticky");
    if (it == metadata.end()) return false;
    return is_sticky_value(it->second);
}

std::optional<std::string> metadata_sid(const Metadata& metadata) {
    auto it = metadata.find("sid");
    if (it == metadata.end() || it->second.empty()) return std::nullopt;
    return it->second;
}

std::optional<std::string> metadata_shard_key(const Metadata& metadata) {
    auto it = metadata.find("shard_key");
    if (it != metadata.end() && !it->second.empty()) return it->second;
    it = metadata.find("tenant_id");
    if (it != metadata.end() && !it->second.empty()) return it->second;
    return std::nullopt;
}

std::optional<creek::v1::Endpoint> find_live_endpoint(
    const std::vector<creek::v1::Endpoint>& endpoints, const std::string& id) {
    for (const auto& ep : endpoints) {
        if (ep.endpoint_id() == id) {
            if (ep.alive()) return ep;
            return std::nullopt;
        }
    }
    return std::nullopt;
}

}

std::optional<creek::v1::Endpoint> StickyBalancer::pick(
    std::string_view service, const Metadata& metadata,
    const std::vector<creek::v1::Endpoint>& endpoints,
    SteadyClock::time_point now) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (endpoints.empty()) return std::nullopt;

    for (auto& kv : entries_) {
        auto& e = kv.second;
        if (!e.lru && (now - e.last_used) >= ttl_) {
            e.lru = true;
            e.lru_since = now;
        }
    }
    while (true) {
        std::size_t lru_count = 0;
        auto oldest = entries_.end();
        for (auto it = entries_.begin(); it != entries_.end(); ++it) {
            if (it->second.lru) {
                ++lru_count;
                if (oldest == entries_.end() ||
                    it->second.lru_since < oldest->second.lru_since) {
                    oldest = it;
                }
            }
        }
        if (lru_count <= capacity_) break;
        entries_.erase(oldest);
    }

    auto do_pick = [&]() -> creek::v1::Endpoint {
        std::size_t idx = static_cast<std::size_t>(round_robin_ % endpoints.size());
        ++round_robin_;
        return endpoints[idx];
    };

    bool sticky = metadata_sticky(metadata);
    auto sid = metadata_sid(metadata);
    auto shard = metadata_shard_key(metadata);

    if (shard.has_value()) {
        std::size_t hash = std::hash<std::string>{}(*shard);
        std::size_t idx = hash % endpoints.size();
        return endpoints[idx];
    }

    if (!sticky || !sid.has_value()) {
        return do_pick();
    }

    std::string key;
    key.reserve(service.size() + 1 + sid->size());
    key.append(service.data(), service.size());
    key.push_back('|');
    key.append(*sid);

    auto it = entries_.find(key);
    if (it != entries_.end()) {
        if (!it->second.lru) {
            auto ep = find_live_endpoint(endpoints, it->second.endpoint_id);
            if (ep.has_value()) {
                it->second.last_used = now;
                return ep;
            }
        }
        auto replacement = do_pick();
        it->second.endpoint_id = replacement.endpoint_id();
        it->second.last_used = now;
        it->second.lru = false;
        it->second.lru_since = SteadyClock::time_point{};
        return replacement;
    }

    auto fresh = do_pick();
    Entry entry;
    entry.endpoint_id = fresh.endpoint_id();
    entry.last_used = now;
    entry.lru_since = SteadyClock::time_point{};
    entry.lru = false;
    entries_.emplace(key, std::move(entry));
    return fresh;
}

void StickyBalancer::invalidate(std::string_view endpoint_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string id(endpoint_id);
    for (auto it = entries_.begin(); it != entries_.end();) {
        if (it->second.endpoint_id == id) {
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
}

std::size_t StickyBalancer::active_size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t count = 0;
    for (const auto& kv : entries_) {
        if (!kv.second.lru) ++count;
    }
    return count;
}

std::size_t StickyBalancer::lru_size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t count = 0;
    for (const auto& kv : entries_) {
        if (kv.second.lru) ++count;
    }
    return count;
}

void StickyBalancer::set_ttl(std::chrono::milliseconds ttl) {
    std::lock_guard<std::mutex> lk(mutex_);
    ttl_ = ttl;
    entries_.clear();
}

void StickyBalancer::set_shard_key(const std::string& key) {
    (void)key;
}

}
