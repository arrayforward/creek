#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

struct redisContext;

namespace creek {

struct RedisOptions {
    std::string host{"127.0.0.1"};
    std::uint16_t port{};
    std::string user;
    std::string password;
    std::string key{"creek:nodes"};
};

class RedisClient {
public:
    RedisClient(RedisOptions options, std::string self_id);
    ~RedisClient();

    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    void connect();
    bool ping();
    bool health_ok();
    bool is_connected();
    void register_node(const std::string& listen_address);
    void register_leaf(const std::string& leaf_address, const std::string& parent_node_id);
    std::unordered_map<std::string, std::string> fetch_nodes();
    std::unordered_map<std::string, std::string> fetch_leaves_for_node(const std::string& node_id);
    std::string hget(const std::string& hash_key, const std::string& field);
    void hdel(const std::string& hash_key, const std::string& field);
    void lease_renew();

private:
    redisContext* connect_locked_();
    bool ensure_locked_();

    RedisOptions options_;
    std::string self_id_;
    std::string registered_address_;
    std::string registered_leaf_address_;
    std::string registered_parent_;
    std::mutex mutex_;
    redisContext* ctx_{nullptr};
};

}