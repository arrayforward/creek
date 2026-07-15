#include "creek/redis.hpp"

#include <hiredis/hiredis.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace creek {

namespace {

struct ReplyGuard {
    redisReply* reply{nullptr};

    explicit ReplyGuard(redisReply* r) : reply(r) {}

    ~ReplyGuard() {
        if (reply) freeReplyObject(reply);
    }

    ReplyGuard(const ReplyGuard&) = delete;
    ReplyGuard& operator=(const ReplyGuard&) = delete;

    ReplyGuard(ReplyGuard&& other) noexcept : reply(other.reply) {
        other.reply = nullptr;
    }

    ReplyGuard& operator=(ReplyGuard&& other) noexcept {
        if (this != &other) {
            if (reply) freeReplyObject(reply);
            reply = other.reply;
            other.reply = nullptr;
        }
        return *this;
    }
};

}

RedisClient::RedisClient(RedisOptions options, std::string self_id)
    : options_(std::move(options)), self_id_(std::move(self_id)) {
    std::lock_guard<std::mutex> lk(mutex_);
    ctx_ = connect_locked_();
    if (!ctx_) {
        throw std::runtime_error("redis: failed to connect to " + options_.host + ":" +
                                 std::to_string(options_.port));
    }
}

RedisClient::~RedisClient() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
}

void RedisClient::connect() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
    ctx_ = connect_locked_();
}

bool RedisClient::ensure_locked_() {
    if (ctx_ && !ctx_->err) return true;
    if (ctx_) {
        redisFree(ctx_);
        ctx_ = nullptr;
    }
    ctx_ = connect_locked_();
    return ctx_ != nullptr;
}

redisContext* RedisClient::connect_locked_() {
    struct timeval timeout{2, 0};
    redisContext* c = redisConnectWithTimeout(options_.host.c_str(), options_.port, timeout);
    if (!c) return nullptr;
    if (c->err) {
        redisFree(c);
        return nullptr;
    }
    redisSetTimeout(c, timeout);
    if (!options_.password.empty()) {
        ReplyGuard g{static_cast<redisReply*>(
            !options_.user.empty()
                ? redisCommand(c, "AUTH %s %s", options_.user.c_str(), options_.password.c_str())
                : redisCommand(c, "AUTH %s", options_.password.c_str()))};
        if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
            redisFree(c);
            return nullptr;
        }
    }
    return c;
}

bool RedisClient::ping() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) return false;
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "PING"))};
    if (!g.reply) return false;
    return g.reply->type == REDIS_REPLY_STATUS &&
           std::string(g.reply->str, g.reply->len) == "PONG";
}

bool RedisClient::health_ok() {
    return ping();
}

bool RedisClient::is_connected() {
    std::lock_guard<std::mutex> lk(mutex_);
    return ctx_ != nullptr && ctx_->err == 0;
}

void RedisClient::register_node(const std::string& listen_address) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) {
        throw std::runtime_error("redis: not connected for register_node");
    }
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HSET %s %s %s",
                                                      options_.key.c_str(),
                                                      self_id_.c_str(),
                                                      listen_address.c_str()))};
    if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis: HSET failed for register_node");
    }
    registered_address_ = listen_address;
    registered_leaf_address_.clear();
    registered_parent_.clear();
}

void RedisClient::register_leaf(const std::string& leaf_address, const std::string& parent_node_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) {
        throw std::runtime_error("redis: not connected for register_leaf");
    }
    std::string leaves_key = options_.key + ":leaves";
    std::string field = parent_node_id + ":" + self_id_;
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HSET %s %s %s",
                                                      leaves_key.c_str(),
                                                      field.c_str(),
                                                      leaf_address.c_str()))};
    if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis: HSET failed for register_leaf");
    }
    registered_leaf_address_ = leaf_address;
    registered_parent_ = parent_node_id;
    registered_address_.clear();
}

std::unordered_map<std::string, std::string> RedisClient::fetch_nodes() {
    std::lock_guard<std::mutex> lk(mutex_);
    std::unordered_map<std::string, std::string> result;
    if (!ensure_locked_()) return result;
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HGETALL %s", options_.key.c_str()))};
    if (!g.reply || g.reply->type != REDIS_REPLY_ARRAY) return result;
    for (std::size_t i = 0; i + 1 < g.reply->elements; i += 2) {
        redisReply* f = g.reply->element[i];
        redisReply* v = g.reply->element[i + 1];
        if (!f || !v || !f->str || !v->str) continue;
        std::string field(f->str, f->len);
        std::string value(v->str, v->len);
        result.emplace(std::move(field), std::move(value));
    }
    return result;
}

std::string RedisClient::hget(const std::string& hash_key, const std::string& field) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) {
        throw std::runtime_error("redis: not connected for hget");
    }
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HGET %s %s",
                                                      hash_key.c_str(),
                                                      field.c_str()))};
    if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis: HGET failed");
    }
    if (g.reply->type == REDIS_REPLY_NIL) return {};
    if (g.reply->str == nullptr) return {};
    return std::string(g.reply->str, g.reply->len);
}

void RedisClient::hdel(const std::string& hash_key, const std::string& field) {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) {
        throw std::runtime_error("redis: not connected for hdel");
    }
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HDEL %s %s",
                                                      hash_key.c_str(),
                                                      field.c_str()))};
    if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
        throw std::runtime_error("redis: HDEL failed");
    }
}

std::unordered_map<std::string, std::string> RedisClient::fetch_leaves_for_node(const std::string& node_id) {
    std::lock_guard<std::mutex> lk(mutex_);
    std::unordered_map<std::string, std::string> result;
    if (!ensure_locked_()) return result;
    std::string leaves_key = options_.key + ":leaves";
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HGETALL %s", leaves_key.c_str()))};
    if (!g.reply || g.reply->type != REDIS_REPLY_ARRAY) return result;
    std::string prefix = node_id + ":";
    for (std::size_t i = 0; i + 1 < g.reply->elements; i += 2) {
        redisReply* f = g.reply->element[i];
        redisReply* v = g.reply->element[i + 1];
        if (!f || !v || !f->str || !v->str) continue;
        std::string field(f->str, f->len);
        std::string value(v->str, v->len);
        if (field.compare(0, prefix.size(), prefix) == 0) {
            std::string leaf_id = field.substr(prefix.size());
            result.emplace(std::move(leaf_id), std::move(value));
        }
    }
    return result;
}

void RedisClient::lease_renew() {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!ensure_locked_()) {
        throw std::runtime_error("redis: not connected for lease_renew");
    }
    if (!registered_address_.empty()) {
        ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HSET %s %s %s",
                                                          options_.key.c_str(),
                                                          self_id_.c_str(),
                                                          registered_address_.c_str()))};
        if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("redis: HSET failed in lease_renew for node");
        }
        return;
    }
    if (!registered_leaf_address_.empty()) {
        std::string leaves_key = options_.key + ":leaves";
        std::string field = registered_parent_ + ":" + self_id_;
        ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "HSET %s %s %s",
                                                          leaves_key.c_str(),
                                                          field.c_str(),
                                                          registered_leaf_address_.c_str()))};
        if (!g.reply || g.reply->type == REDIS_REPLY_ERROR) {
            throw std::runtime_error("redis: HSET failed in lease_renew for leaf");
        }
        return;
    }
    ReplyGuard g{static_cast<redisReply*>(redisCommand(ctx_, "PING"))};
    if (!g.reply || g.reply->type != REDIS_REPLY_STATUS) {
        throw std::runtime_error("redis: PING failed in lease_renew");
    }
}

}