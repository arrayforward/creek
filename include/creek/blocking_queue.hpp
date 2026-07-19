#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

// Note: This queue already uses value semantics (moves items in/out).
// For CSP-style deep-copy channels with buffer management, use
// creek::framework::CopyChannel from framework/channel.hpp.
// This class is kept for backward compatibility with existing code.

namespace creek {

template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(std::size_t capacity = 4096) : capacity_(capacity) {}

    void push(T item) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return queue_.size() < capacity_ || closed_; });
        if (closed_) return;
        queue_.push(std::move(item));
        not_empty_.notify_one();
    }

    bool try_push(T item) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_ || queue_.size() >= capacity_) return false;
        queue_.push(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    std::optional<T> take() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return !queue_.empty() || closed_; });
        if (closed_ && queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    template <typename Rep, typename Period>
    std::optional<T> take_for(const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!not_empty_.wait_for(lock, timeout, [this] { return !queue_.empty() || closed_; })) {
            return std::nullopt;
        }
        if (closed_ && queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    std::optional<T> poll() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) return std::nullopt;
        T item = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return item;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    std::queue<T> queue_;
    std::size_t capacity_;
    bool closed_{false};
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
};

} // namespace creek
