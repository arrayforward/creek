#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>

namespace creek::framework {

using TimePoint = std::chrono::steady_clock::time_point;

template <typename T>
class CopyChannel {
public:
    explicit CopyChannel(std::size_t m_baselinebuffer = 16, std::size_t max_buffer = 4096);

    CopyChannel(const CopyChannel&) = delete;
    CopyChannel& operator=(const CopyChannel&) = delete;

    void send(const T& item);
    void send(T&& item);
    bool try_send(const T& item);
    bool try_send(T&& item);

    std::optional<T> recv(std::chrono::milliseconds timeout = std::chrono::milliseconds(0));
    std::optional<T> recv_for(std::chrono::milliseconds timeout);
    std::optional<T> poll();

    std::size_t drain_all(std::vector<T>& out);

    void close();
    bool is_closed() const;
    std::size_t depth() const;
    std::size_t capacity() const;

private:
    struct Node {
        T data;
        Node* next{nullptr};
    };

    void idle_shrink_locked();

    Node* m_head{nullptr};
    Node* m_tail{nullptr};
    std::size_t m_size{0};
    std::size_t m_baseline;
    std::size_t m_max_nodes;
    TimePoint m_last_idle_at{};
    bool m_closed{false};
    mutable std::mutex m_mutex;
    std::condition_variable m_not_empty;
};

template <typename T>
CopyChannel<T>::CopyChannel(std::size_t m_baselinebuffer, std::size_t max_buffer)
    : m_baseline(m_baselinebuffer), m_max_nodes(max_buffer) {}

template <typename T>
void CopyChannel<T>::send(const T& item) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_closed) return;
    auto* node = new Node{item};
    if (m_tail) {
        m_tail->next = node;
    } else {
        m_head = node;
    }
    m_tail = node;
    ++m_size;
    m_not_empty.notify_one();
}

template <typename T>
void CopyChannel<T>::send(T&& item) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_closed) return;
    auto* node = new Node{std::move(item)};
    if (m_tail) {
        m_tail->next = node;
    } else {
        m_head = node;
    }
    m_tail = node;
    ++m_size;
    m_not_empty.notify_one();
}

template <typename T>
bool CopyChannel<T>::try_send(const T& item) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed || m_size >= m_max_nodes) return false;
    auto* node = new Node{item};
    if (m_tail) {
        m_tail->next = node;
    } else {
        m_head = node;
    }
    m_tail = node;
    ++m_size;
    m_not_empty.notify_one();
    return true;
}

template <typename T>
bool CopyChannel<T>::try_send(T&& item) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed || m_size >= m_max_nodes) return false;
    auto* node = new Node{std::move(item)};
    if (m_tail) {
        m_tail->next = node;
    } else {
        m_head = node;
    }
    m_tail = node;
    ++m_size;
    m_not_empty.notify_one();
    return true;
}

template <typename T>
std::optional<T> CopyChannel<T>::recv(std::chrono::milliseconds timeout) {
    if (timeout.count() == 0) return poll();
    return recv_for(timeout);
}

template <typename T>
std::optional<T> CopyChannel<T>::recv_for(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_not_empty.wait_for(lock, timeout, [this] { return m_head != nullptr || m_closed; })) {
        return std::nullopt;
    }
    if (m_head == nullptr) return std::nullopt;
    Node* node = m_head;
    m_head = node->next;
    if (m_head == nullptr) m_tail = nullptr;
    --m_size;
    T result = std::move(node->data);
    delete node;
    return result;
}

template <typename T>
std::optional<T> CopyChannel<T>::poll() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_head == nullptr) return std::nullopt;
    Node* node = m_head;
    m_head = node->next;
    if (m_head == nullptr) m_tail = nullptr;
    --m_size;
    T result = std::move(node->data);
    delete node;
    return result;
}

template <typename T>
std::size_t CopyChannel<T>::drain_all(std::vector<T>& out) {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::size_t count = m_size;
    out.reserve(out.size() + count);
    Node* node = m_head;
    while (node) {
        out.push_back(std::move(node->data));
        Node* next = node->next;
        delete node;
        node = next;
    }
    m_head = m_tail = nullptr;
    m_size = 0;
    return count;
}

template <typename T>
void CopyChannel<T>::close() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_closed = true;
    Node* node = m_head;
    while (node) {
        Node* next = node->next;
        delete node;
        node = next;
    }
    m_head = m_tail = nullptr;
    m_size = 0;
    m_not_empty.notify_all();
}

template <typename T>
bool CopyChannel<T>::is_closed() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_closed;
}

template <typename T>
std::size_t CopyChannel<T>::depth() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_size;
}

template <typename T>
std::size_t CopyChannel<T>::capacity() const {
    return m_max_nodes;
}

// Convenience alias for migrating from creek::BlockingQueue
// Provides CSP-style deep-copy semantics with buffer management
template <typename T>
using BlockingChannel = CopyChannel<T>;

} // namespace creek::framework
