#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>

namespace creek::framework {

enum class DataPolicy : std::uint8_t {
    ThreadOwned = 0,
    SharedLocked = 1,
    ValueCopy = 2
};

class Blackboard {
public:
    Blackboard() = default;
    Blackboard(const Blackboard&) = delete;
    Blackboard& operator=(const Blackboard&) = delete;

    template <typename T>
    T* register_value(const std::string& key, T value, DataPolicy policy = DataPolicy::SharedLocked);

    template <typename T>
    T* get(const std::string& key);

    template <typename T>
    std::optional<T> get_copy(const std::string& key) const;

    void mark_changed(const std::string& key);

    const std::unordered_set<std::string>& changed_keys() const { return m_changed_keys; }
    void clear_changed_keys() { m_changed_keys.clear(); }

    std::unordered_set<std::string> swap_changed_keys();

    std::size_t size() const;
    bool contains(const std::string& key) const;
    void remove(const std::string& key);

    class Snapshot;
    Snapshot export_snapshot() const;

private:
    struct Entry {
        std::type_index type{typeid(void)};
        DataPolicy policy{DataPolicy::SharedLocked};
        std::shared_ptr<void> data;
        mutable std::mutex mtx;
    };

    mutable std::mutex m_registry_mutex;
    std::unordered_map<std::string, std::unique_ptr<Entry>> m_registry;
    std::unordered_set<std::string> m_changed_keys;
};

template <typename T>
T* Blackboard::register_value(const std::string& key, T value, DataPolicy policy) {
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    auto entry = std::make_unique<Entry>();
    entry->type = std::type_index(typeid(T));
    entry->policy = policy;
    entry->data = std::make_shared<T>(std::move(value));
    auto* raw = static_cast<T*>(entry->data.get());
    m_registry[key] = std::move(entry);
    return raw;
}

template <typename T>
T* Blackboard::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    auto it = m_registry.find(key);
    if (it == m_registry.end()) return nullptr;
    if (it->second->type != std::type_index(typeid(T))) return nullptr;
    return static_cast<T*>(it->second->data.get());
}

template <typename T>
std::optional<T> Blackboard::get_copy(const std::string& key) const {
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    auto it = m_registry.find(key);
    if (it == m_registry.end()) return std::nullopt;
    if (it->second->type != std::type_index(typeid(T))) return std::nullopt;
    std::lock_guard<std::mutex> data_lock(it->second->mtx);
    return *static_cast<const T*>(it->second->data.get());
}

} // namespace creek::framework
