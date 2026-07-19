#include "creek/framework/blackboard.hpp"

#include <memory>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <utility>

namespace creek::framework {

class Blackboard::Snapshot {
public:
    struct DataEntry {
        std::type_index type{typeid(void)};
        std::shared_ptr<void> data;
    };

    std::unordered_map<std::string, DataEntry> entries;
};

void Blackboard::mark_changed(const std::string& key)
{
    m_changed_keys.insert(key);
}

std::unordered_set<std::string> Blackboard::swap_changed_keys()
{
    std::unordered_set<std::string> result;
    result.swap(m_changed_keys);
    return result;
}

std::size_t Blackboard::size() const
{
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    return m_registry.size();
}

bool Blackboard::contains(const std::string& key) const
{
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    return m_registry.find(key) != m_registry.end();
}

void Blackboard::remove(const std::string& key)
{
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    m_registry.erase(key);
}

Blackboard::Snapshot Blackboard::export_snapshot() const
{
    std::lock_guard<std::mutex> lock(m_registry_mutex);
    Snapshot snap;
    for (const auto& [key, entry] : m_registry) {
        snap.entries.emplace(key, Snapshot::DataEntry{entry->type, entry->data});
    }
    return snap;
}

} // namespace creek::framework
