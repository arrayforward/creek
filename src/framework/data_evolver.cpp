#include "creek/framework/data_evolver.hpp"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace creek::framework {

void DataEvolver::register_component(std::string name, EvolveFn evolve_fn)
{
    m_names.push_back(std::move(name));
    m_functions.push_back(std::move(evolve_fn));
}

void DataEvolver::unregister_component(const std::string& name)
{
    for (std::size_t i = 0; i < m_names.size(); ++i) {
        if (m_names[i] == name) {
            m_names.erase(m_names.begin() + static_cast<std::ptrdiff_t>(i));
            m_functions.erase(m_functions.begin() + static_cast<std::ptrdiff_t>(i));
            return;
        }
    }
}

ChangeSet DataEvolver::evolve_once(Blackboard* blackboard)
{
    auto changed_keys = blackboard->swap_changed_keys();

    ChangeSet accumulated;
    if (changed_keys.empty()) {
        return accumulated;
    }

    std::vector<std::string> keys_vec(changed_keys.begin(), changed_keys.end());

    for (std::size_t i = 0; i < m_functions.size(); ++i) {
        ChangeSet cs = m_functions[i](blackboard, keys_vec);
        for (auto& msg : cs.new_messages) {
            accumulated.new_messages.push_back(std::move(msg));
        }
        for (auto& call : cs.external_calls) {
            accumulated.external_calls.push_back(std::move(call));
        }
    }

    return accumulated;
}

} // namespace creek::framework
