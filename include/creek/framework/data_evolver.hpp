#pragma once

#include "creek/framework/blackboard.hpp"
#include "creek/framework/change_set.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace creek::framework {

class DataEvolver {
public:
    using EvolveFn = std::function<ChangeSet(Blackboard*, const std::vector<std::string>& changed_keys)>;

    DataEvolver() = default;

    void register_component(std::string name, EvolveFn evolve_fn);
    void unregister_component(const std::string& name);

    ChangeSet evolve_once(Blackboard* blackboard);

    const std::vector<std::string>& component_names() const { return m_names; }
    std::size_t component_count() const { return m_names.size(); }

private:
    std::vector<std::string> m_names;
    std::vector<EvolveFn> m_functions;
};

} // namespace creek::framework
