#include "creek/runtime.hpp"

// The implementation was split into per-component translation units:
//   src/node/node_runtime.cpp   - NodeRuntime::Impl method definitions
//   src/leaf/leaf_runtime.cpp   - LeafRuntime::Impl method definitions
//   src/rpc/*.cpp               - gRPC service method definitions
// This file keeps the public NodeRuntime/LeafRuntime pimpl forwarding
// methods, which need the complete Impl types.

#include "node/node_runtime_impl.hpp"
#include "leaf/leaf_runtime_impl.hpp"

#include <utility>

namespace creek {

NodeRuntime::NodeRuntime(NodeConfig config) : m_impl(std::make_unique<Impl>(std::move(config))) {}
NodeRuntime::~NodeRuntime() = default;
bool NodeRuntime::start() { return m_impl->start(); }
void NodeRuntime::stop() { m_impl->stop(); }
void NodeRuntime::set_framework(framework::Framework* fw) { m_impl->set_framework(fw); }
framework::ChangeSet NodeRuntime::process_batch(const std::vector<framework::Message>& batch) {
    return m_impl->process_batch(batch);
}

LeafRuntime::LeafRuntime(LeafConfig config) : m_impl(std::make_unique<Impl>(std::move(config))) {}
LeafRuntime::~LeafRuntime() = default;
bool LeafRuntime::start() { return m_impl->start(); }
void LeafRuntime::stop() { m_impl->stop(); }
void LeafRuntime::set_framework(framework::Framework* fw) { m_impl->set_framework(fw); }
framework::ChangeSet LeafRuntime::process_batch(const std::vector<framework::Message>& batch) {
    return m_impl->process_batch(batch);
}

}
