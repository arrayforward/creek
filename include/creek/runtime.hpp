#pragma once

// Backward-compatible umbrella header. The runtime API was split into
// per-role headers; existing code that includes "creek/runtime.hpp"
// continues to work unchanged.
#include "creek/node/node_runtime.hpp"
#include "creek/leaf/leaf_runtime.hpp"
