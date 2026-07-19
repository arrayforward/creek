#pragma once

#include "creek/framework/message.hpp"

#include <functional>
#include <string>
#include <vector>

namespace creek::framework {

using OutgoingCall = std::function<void()>;

struct ChangeSet {
    std::vector<Message> new_messages;
    std::vector<OutgoingCall> external_calls;

    bool empty() const { return new_messages.empty() && external_calls.empty(); }
    void clear() {
        new_messages.clear();
        external_calls.clear();
    }
};

} // namespace creek::framework
