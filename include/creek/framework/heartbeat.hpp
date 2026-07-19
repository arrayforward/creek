#pragma once

#include "creek/framework/change_set.hpp"
#include "creek/framework/channel.hpp"
#include "creek/framework/message.hpp"

#include <functional>
#include <memory>
#include <vector>

namespace creek::framework {

class Blackboard;

using BatchProcessor = std::function<ChangeSet(const std::vector<Message>&)>;

class MessageHeartbeat {
public:
    MessageHeartbeat(CopyChannel<Message>* input_channel, Blackboard* blackboard);

    MessageHeartbeat(const MessageHeartbeat&) = delete;
    MessageHeartbeat& operator=(const MessageHeartbeat&) = delete;

    void set_processor(BatchProcessor processor);
    ChangeSet beat();

    void set_generate_tick(bool enabled) { m_generate_tick = enabled; }
    bool generate_tick() const { return m_generate_tick; }

    std::size_t m_lastbatch_size() const { return m_last_batch_size; }

private:
    CopyChannel<Message>* m_input_channel;
    Blackboard* m_blackboard;
    BatchProcessor m_processor;
    std::size_t m_last_batch_size{0};
    bool m_generate_tick{true};
};

} // namespace creek::framework
