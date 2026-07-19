#include "creek/framework/heartbeat.hpp"
#include "creek/framework/blackboard.hpp"

#include <utility>
#include <vector>

namespace creek::framework {

MessageHeartbeat::MessageHeartbeat(CopyChannel<Message>* input_channel, Blackboard* blackboard)
    : m_input_channel(input_channel)
    , m_blackboard(blackboard)
{
}

void MessageHeartbeat::set_processor(BatchProcessor processor)
{
    m_processor = std::move(processor);
}

ChangeSet MessageHeartbeat::beat()
{
    m_blackboard->clear_changed_keys();

    std::vector<Message> batch;
    m_last_batch_size = m_input_channel->drain_all(batch);

    if (m_generate_tick) {
        batch.emplace_back(MessageKind::TimerTick, "heartbeat");
    }

    ChangeSet cs;
    if (m_processor) {
        cs = m_processor(batch);
    }

    return cs;
}

} // namespace creek::framework
