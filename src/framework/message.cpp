#include "creek/framework/message.hpp"

#include <atomic>

namespace creek::framework {

std::atomic<std::uint64_t> Message::m_idcounter_{1};

MessageId Message::next_id()
{
    return m_idcounter_.fetch_add(1, std::memory_order_relaxed);
}

void Message::reset_id_counter(MessageId base)
{
    m_idcounter_.store(base, std::memory_order_relaxed);
}

} // namespace creek::framework
