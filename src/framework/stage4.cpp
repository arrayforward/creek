#include "creek/framework/stage4.hpp"

#include <chrono>
#include <functional>
#include <utility>

namespace creek::framework {

Stage4Executor::Stage4Executor(CopyChannel<Message>* output_channel)
    : m_output_channel(output_channel)
{
}

Stage4Executor::~Stage4Executor()
{
    stop();
}

void Stage4Executor::execute(ChangeSet cs)
{
    for (auto& msg : cs.new_messages) {
        m_output_channel->send(std::move(msg));
    }

    std::size_t call_count = cs.external_calls.size();
    for (auto& call : cs.external_calls) {
        m_work_queue.send(std::move(call));
    }
    m_pending.fetch_add(call_count, std::memory_order_release);
}

void Stage4Executor::start(std::size_t worker_threads)
{
    if (m_running.load(std::memory_order_acquire)) {
        return;
    }
    m_running.store(true, std::memory_order_release);
    m_workers.reserve(worker_threads);
    for (std::size_t i = 0; i < worker_threads; ++i) {
        m_workers.emplace_back(&Stage4Executor::worker_loop, this);
    }
}

void Stage4Executor::stop()
{
    m_running.store(false, std::memory_order_release);
    m_work_queue.close();

    for (auto& t : m_workers) {
        if (t.joinable()) {
            t.join();
        }
    }
    m_workers.clear();
}

void Stage4Executor::set_call_logger(Logger logger)
{
    m_call_logger = std::move(logger);
}

void Stage4Executor::worker_loop()
{
    while (m_running.load(std::memory_order_acquire)) {
        auto call = m_work_queue.recv_for(std::chrono::milliseconds(100));
        if (!call.has_value()) {
            continue;
        }
        try {
            if (m_call_logger) {
                m_call_logger("stage4: executing external call");
            }
            (*call)();
            m_pending.fetch_sub(1, std::memory_order_release);
        } catch (const std::exception& e) {
            if (m_call_logger) {
                m_call_logger(std::string("stage4: call exception: ") + e.what());
            }
            m_pending.fetch_sub(1, std::memory_order_release);
        } catch (...) {
            if (m_call_logger) {
                m_call_logger("stage4: call unknown exception");
            }
            m_pending.fetch_sub(1, std::memory_order_release);
        }
    }
}

} // namespace creek::framework
