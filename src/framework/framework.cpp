#include "creek/framework/framework.hpp"
#include "creek/framework/logger_adapter.hpp"

#include <chrono>
#include <memory>
#include <new>
#include <utility>

namespace creek::framework {

Framework::Framework(ReactorConfig reactor_cfg, TimeSource* time_source)
    : m_reactor(std::move(reactor_cfg), time_source)
    , m_heartbeat(&m_input_buffer, &m_blackboard)
    , m_stage4(std::make_unique<Stage4Executor>(&m_new_message_channel))
{
    m_input_channel = &m_input_buffer;
}

Framework::~Framework()
{
    stop();
}

void Framework::start()
{
    if (m_running.load(std::memory_order_acquire)) {
        return;
    }
    m_running.store(true, std::memory_order_release);
    m_stage4->start(2);
    m_reactor.start();
    m_reactor.schedule_timer_in(
        Task("heartbeat_tick", [this] { on_heartbeat_timer(); }, TaskPriority::High, false),
        std::chrono::milliseconds(20));
}

void Framework::stop()
{
    m_running.store(false, std::memory_order_release);
    m_reactor.stop();
    m_stage4->stop();
}

bool Framework::is_running() const
{
    return m_running.load(std::memory_order_acquire);
}

void Framework::register_input_channel(CopyChannel<Message>* channel)
{
    m_input_channel = channel;
    m_heartbeat.~MessageHeartbeat();
    new (&m_heartbeat) MessageHeartbeat(m_input_channel, &m_blackboard);
    if (m_processor) {
        m_heartbeat.set_processor(m_processor);
    }
}

void Framework::set_batch_processor(BatchProcessor processor)
{
    m_processor = std::move(processor);
    m_heartbeat.set_processor(m_processor);
}

void Framework::run_one_cycle()
{
    auto beat_start = std::chrono::steady_clock::now();

    ChangeSet cs = m_heartbeat.beat();

    ChangeSet evo_cs = m_data_evolver.evolve_once(&m_blackboard);

    for (auto& msg : evo_cs.new_messages) {
        cs.new_messages.push_back(std::move(msg));
    }
    for (auto& call : evo_cs.external_calls) {
        cs.external_calls.push_back(std::move(call));
    }

    m_stage4->execute(std::move(cs));

    std::vector<Message> recycled;
    m_new_message_channel.drain_all(recycled);
    for (auto& msg : recycled) {
        m_input_buffer.send(std::move(msg));
    }

    auto beat_end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        beat_end - beat_start);
    m_metrics.record_heartbeat(elapsed);

    m_metrics.set_io_queue_depth(
        static_cast<std::int64_t>(m_reactor.io_queue_depth()));
    m_metrics.set_cpu_queue_depth(
        static_cast<std::int64_t>(m_reactor.cpu_queue_depth()));
}

void Framework::on_heartbeat_timer()
{
    if (!m_running.load(std::memory_order_acquire)) {
        return;
    }
    run_one_cycle();

    m_reactor.schedule_timer_in(
        Task("heartbeat_tick", [this] { on_heartbeat_timer(); }, TaskPriority::High, false),
        std::chrono::milliseconds(20));
}

MetricsCollector::PressureSnapshot Framework::pressure() const
{
    return m_metrics.pressure();
}

} // namespace creek::framework
