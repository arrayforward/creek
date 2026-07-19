#pragma once

#include "creek/framework/time_source.hpp"
#include "creek/framework/task.hpp"
#include "creek/framework/logger_adapter.hpp"
#include "creek/framework/channel.hpp"
#include "creek/framework/timer.hpp"
#include "creek/framework/blackboard.hpp"
#include "creek/framework/message.hpp"
#include "creek/framework/change_set.hpp"
#include "creek/framework/heartbeat.hpp"
#include "creek/framework/data_evolver.hpp"
#include "creek/framework/stage4.hpp"
#include "creek/framework/reactor.hpp"
#include "creek/framework/metrics.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace creek::framework {

class Framework {
public:
    explicit Framework(ReactorConfig reactor_cfg, TimeSource* time_source = nullptr);
    ~Framework();

    Framework(const Framework&) = delete;
    Framework& operator=(const Framework&) = delete;

    void start();
    void stop();
    bool is_running() const;

    void register_input_channel(CopyChannel<Message>* channel);
    MessageHeartbeat& heartbeat() { return m_heartbeat; }
    DataEvolver& data_evolver() { return m_data_evolver; }
    Stage4Executor& stage4_executor() { return *m_stage4; }
    Blackboard& blackboard() { return m_blackboard; }
    Reactor& reactor() { return m_reactor; }
    MetricsCollector& metrics() { return m_metrics; }

    CopyChannel<Message>& output_channel() { return m_output_channel; }
    CopyChannel<Message>& new_message_channel() { return m_new_message_channel; }
    CopyChannel<Message>& input_channel() { return *m_input_channel; }

    void run_one_cycle();

    void set_batch_processor(BatchProcessor processor);

    MetricsCollector::PressureSnapshot pressure() const;

private:
    void on_heartbeat_timer();

    Reactor m_reactor;
    Blackboard m_blackboard;
    MetricsCollector m_metrics;

    CopyChannel<Message> m_input_buffer;
    CopyChannel<Message>* m_input_channel{&m_input_buffer};

    MessageHeartbeat m_heartbeat;
    DataEvolver m_data_evolver;
    std::unique_ptr<Stage4Executor> m_stage4;
    CopyChannel<Message> m_output_channel;
    CopyChannel<Message> m_new_message_channel;

    BatchProcessor m_processor;
    std::atomic<bool> m_running{false};
};

} // namespace creek::framework
