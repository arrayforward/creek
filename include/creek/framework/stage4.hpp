#pragma once

#include "creek/framework/change_set.hpp"
#include "creek/framework/channel.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

namespace creek::framework {

class Stage4Executor {
public:
    Stage4Executor(CopyChannel<Message>* output_channel);

    Stage4Executor(const Stage4Executor&) = delete;
    Stage4Executor& operator=(const Stage4Executor&) = delete;
    ~Stage4Executor();

    void execute(ChangeSet cs);

    void start(std::size_t worker_threads = 1);
    void stop();

    std::size_t m_pendingwork() const { return m_pending.load(); }

    using Logger = std::function<void(const std::string&)>;
    void set_call_logger(Logger logger);

private:
    void worker_loop();

    CopyChannel<Message>* m_output_channel;
    CopyChannel<OutgoingCall> m_work_queue;
    std::vector<std::thread> m_workers;
    std::atomic<bool> m_running{false};
    std::atomic<std::size_t> m_pending{0};
    Logger m_call_logger;
};

} // namespace creek::framework
