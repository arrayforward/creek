#include "creek/framework/time_source.hpp"
#include "creek/framework/task.hpp"
#include "creek/framework/channel.hpp"
#include "creek/framework/timer.hpp"
#include "creek/framework/blackboard.hpp"
#include "creek/framework/message.hpp"
#include "creek/framework/heartbeat.hpp"
#include "creek/framework/data_evolver.hpp"
#include "creek/framework/stage4.hpp"
#include "creek/framework/change_set.hpp"
#include "creek/framework/framework.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace cf = creek::framework;

// ============================================================================
// 1. TimeSource tests
// ============================================================================

TEST(TimeSourceTest, RealSteadyNowAdvances)
{
    cf::RealTimeSource rts;
    auto t1 = rts.steady_now();
    auto t2 = rts.steady_now();
    EXPECT_LE(t1, t2);
}

TEST(TimeSourceTest, VirtualAdvanceIncrementsSteadyNow)
{
    cf::VirtualTimeSource vts;
    auto t0 = vts.steady_now();
    vts.advance(std::chrono::milliseconds(10));
    auto t1 = vts.steady_now();
    EXPECT_EQ(t1 - t0, std::chrono::milliseconds(10));
}

TEST(TimeSourceTest, VirtualResetWorks)
{
    cf::VirtualTimeSource vts;
    vts.advance(std::chrono::milliseconds(100));
    auto base = std::chrono::steady_clock::time_point(std::chrono::milliseconds(500));
    vts.reset(base);
    EXPECT_EQ(vts.steady_now(), base);
}

TEST(TimeSourceTest, VirtualSystemNowMapping)
{
    cf::VirtualTimeSource vts;
    vts.advance(std::chrono::milliseconds(42));
    auto steady = vts.steady_now();
    auto sys = vts.system_now();
    auto steady_ms = std::chrono::duration_cast<std::chrono::milliseconds>(steady.time_since_epoch());
    auto sys_ms = std::chrono::duration_cast<std::chrono::milliseconds>(sys.time_since_epoch());
    EXPECT_EQ(steady_ms.count(), sys_ms.count());
}

// ============================================================================
// 2. Task tests
// ============================================================================

TEST(TaskTest, CreatesWithUniqueIds)
{
    cf::Task a("a", [] {});
    cf::Task b("b", [] {});
    EXPECT_NE(a.id(), b.id());
    EXPECT_GT(a.id(), 0u);
}

TEST(TaskTest, RunExecutesFunction)
{
    int value = 0;
    cf::Task t("inc", [&] { value = 42; });
    t.run();
    EXPECT_EQ(value, 42);
}

TEST(TaskTest, MoveSemantics)
{
    cf::Task a("src", [] {});
    auto m_idbefore = a.id();
    cf::Task b(std::move(a));
    EXPECT_EQ(b.id(), m_idbefore);
    EXPECT_EQ(a.id(), 0u);
}

TEST(TaskTest, NameIsPreserved)
{
    cf::Task t("my_task", [] {});
    EXPECT_EQ(t.name(), "my_task");
}

// ============================================================================
// 3. CopyChannel tests
// ============================================================================

TEST(CopyChannelTest, SendRecvIntRoundtrip)
{
    cf::CopyChannel<int> ch;
    ch.send(42);
    auto val = ch.poll();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 42);
}

TEST(CopyChannelTest, SendRecvStringRoundtrip)
{
    cf::CopyChannel<std::string> ch;
    ch.send(std::string("hello"));
    auto val = ch.poll();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "hello");
}

TEST(CopyChannelTest, ClosePreventsSend)
{
    cf::CopyChannel<int> ch;
    ch.close();
    EXPECT_TRUE(ch.is_closed());
    ch.send(99);
    EXPECT_EQ(ch.depth(), 0u);
}

TEST(CopyChannelTest, DrainAllEmptiesChannel)
{
    cf::CopyChannel<int> ch;
    ch.send(1);
    ch.send(2);
    ch.send(3);
    std::vector<int> out;
    auto count = ch.drain_all(out);
    EXPECT_EQ(count, 3u);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], 1);
    EXPECT_EQ(out[1], 2);
    EXPECT_EQ(out[2], 3);
    EXPECT_EQ(ch.depth(), 0u);
}

TEST(CopyChannelTest, MultipleProducersSingleConsumer)
{
    cf::CopyChannel<int> ch;
    std::thread t1([&] { for (int i = 0; i < 100; ++i) ch.send(i); });
    std::thread t2([&] { for (int i = 100; i < 200; ++i) ch.send(i); });
    t1.join();
    t2.join();
    int sum = 0;
    for (;;) {
        auto val = ch.poll();
        if (!val.has_value()) break;
        sum += *val;
    }
    EXPECT_EQ(sum, 19900);
}

TEST(CopyChannelTest, ChannelPreservesOrder)
{
    cf::CopyChannel<int> ch;
    for (int i = 0; i < 10; ++i) ch.send(i);
    for (int i = 0; i < 10; ++i) {
        auto val = ch.poll();
        ASSERT_TRUE(val.has_value());
        EXPECT_EQ(*val, i);
    }
}

TEST(CopyChannelTest, PollReturnsNulloptWhenEmpty)
{
    cf::CopyChannel<int> ch;
    EXPECT_FALSE(ch.poll().has_value());
}

// ============================================================================
// 4. PriorityTimer tests
// ============================================================================

TEST(PriorityTimerTest, ScheduleInFiresAfterDelay)
{
    cf::VirtualTimeSource vts;
    cf::PriorityTimer timer(&vts);
    int fired = 0;
    timer.schedule_in(cf::Task("t", [&] { ++fired; }), std::chrono::milliseconds(10));
    EXPECT_TRUE(timer.get_expired_tasks().empty());
    EXPECT_EQ(fired, 0);
    vts.advance(std::chrono::milliseconds(15));
    auto tasks = timer.get_expired_tasks();
    ASSERT_EQ(tasks.size(), 1u);
    tasks[0].run();
    EXPECT_EQ(fired, 1);
}

TEST(PriorityTimerTest, ScheduleAtFiresAtCorrectTime)
{
    cf::VirtualTimeSource vts;
    vts.advance(std::chrono::milliseconds(100));
    cf::PriorityTimer timer(&vts);
    int fired = 0;
    auto deadline = vts.steady_now() + std::chrono::milliseconds(50);
    timer.schedule_at(cf::Task("t", [&] { ++fired; }), deadline);
    vts.advance(std::chrono::milliseconds(20));
    EXPECT_TRUE(timer.get_expired_tasks().empty());
    vts.advance(std::chrono::milliseconds(35));
    auto tasks = timer.get_expired_tasks();
    ASSERT_EQ(tasks.size(), 1u);
    tasks[0].run();
    EXPECT_EQ(fired, 1);
}

TEST(PriorityTimerTest, SkippableTasksSkippedWhenExpiredPastThreshold)
{
    cf::VirtualTimeSource vts;
    cf::PriorityTimer timer(&vts, std::chrono::milliseconds(1000));
    int fired = 0;
    cf::Task t("skip", [&] { ++fired; }, cf::TaskPriority::Normal, true);
    timer.schedule_in(std::move(t), std::chrono::milliseconds(10));
    vts.advance(std::chrono::milliseconds(2000));
    auto tasks = timer.get_expired_tasks();
    EXPECT_TRUE(tasks.empty());
    EXPECT_EQ(fired, 0);
}

TEST(PriorityTimerTest, NonSkippableTasksFireEvenWhenExpired)
{
    cf::VirtualTimeSource vts;
    cf::PriorityTimer timer(&vts, std::chrono::milliseconds(1000));
    int fired = 0;
    timer.schedule_in(cf::Task("ns", [&] { ++fired; }, cf::TaskPriority::Normal, false),
                      std::chrono::milliseconds(10));
    vts.advance(std::chrono::milliseconds(2000));
    auto tasks = timer.get_expired_tasks();
    ASSERT_EQ(tasks.size(), 1u);
    tasks[0].run();
    EXPECT_EQ(fired, 1);
}

TEST(PriorityTimerTest, CancelPreventsTaskFromFiring)
{
    cf::VirtualTimeSource vts;
    cf::PriorityTimer timer(&vts);
    int fired = 0;
    cf::Task t("cancel_me", [&] { ++fired; });
    auto tid = t.id();
    timer.schedule_in(std::move(t), std::chrono::milliseconds(10));
    EXPECT_TRUE(timer.cancel(tid));
    vts.advance(std::chrono::milliseconds(20));
    EXPECT_TRUE(timer.get_expired_tasks().empty());
    EXPECT_EQ(fired, 0);
}

TEST(PriorityTimerTest, GetExpiredTasksReturnsEmptyWhenNothingExpired)
{
    cf::VirtualTimeSource vts;
    cf::PriorityTimer timer(&vts);
    EXPECT_TRUE(timer.get_expired_tasks().empty());
}

// ============================================================================
// 5. Blackboard tests
// ============================================================================

TEST(BlackboardTest, RegisterValueReturnsNonNullPointer)
{
    cf::Blackboard bb;
    auto* p = bb.register_value("count", 42);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(*p, 42);
}

TEST(BlackboardTest, GetReturnsSamePointer)
{
    cf::Blackboard bb;
    auto* p1 = bb.register_value("count", 42);
    auto* p2 = bb.get<int>("count");
    EXPECT_EQ(p1, p2);
}

TEST(BlackboardTest, GetCopyReturnsCorrectValue)
{
    cf::Blackboard bb;
    bb.register_value("count", 42);
    auto copy = bb.get_copy<int>("count");
    ASSERT_TRUE(copy.has_value());
    EXPECT_EQ(*copy, 42);
    EXPECT_FALSE(bb.get_copy<int>("nonexistent").has_value());
}

TEST(BlackboardTest, MarkChangedAndSwapChangedKeysWork)
{
    cf::Blackboard bb;
    bb.mark_changed("a");
    bb.mark_changed("b");
    auto keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.size(), 2u);
    EXPECT_TRUE(keys.count("a"));
    EXPECT_TRUE(keys.count("b"));
    EXPECT_TRUE(bb.changed_keys().empty());
}

TEST(BlackboardTest, RemoveRemovesEntry)
{
    cf::Blackboard bb;
    bb.register_value("x", 1);
    EXPECT_TRUE(bb.contains("x"));
    ASSERT_EQ(bb.size(), 1u);
    bb.remove("x");
    EXPECT_FALSE(bb.contains("x"));
    EXPECT_EQ(bb.size(), 0u);
    EXPECT_EQ(bb.get<int>("x"), nullptr);
}

TEST(BlackboardTest, TypeMismatchGetReturnsNullptr)
{
    cf::Blackboard bb;
    bb.register_value("val", 42);
    EXPECT_EQ(bb.get<double>("val"), nullptr);
    EXPECT_NE(bb.get<int>("val"), nullptr);
}

// ============================================================================
// 6. Message tests
// ============================================================================

TEST(MessageTest, NextIdReturnsUniqueIncremented)
{
    cf::Message::reset_id_counter(1);
    auto id1 = cf::Message::next_id();
    auto id2 = cf::Message::next_id();
    EXPECT_EQ(id1, 1u);
    EXPECT_EQ(id2, 2u);
}

TEST(MessageTest, ConstructionWithKindAndSourceWorks)
{
    cf::Message::reset_id_counter(1);
    cf::Message msg(cf::MessageKind::UdpDatagram, "client1");
    EXPECT_EQ(msg.id, 1u);
    EXPECT_EQ(msg.kind, cf::MessageKind::UdpDatagram);
    EXPECT_EQ(msg.source, "client1");
}

TEST(MessageTest, ResetIdCounterWorks)
{
    cf::Message::reset_id_counter(100);
    EXPECT_EQ(cf::Message::next_id(), 100u);
    cf::Message::reset_id_counter();
    EXPECT_EQ(cf::Message::next_id(), 0u);
}

// ============================================================================
// 7. MessageHeartbeat tests
// ============================================================================

TEST(MessageHeartbeatTest, BeatDrainsInputChannel)
{
    cf::CopyChannel<cf::Message> input;
    cf::Blackboard bb;
    cf::MessageHeartbeat hb(&input, &bb);
    hb.set_generate_tick(false);
    input.send(cf::Message(cf::MessageKind::Custom, "test"));
    input.send(cf::Message(cf::MessageKind::UdpDatagram, "udp"));
    auto cs = hb.beat();
    EXPECT_EQ(input.depth(), 0u);
    EXPECT_EQ(hb.m_lastbatch_size(), 2u);
}

TEST(MessageHeartbeatTest, BeatInvokesProcessorWithCorrectBatch)
{
    cf::CopyChannel<cf::Message> input;
    cf::Blackboard bb;
    cf::MessageHeartbeat hb(&input, &bb);
    hb.set_generate_tick(false);
    std::vector<cf::Message> captured;
    hb.set_processor([&](const std::vector<cf::Message>& batch) -> cf::ChangeSet {
        captured = batch;
        return {};
    });
    input.send(cf::Message(cf::MessageKind::Custom, "a"));
    input.send(cf::Message(cf::MessageKind::Custom, "b"));
    hb.beat();
    ASSERT_EQ(captured.size(), 2u);
    EXPECT_EQ(captured[0].source, "a");
    EXPECT_EQ(captured[1].source, "b");
}

TEST(MessageHeartbeatTest, ProcessorResultIsReturned)
{
    cf::CopyChannel<cf::Message> input;
    cf::Blackboard bb;
    cf::MessageHeartbeat hb(&input, &bb);
    hb.set_generate_tick(false);
    hb.set_processor([](const std::vector<cf::Message>&) -> cf::ChangeSet {
        cf::ChangeSet cs;
        cs.new_messages.push_back(cf::Message(cf::MessageKind::SystemEvent, "result"));
        return cs;
    });
    auto cs = hb.beat();
    ASSERT_EQ(cs.new_messages.size(), 1u);
    EXPECT_EQ(cs.new_messages[0].source, "result");
}

TEST(MessageHeartbeatTest, SettingProcessorWorks)
{
    cf::CopyChannel<cf::Message> input;
    cf::Blackboard bb;
    cf::MessageHeartbeat hb(&input, &bb);
    hb.set_generate_tick(false);
    int calls = 0;
    hb.set_processor([&](const std::vector<cf::Message>&) -> cf::ChangeSet {
        ++calls;
        return {};
    });
    hb.beat();
    EXPECT_EQ(calls, 1);
    hb.set_processor(nullptr);
    hb.beat();
    EXPECT_EQ(calls, 1);
}

// ============================================================================
// 8. DataEvolver tests
// ============================================================================

TEST(DataEvolverTest, RegisterComponentWorks)
{
    cf::DataEvolver de;
    de.register_component("comp1",
        [](cf::Blackboard*, const std::vector<std::string>&) -> cf::ChangeSet { return {}; });
    EXPECT_EQ(de.component_count(), 1u);
    ASSERT_EQ(de.component_names().size(), 1u);
    EXPECT_EQ(de.component_names()[0], "comp1");
}

TEST(DataEvolverTest, EvolveOnceTriggersWhenChangedKeysExist)
{
    cf::Blackboard bb;
    cf::DataEvolver de;
    int called = 0;
    de.register_component("c",
        [&](cf::Blackboard*, const std::vector<std::string>& keys) -> cf::ChangeSet {
            ++called;
            EXPECT_EQ(keys.size(), 1u);
            EXPECT_EQ(keys[0], "score");
            return {};
        });
    bb.mark_changed("score");
    auto cs = de.evolve_once(&bb);
    EXPECT_EQ(called, 1);
    EXPECT_TRUE(cs.empty());
}

TEST(DataEvolverTest, EvolveOnceDoesNotTriggerForNewlyChangedKeysDuringEvo)
{
    cf::Blackboard bb;
    cf::DataEvolver de;
    int call_count = 0;
    de.register_component("reactive",
        [&](cf::Blackboard* b, const std::vector<std::string>& keys) -> cf::ChangeSet {
            ++call_count;
            if (!keys.empty()) b->mark_changed(keys[0]);
            return {};
        });
    bb.mark_changed("x");
    de.evolve_once(&bb);
    EXPECT_EQ(call_count, 1);
    de.evolve_once(&bb);
    EXPECT_EQ(call_count, 2);
}

TEST(DataEvolverTest, MultipleComponentsRunInOrder)
{
    cf::Blackboard bb;
    cf::DataEvolver de;
    std::vector<std::string> order;
    de.register_component("A",
        [&](cf::Blackboard*, const std::vector<std::string>&) -> cf::ChangeSet {
            order.push_back("A");
            return {};
        });
    de.register_component("B",
        [&](cf::Blackboard*, const std::vector<std::string>&) -> cf::ChangeSet {
            order.push_back("B");
            return {};
        });
    bb.mark_changed("x");
    de.evolve_once(&bb);
    ASSERT_EQ(order.size(), 2u);
    EXPECT_EQ(order[0], "A");
    EXPECT_EQ(order[1], "B");
}

TEST(DataEvolverTest, UnregisterRemovesComponent)
{
    cf::Blackboard bb;
    cf::DataEvolver de;
    int a_calls = 0, b_calls = 0;
    de.register_component("A",
        [&](cf::Blackboard*, const std::vector<std::string>&) -> cf::ChangeSet { ++a_calls; return {}; });
    de.register_component("B",
        [&](cf::Blackboard*, const std::vector<std::string>&) -> cf::ChangeSet { ++b_calls; return {}; });
    de.unregister_component("A");
    EXPECT_EQ(de.component_count(), 1u);
    bb.mark_changed("x");
    de.evolve_once(&bb);
    EXPECT_EQ(a_calls, 0);
    EXPECT_EQ(b_calls, 1);
}

// ============================================================================
// 9. Stage4Executor tests
// ============================================================================

TEST(Stage4ExecutorTest, ExecuteSendsNewMessagesToOutputChannel)
{
    cf::CopyChannel<cf::Message> output;
    cf::Stage4Executor exec(&output);
    cf::ChangeSet cs;
    cs.new_messages.push_back(cf::Message(cf::MessageKind::Custom, "hello"));
    cs.new_messages.push_back(cf::Message(cf::MessageKind::SystemEvent, "world"));
    exec.execute(std::move(cs));
    auto m1 = output.poll();
    ASSERT_TRUE(m1.has_value());
    EXPECT_EQ(m1->source, "hello");
    auto m2 = output.poll();
    ASSERT_TRUE(m2.has_value());
    EXPECT_EQ(m2->source, "world");
}

TEST(Stage4ExecutorTest, ExecuteSubmitsExternalCallsToWorkQueue)
{
    cf::CopyChannel<cf::Message> output;
    cf::Stage4Executor exec(&output);
    int call_count = 0;
    cf::ChangeSet cs;
    cs.external_calls.push_back([&] { ++call_count; });
    cs.external_calls.push_back([&] { ++call_count; });
    exec.execute(std::move(cs));
    EXPECT_EQ(exec.m_pendingwork(), 2u);
}

TEST(Stage4ExecutorTest, StartStopWorkerThreads)
{
    cf::CopyChannel<cf::Message> output;
    cf::Stage4Executor exec(&output);
    exec.start(2);
    exec.stop();
    exec.start(1);
    exec.stop();
}

TEST(Stage4ExecutorTest, PendingCounterTracksWork)
{
    cf::CopyChannel<cf::Message> output;
    cf::Stage4Executor exec(&output);
    EXPECT_EQ(exec.m_pendingwork(), 0u);
    cf::ChangeSet cs;
    cs.external_calls.push_back([] {});
    exec.execute(std::move(cs));
    EXPECT_EQ(exec.m_pendingwork(), 1u);
}

// ============================================================================
// 10. ChangeSet tests
// ============================================================================

TEST(ChangeSetTest, EmptyReturnsTrueForDefault)
{
    cf::ChangeSet cs;
    EXPECT_TRUE(cs.empty());
}

TEST(ChangeSetTest, AddingMessagesMakesNonEmpty)
{
    cf::ChangeSet cs;
    cs.new_messages.push_back(cf::Message(cf::MessageKind::Custom, "src"));
    EXPECT_FALSE(cs.empty());
}

TEST(ChangeSetTest, AddingExternalCallsMakesNonEmpty)
{
    cf::ChangeSet cs;
    cs.external_calls.push_back([] {});
    EXPECT_FALSE(cs.empty());
}

TEST(ChangeSetTest, ClearResets)
{
    cf::ChangeSet cs;
    cs.new_messages.push_back(cf::Message(cf::MessageKind::Custom, "src"));
    cs.external_calls.push_back([] {});
    cs.clear();
    EXPECT_TRUE(cs.empty());
    EXPECT_TRUE(cs.new_messages.empty());
    EXPECT_TRUE(cs.external_calls.empty());
}

// ============================================================================
// 11. Framework integration tests
// ============================================================================

TEST(FrameworkIntegrationTest, RunOneCycleProcessesMessages)
{
    cf::VirtualTimeSource vts;
    cf::ReactorConfig cfg;
    cf::Framework fw(cfg, &vts);

    std::vector<cf::Message> batch_seen;
    fw.set_batch_processor([&](const std::vector<cf::Message>& batch) -> cf::ChangeSet {
        batch_seen = batch;
        return {};
    });

    cf::CopyChannel<cf::Message> input;
    fw.register_input_channel(&input);
    input.send(cf::Message(cf::MessageKind::UdpDatagram, "client"));
    input.send(cf::Message(cf::MessageKind::PeerEvent, "peer"));

    fw.run_one_cycle();

    auto has_client = [&] {
        for (auto& m : batch_seen)
            if (m.source == "client") return true;
        return false;
    };
    auto has_peer = [&] {
        for (auto& m : batch_seen)
            if (m.source == "peer") return true;
        return false;
    };
    EXPECT_TRUE(has_client());
    EXPECT_TRUE(has_peer());
}

TEST(FrameworkIntegrationTest, Stage4NewMessagesReenterInputForNextCycle)
{
    cf::VirtualTimeSource vts;
    cf::ReactorConfig cfg;
    cf::Framework fw(cfg, &vts);

    bool saw_echo = false;
    fw.set_batch_processor([&](const std::vector<cf::Message>& batch) -> cf::ChangeSet {
        for (auto& m : batch) {
            if (m.source == "echo") saw_echo = true;
        }
        cf::ChangeSet cs;
        cs.new_messages.push_back(cf::Message(cf::MessageKind::SystemEvent, "echo"));
        return cs;
    });

    fw.run_one_cycle(); // TimerTick triggers processor -> emits "echo" -> recycled to input_buffer
    fw.run_one_cycle(); // "echo" appears as input + another TimerTick
    EXPECT_TRUE(saw_echo);
}

// ============================================================================
// 12. Deterministic replay test
// ============================================================================

static std::string serialize_change_set(const cf::ChangeSet& cs)
{
    std::string result;
    for (auto& m : cs.new_messages) {
        result += std::to_string(static_cast<int>(m.kind));
        result += ":";
        result += m.source;
        result += ";";
    }
    result += "ec=" + std::to_string(cs.external_calls.size());
    return result;
}

TEST(DeterministicReplayTest, SameInputsSameOutputs)
{
    auto run = []() -> std::string {
        cf::VirtualTimeSource vts;
        cf::ReactorConfig cfg;
        cf::Framework fw(cfg, &vts);

        fw.blackboard().register_value("counter", 0);

        cf::CopyChannel<cf::Message> input;
        fw.register_input_channel(&input);

        fw.set_batch_processor([&fw](const std::vector<cf::Message>& batch) -> cf::ChangeSet {
            cf::ChangeSet cs;
            for (auto& m : batch) {
                if (m.kind == cf::MessageKind::TimerTick) continue;
                if (m.source == "increment") {
                    auto* count = fw.blackboard().get<int>("counter");
                    if (count) {
                        *count = *count + 1;
                        fw.blackboard().mark_changed("counter");
                    }
                }
                cf::Message resp(cf::MessageKind::SystemEvent, m.source + "_ack");
                resp.payload = {0x01, 0x02, 0x03};
                cs.new_messages.push_back(std::move(resp));
            }
            return cs;
        });

        input.send(cf::Message(cf::MessageKind::Custom, "alpha"));
        input.send(cf::Message(cf::MessageKind::Custom, "beta"));

        cf::ChangeSet cs = fw.heartbeat().beat();
        return serialize_change_set(cs);
    };

    auto r1 = run();
    auto r2 = run();
    EXPECT_EQ(r1, r2);
    EXPECT_FALSE(r1.empty());
    EXPECT_NE(r1.find("alpha_ack"), std::string::npos);
    EXPECT_NE(r1.find("beta_ack"), std::string::npos);
}

// ============================================================================
// Routing Adapter Tests
// ============================================================================

#include "creek/framework/routing_adapter.hpp"

TEST(RoutingAdapterTest, DirectoryAdapterMergeAndTrack) {
    cf::Blackboard bb;
    cf::DirectoryBlackboardAdapter adapter(&bb);

    creek::v1::Endpoint ep;
    ep.set_endpoint_id("ep1");
    ep.set_service("svc");
    ep.set_owner_leaf("leaf1");
    ep.set_version(1);
    ep.set_updated_ms(1000);
    ep.set_alive(true);

    EXPECT_TRUE(adapter.merge(ep));
    auto keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.count("endpoint_directory"), 1u);

    EXPECT_FALSE(adapter.merge(ep));
    keys = bb.swap_changed_keys();
    EXPECT_TRUE(keys.empty());
}

TEST(RoutingAdapterTest, DirectoryAdapterFindAndSnapshot) {
    cf::Blackboard bb;
    cf::DirectoryBlackboardAdapter adapter(&bb);

    creek::v1::Endpoint ep;
    ep.set_endpoint_id("ep1");
    ep.set_service("svc");
    ep.set_version(1);
    ep.set_updated_ms(1000);
    adapter.merge(ep);

    auto found = adapter.find("ep1");
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(found->service(), "svc");

    auto snap = adapter.snapshot("test");
    EXPECT_EQ(snap.endpoints_size(), 1);
    EXPECT_EQ(snap.source_id(), "test");
}

TEST(RoutingAdapterTest, BalancerAdapterPickTracksChange) {
    cf::Blackboard bb;
    cf::BalancerBlackboardAdapter adapter(&bb);

    std::vector<creek::v1::Endpoint> endpoints;
    creek::v1::Endpoint ep;
    ep.set_endpoint_id("ep1");
    ep.set_service("svc");
    ep.set_alive(true);
    endpoints.push_back(ep);

    creek::Metadata metadata;
    auto result = adapter.pick("svc", metadata, endpoints);
    ASSERT_TRUE(result.has_value());

    auto keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.count("sticky_balancer"), 1u);
}

TEST(RoutingAdapterTest, BreakerAdapterRecordsAndTracks) {
    cf::Blackboard bb;
    cf::BreakerBlackboardAdapter adapter(&bb);

    adapter.record_success("ep1", 100);
    auto keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.count("circuit_breaker"), 1u);

    adapter.record_failure("ep1");
    keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.count("circuit_breaker"), 1u);

    EXPECT_TRUE(adapter.allow("ep1"));
}

TEST(RoutingAdapterTest, MetricsAdapterRecordsAndTracks) {
    cf::Blackboard bb;
    cf::MetricsBlackboardAdapter adapter(&bb);

    creek::MetricEvent ev;
    ev.direction = "inbound";
    ev.rpc_name = "test_rpc";
    ev.bytes = 100;
    ev.latency_us = 500;
    ev.success = true;

    adapter.record(ev);
    auto keys = bb.swap_changed_keys();
    EXPECT_EQ(keys.count("metrics_store"), 1u);

    auto current = adapter.current();
    EXPECT_FALSE(current.empty());
}

// ============================================================================
// Periodic Task Tests
// ============================================================================

TEST(PeriodicTaskTest, PeriodicTaskFiresMultipleTimes) {
    cf::VirtualTimeSource vts;
    cf::ReactorConfig cfg;
    cfg.io_threads = 1;
    cfg.cpu_threads = 1;
    cf::Reactor reactor(cfg, &vts);

    std::atomic<int> fire_count{0};
    reactor.start();

    auto id = reactor.schedule_periodic("test_periodic",
        [&fire_count] { fire_count.fetch_add(1); },
        std::chrono::milliseconds(100));

    vts.advance(std::chrono::milliseconds(100));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    vts.advance(std::chrono::milliseconds(100));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    vts.advance(std::chrono::milliseconds(100));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    reactor.cancel_periodic(id);
    reactor.stop();

    EXPECT_GE(fire_count.load(), 1);
}

TEST(PeriodicTaskTest, CancelStopsPeriodicTask) {
    cf::VirtualTimeSource vts;
    cf::ReactorConfig cfg;
    cfg.io_threads = 1;
    cfg.cpu_threads = 1;
    cf::Reactor reactor(cfg, &vts);

    std::atomic<int> fire_count{0};
    reactor.start();

    auto id = reactor.schedule_periodic("test_cancel",
        [&fire_count] { fire_count.fetch_add(1); },
        std::chrono::milliseconds(50));

    vts.advance(std::chrono::milliseconds(50));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    int before_cancel = fire_count.load();
    reactor.cancel_periodic(id);

    vts.advance(std::chrono::milliseconds(200));
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    reactor.stop();
    int after_cancel = fire_count.load();

    EXPECT_EQ(before_cancel, after_cancel);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
