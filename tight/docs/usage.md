# tight 协议使用文档

tight 是一个自包含、零第三方依赖的 C++17 可靠 UDP 传输库，面向端云实时通信
场景（IoT 设备 ↔ 云端网关）。本文档覆盖：集成方式、快速上手、完整的
客户端/服务器端示例、配置项参考、API 参考与行为约定。

## 目录

- [1. 特性一览](#1-特性一览)
- [2. 集成](#2-集成)
- [3. 快速上手（最小示例）](#3-快速上手最小示例)
- [4. 完整客户端示例（IoT 音频设备）](#4-完整客户端示例iot-音频设备)
- [5. 完整服务器端示例（多设备网关）](#5-完整服务器端示例多设备网关)
- [6. TightConfig 配置参考](#6-tightconfig-配置参考)
- [7. API 参考](#7-api-参考)
- [8. 行为约定（必读）](#8-行为约定必读)
- [9. 典型场景参数配方](#9-典型场景参数配方)

---

## 1. 特性一览

| 特性 | 说明 |
|---|---|
| 可靠传输 | ACK 确认 + NACK 丢包重传（每包最多 10 次），缺口超过 3.5×RTT 即上报，确认前每个报告周期重复 NACK |
| 加密 | 握手 X25519 密钥交换，HKDF-SHA256 派生会话密钥，数据面 AES-256-GCM（报文头做 AAD），可关 |
| FEC | Reed-Solomon GF(2⁸) 擦除码，冗余率由迟到率信息熵 H(p)×1.2 动态驱动 |
| 拥塞控制 | BBR（BtlBw 窗口最大值 + RTprop 最小 RTT），令牌桶 pacing |
| 时钟对表 | 握手对表 + 每次心跳重对表，可测单向传输时间（慢报文统计） |
| 命令通道 | 单报文控制指令，保序投递（乱序最多等 3×RTT），插队直达 |
| 优先级 | 数据消息支持优先级，高优先级先出队（音频不被文件流阻塞） |
| 精简模式 | 客户端单线程、64KB 小栈、小缓冲小队列，空闲实例 ~76KB，可运行时切换 |

## 2. 集成

### 方式一：add_subdirectory（推荐）

```cmake
add_subdirectory(tight)
target_link_libraries(your_app PRIVATE tight)
# Windows 需链接 ws2_32（tight 的 CMake 已处理）
```

### 方式二：独立构建安装

```bash
cmake -B build -S tight -G "MinGW Makefiles"
cmake --build build -j 4
cmake --install build --prefix /your/prefix
# 头文件: prefix/include/tight/  库: prefix/lib/libtight.a
```

代码中包含：

```cpp
#include "tight/tight.hpp"   // 聚合头：TightTransport + TightConfig + 全部类型
```

## 3. 快速上手（最小示例）

### 3.1 服务器（echo）

```cpp
#include "tight/tight.hpp"
#include <cstdio>

int main() {
    tight::TightConfig cfg;
    cfg.bind  = tight::NetAddress("0.0.0.0", 9443);
    cfg.id    = "gateway";
    cfg.token = "shared-secret";
    cfg.role  = tight::LinkRole::Node;      // 服务器必须是 Node

    tight::TightTransport server(cfg);

    server.set_message_callback(
        [&server](const std::string& peer, tight::Bytes payload) {
            std::printf("[msg] %s: %zu bytes\n", peer.c_str(), payload.size());
            server.send(peer, std::move(payload));   // echo 回去
        });
    server.set_peer_callback([](const tight::PeerEvent& e) {
        std::printf("[peer] %s state=%d\n", e.id.c_str(), static_cast<int>(e.state));
    });

    if (!server.start()) return 1;
    std::puts("listening on 9443...");
    std::getchar();
    server.stop();
}
```

### 3.2 客户端

```cpp
#include "tight/tight.hpp"
#include <cstdio>
#include <cstring>

int main() {
    tight::TightConfig cfg;
    cfg.bind      = tight::NetAddress("0.0.0.0", 0);   // 端口 0 = 系统分配
    cfg.id        = "device-001";
    cfg.token     = "shared-secret";
    cfg.lite_mode = true;                              // 客户端精简模式

    tight::TightTransport client(cfg);
    client.set_message_callback([](const std::string&, tight::Bytes payload) {
        std::printf("[echo] %.*s\n", (int)payload.size(), (const char*)payload.data());
    });

    if (!client.start()) return 1;
    if (!client.connect({"gateway", tight::NetAddress("127.0.0.1", 9443)})) return 1;

    const char* text = "hello tight";
    client.send("gateway", tight::Bytes(text, text + std::strlen(text)));

    std::getchar();
    client.stop();
}
```

## 4. 完整客户端示例（IoT 音频设备）

场景：16kHz 单声道 G.711 音频上行 + 视频流 + 偶尔的文件上传，
lite 模式，断线自动重连（协议内置），40ms 组包。

```cpp
#include "tight/tight.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>

using namespace std::chrono;

class AudioDevice {
public:
    AudioDevice() {
        tight::TightConfig cfg;
        cfg.bind      = tight::NetAddress("0.0.0.0", 0);
        cfg.id        = "device-001";
        cfg.token     = "shared-secret";

        // --- IoT 场景配方（详见第 9 节）---
        cfg.lite_mode            = true;              // 单线程低占用
        cfg.mtu                  = 1350;              // 载荷 1286B，整包纳 PCM 40ms 帧
        cfg.max_message_bytes    = 64 * 1024;         // 文件块上限
        cfg.flush_interval       = milliseconds(10);  // lite 下自动钳制 ≥10ms，省 CPU
        cfg.report_interval      = milliseconds(500);
        cfg.speed_test_enabled   = true;              // 视频/文件需要准确带宽 pacing
        cfg.initial_bandwidth_bytes = 4 * 1024 * 1024;
        cfg.late_rtt_multiplier  = 2.5;               // FEC 更快响应劣化
        cfg.encryption_enabled   = true;

        m_client = std::make_unique<tight::TightTransport>(cfg);

        m_client->set_message_callback(
            [](const std::string&, tight::Bytes payload) {
                // 下行音频帧：送入解码播放
                std::printf("[downlink] %zu bytes\n", payload.size());
            });
        m_client->set_command_callback(
            [](const std::string&, tight::Bytes cmd) {
                // 云端控制指令（保序到达）：云台/按键/参数下发
                std::printf("[cmd] %.*s\n", (int)cmd.size(), (const char*)cmd.data());
            });
        m_client->set_peer_callback([this](const tight::PeerEvent& e) {
            m_online = (e.state == tight::LinkState::Online);
        });
    }

    bool run(const char* host, std::uint16_t port) {
        if (!m_client->start()) return false;
        if (!m_client->connect({"gateway", tight::NetAddress(host, port)})) return false;

        // 音频线程：40ms 一帧（G.711 640B）
        std::thread audio([this] {
            auto next = steady_clock::now();
            while (m_running) {
                next += milliseconds(40);
                std::this_thread::sleep_until(next);
                if (!m_online) continue;
                tight::Bytes frame = capture_g711_40ms();     // 采集+编码
                // 队列满（返回 false）直接丢帧：陈旧音频不如丢弃
                m_client->send_priority("gateway", std::move(frame), 2);
            }
        });

        // 视频线程：按码率 pacing，中优先级
        std::thread video([this] {
            while (m_running) {
                if (!m_online) { std::this_thread::sleep_for(milliseconds(100)); continue; }
                tight::Bytes chunk = capture_video_chunk();   // 8~16KB
                while (!m_client->send_priority("gateway", std::move(chunk), 1))
                    std::this_thread::sleep_for(milliseconds(5));
                pace_to_bitrate(512 * 1024 / 8, chunk.size());
            }
        });

        audio.join();
        video.join();
        m_client->stop();
        return true;
    }

private:
    tight::Bytes capture_g711_40ms() { return tight::Bytes(640, 0x55); }
    tight::Bytes capture_video_chunk() { return tight::Bytes(8192, 0x11); }
    void pace_to_bitrate(std::size_t, std::size_t) {
        std::this_thread::sleep_for(milliseconds(120));
    }

    std::unique_ptr<tight::TightTransport> m_client;
    std::atomic<bool> m_running{true};
    std::atomic<bool> m_online{false};
};

int main() {
    AudioDevice dev;
    return dev.run("127.0.0.1", 9443) ? 0 : 1;
}
```

要点：

- **优先级**：音频=2、视频=1、文件=0（`send_priority` 第三参数），
  文件 bulk 永远不会堵在音频前面
- **`send` 返回 false** 表示队列满或超 `max_message_bytes`：音频/视频丢帧，
  文件退避重试
- **断线**：协议内部自动重连（Node 端 `m_reconnect`），应用只需用
  `PeerEvent.state` 判断当前是否 Online

## 5. 完整服务器端示例（多设备网关）

```cpp
#include "tight/tight.hpp"

#include <cstdio>
#include <string>
#include <thread>
#include <unordered_map>

class Gateway {
public:
    Gateway() {
        tight::TightConfig cfg;
        cfg.bind  = tight::NetAddress("0.0.0.0", 9443);
        cfg.id    = "gateway";
        cfg.token = "shared-secret";
        cfg.role  = tight::LinkRole::Node;      // 必须 Node：接受设备接入

        // 服务器保持普通模式（4 线程），仅对齐业务参数
        cfg.mtu                  = 1350;
        cfg.max_message_bytes    = 64 * 1024;
        cfg.flush_interval       = std::chrono::milliseconds(2);  // 保音频链路
        cfg.queue_limit          = 4096;   // 多设备汇聚，给足缓冲深度
        cfg.drop_log             = true;   // 异常消息丢弃告警（服务器端开启）

        m_server = std::make_unique<tight::TightTransport>(cfg);

        m_server->set_peer_callback([](const tight::PeerEvent& e) {
            std::printf("[peer] %s state=%d client_id=%u\n",
                        e.id.c_str(), static_cast<int>(e.state), e.client_id);
        });

        m_server->set_message_callback(
            [this](const std::string& peer, tight::Bytes payload) {
                dispatch(peer, std::move(payload));
            });

        m_server->set_command_callback(
            [this](const std::string& peer, tight::Bytes cmd) {
                // 设备上行控制信息（按键/告警），保序到达
                std::printf("[cmd] %s: %.*s\n",
                            peer.c_str(), (int)cmd.size(), (const char*)cmd.data());
            });
    }

    bool run() {
        if (!m_server->start()) return false;
        std::printf("gateway on 9443, port=%u\n", m_server->local_port());
        std::getchar();
        m_server->stop();
        return true;
    }

private:
    void dispatch(const std::string& peer, tight::Bytes payload) {
        // 按业务首字节分流：0x01=音频 0x02=视频 0x03=文件块
        if (payload.empty()) return;
        switch (payload[0]) {
        case 0x01: forward_to_audio_pipeline(peer, payload); break;
        case 0x02: forward_to_video_pipeline(peer, payload); break;
        case 0x03: handle_file_chunk(peer, payload);         break;
        default: break;
        }
    }

    void forward_to_audio_pipeline(const std::string&, const tight::Bytes&) {}
    void forward_to_video_pipeline(const std::string&, const tight::Bytes&) {}

    void handle_file_chunk(const std::string& peer, const tight::Bytes& payload) {
        // 文件块：应用层序号校验，缺块经命令通道请求补发
        // （协议层重传上限 10 次，耗尽后静默丢弃，文件需应用层兜底）
        // send_command() 单报文保序，适合补发请求：
        //   m_server->send_command(peer, make_resend_request(missing_seq));
        (void)peer; (void)payload;
    }

    std::unique_ptr<tight::TightTransport> m_server;
};

int main() {
    Gateway gw;
    return gw.run() ? 0 : 1;
}
```

## 6. TightConfig 配置参考

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `bind` | NetAddress | — | 绑定地址；端口 0 = 系统分配（可用 `local_port()` 查询） |
| `id` | std::string | — | 本端标识，握手时交换；空 = 匿名（服务器分配 anon-*） |
| `token` | std::string | — | 接入令牌，握手校验 |
| `role` | LinkRole | Leaf | Node = 接受接入（服务器）；Leaf = 终端设备 |
| `mtu` | size_t | **1350** | 链路 MTU；单包载荷 = mtu-48（开 GCM 再 -16 = 1286B） |
| `heartbeat` | ms | 5000 | 心跳周期，兼作时钟重对表 |
| `report_interval` | ms | 1000 | ACK/NACK 报告周期（丢失序号确认前每周期重复上报） |
| `flush_interval` | ms | 10 | 排空节拍；**lite 模式自动钳制 ≥10ms**（IoT 省 CPU） |
| `dead_timeout` | ms | 30000 | 对端静默判定死亡 |
| `retransmit_timeout` | ms | 500 | 握手重传间隔 |
| `initial_bandwidth_bytes` | uint64 | 100MB | BBR 初始带宽（令牌桶种子），未知链路保持默认 |
| `queue_limit` | size_t | 65536 | 发送队列消息数上限（**lite ≤128**） |
| `max_message_bytes` | size_t | 64KB | 单消息上限，钳制 [8KB, 10MB]；超限 `send` 返回 false，接收侧丢弃畸形分片组 |
| `drop_log` | bool | true | 丢弃异常消息时告警（**lite 强制关闭**） |
| `retransmit_enabled` | bool | true | 数据面 NACK 重传。关闭：不生成 NACK、缺口立即跳过，并经握手通告对端停止保留重传缓冲；任一端可单方面关闭，纯 FEC 兜底 |
| `late_rtt_multiplier` | double | 4.0 | 慢报文判定阈值（倍 RTT）；调低 → FEC 响应更灵敏 |
| `speed_test_enabled` | bool | true | 建连后发探测列车测带宽 |
| `speed_test_bytes` | size_t | 100KB | 探测列车长度 |
| `encryption_enabled` | bool | true | ECDH + AES-256-GCM |
| `socket_buffer_bytes` | size_t | 8MB | SO_RCVBUF/SO_SNDBUF（**lite ≤16KB**） |
| `encode_queue_limit` | size_t | 4096 | 待分片消息队列（**lite ≤64**） |
| `outbound_queue_limit` | size_t | 65536 | 待发数据报队列（**lite ≤256**） |
| `lite_mode` | bool | false | 客户端精简模式；`set_lite_mode()` 可运行时切换 |

## 7. API 参考

```cpp
class TightTransport {
public:
    using MessageCallback = std::function<void(const std::string& peer_id, Bytes payload)>;
    using PeerCallback    = std::function<void(const PeerEvent& event)>;
    using CommandCallback = std::function<void(const std::string& peer_id, Bytes payload)>;

    explicit TightTransport(TightConfig config);

    void set_message_callback(MessageCallback);   // 数据消息（Data/Parity 重组后投递）
    void set_peer_callback(PeerCallback);         // 对端状态变化（Online/Closed...）
    void set_command_callback(CommandCallback);   // 命令通道（保序）

    bool start();                                 // 绑定 + 起线程
    void stop();

    bool connect(const RemotePeer& remote);       // {id, NetAddress}
    bool send(const std::string& peer_id, Bytes payload);                  // 优先级 0
    bool send_priority(const std::string& peer_id, Bytes payload, int priority);
    bool send_command(const std::string& peer_id, Bytes payload);          // 单报文，超限 false

    void set_lite_mode(bool lite);                // 运行时切换线程模型，本端属性
    bool lite_mode() const;

    std::vector<PeerEvent> peers() const;         // 当前对端快照
    std::uint16_t local_port() const;
};
```

`PeerEvent`：`id` / `role` / `state`（Closed→Handshake→Established→Online）/ `client_id`。

辅助组件（可单独使用，见各自头文件）：

- `tight/fec.hpp`：`ReedSolomon::encode/decode`（span 接口，零拷贝）
- `tight/bandwidth.hpp`：BBR 带宽/RTT 估算器
- `tight/packet_codec.hpp`：线格式编解码 + CRC32
- `tight/blocking_queue.hpp`：有界阻塞队列（节点回收池）
- `tight/logger.hpp`：`TIGHT_LOG_*` 宏

## 8. 行为约定（必读）

1. **`send*` 返回 false 的三种情况**：未 start / 队列满 / 超过
   `max_message_bytes`。实时流丢帧，文件流退避重试。
2. **重传上限**：每包最多重传 10 次（NACK 驱动），耗尽后静默丢弃。
   实时音视频无感（FEC + 丢帧掩盖）；**文件传输必须在应用层做
   块序号校验 + 缺块补发 + 整文件校验**（补发请求走 `send_command`）。
3. **缺口跳过**：收端发现缺口超过 3.5×RTT 即跳过（ACK 游标不停滞），
   迟到的重传仍正常投递——乱序不会造成消息丢失。
4. **命令通道**：单报文（≤ 单包载荷）、保序、乱序最多等 3×RTT 后跳号。
5. **lite 模式是本端属性**：客户端 lite、服务器普通模式互不影响；
   握手/加密/测速等协议行为两端一致。
6. **内存**：空闲 lite 实例 ~76KB；传输期驻留 ≈ 在途字节数，与总流量
   无关（平台期，已验证 16MB 传输 delta ~136KB）。
7. **重传协商（握手能力标志）**：握手载荷尾部 1 字节 bit0 通告
   `retransmit_enabled`。发送端保留重传缓冲的条件 = 本端配置 && 对端
   通告，因此**任一端可单方面关闭整条链路的重传**（云端可按设备类型
   下发，端侧也可上报）。关闭后控制包（握手/命令）可靠性不受影响，
   数据面靠 FEC + 应用层容错。内存收益：在途增量从 ∝码率×确认窗口
   （50KB/s 实测 128KB，2Mbps 视频预估 ~500KB）降为**常数 ~24KB**；
   无重传协议栈预算可按 **76KB 静态 + ~50KB 动态** 封顶。

## 9. 典型场景参数配方

### 9.1 IoT 音频设备（16kHz 单声道 G.711/PCM + 视频 + 文件）

```cpp
cfg.lite_mode            = true;
cfg.mtu                  = 1350;                 // PCM 40ms 帧 1280B 整包容纳
cfg.flush_interval       = 10ms;                 // lite 自动 ≥10ms
cfg.report_interval      = 500ms;
cfg.late_rtt_multiplier  = 2.5;
cfg.speed_test_enabled   = true;                 // 有视频/文件时需要
cfg.initial_bandwidth_bytes = 4 * 1024 * 1024;
cfg.retransmit_enabled   = false;                // 纯实时 AV：在途内存常数化
// 组包：40ms/帧；优先级 音频2 视频1 文件0；文件块 8KB + 应用层确认
```

### 9.2 云端网关（多设备汇聚）

```cpp
cfg.role                 = tight::LinkRole::Node;
cfg.flush_interval       = 2ms;
cfg.queue_limit          = 4096;
cfg.drop_log             = true;
// 其余默认；socket_buffer 8MB 吸收多设备并发突发
```

### 9.3 纯低速传感（只发遥测，极致省电）

```cpp
cfg.lite_mode            = true;
cfg.speed_test_enabled   = false;
cfg.max_message_bytes    = 8 * 1024;             // 下限
cfg.report_interval      = 2000ms;
cfg.heartbeat            = 10000ms;
cfg.retransmit_enabled   = false;                // 遥测丢一包无所谓，内存封顶
```
