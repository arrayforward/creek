# Creek — 有状态的服务网格边车

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue)](https://en.cppreference.com/w/cpp/20)
[![Platform](https://img.shields.io/badge/platform-Linux%20|%20macOS%20|%20Windows-brightgreen)]()

Creek 是一个**轻量级服务网格 Sidecar**，专为**有状态业务**设计。基于自研 UDP-over-Tight 可靠传输协议，在 Leaf / Node 两层拓扑之上提供**分布式 RPC 路由**、**粘性会话负载均衡**、**Redis 驱动服务发现**以及 **OpenTelemetry 兼容的 Metrics 观测**。

---

## 为什么选择 Creek？

### 解决的关键问题

**1. 传统 Service Mesh 太重** — Istio / Envoy 需要数十个 CRD、庞大的控制面和 iptables 拦截，不适合中小型集群或边缘部署。Creek 单个二进制即可同时承担数据面转发、服务注册、负载均衡和指标采集。

**2. 有状态路由缺失** — 大多数网关只能做轮询或加权随机，缺少"同一 session 始终路由到同一后端"的能力。Creek 在 Leaf 侧解析 gRPC metadata 中的 `sid`，实现**一分钟粘性负载均衡**，后端离线后自动降级到 LRU 缓存重选。

**3. 跨语言 RPC 代理** — 同时暴露 gRPC 和 JSON-RPC 入口，业务服务无需内嵌网格逻辑，只需通过标准 gRPC 协议向 Leaf 注册即可被整个集群发现和调用。

**4. 可靠的 UDP 传输** — 自研 **Tight 协议**在 UDP 之上提供 ACK 重传、FEC 纠错、Token-Bucket Pacer 和带宽估算，不依赖 QUIC 或 SCTP，在高丢包场景下优于 TCP。

**5. 服务发现零配置** — 通过 Redis Hash 实现 Node / Leaf 的自动注册与发现，启动后无需手动维护 peer 列表。

---

## 核心特性

| 类别 | 特性 | 说明 |
|---|---|---|
| 🏗️ **拓扑** | Leaf / Node 双层 | Node 全网状，Leaf 挂载到 Node，对应任意网络拓扑 |
| 🔄 **协议** | Tight UDP | 48 字节包头、CRC32 校验、Seed 重传、R-S 单冗余 FEC |
| 📡 **路由** | 有状态跨节点 | 基于 Protobuf RoutedRequest，hop_limit 防环，请求级去重 |
| 🎯 **负载均衡** | 粘性会话 | gRPC metadata `sticky/sid` → 1 分钟 LRU 缓存 → 后端离线自动切换 |
| 🗂️ **目录同步** | LWW 版本合并 | 基于 Endpoint 版本号和时间戳的全网增量同步，周期 100 ms |
| 🔍 **服务发现** | Redis + 静态 | 可选 Redis Hash 自动发现，兼容静态 `--peer` / `--parent` |
| 📊 **可观测性** | OpenMetrics + JSON | 分钟桶累计 + 上一分钟快照 + 读取清零（take），HTTP 拉取 |
| 🌐 **多协议** | gRPC + JSON-RPC | Leaf 同时暴露 gRPC Server 和 HTTP POST `/rpc` 入口 |
| 🧪 **测试** | 三级自动化 | 79 项 Tight 协议单测 + 路由/指标单测 + Python / C++ 双 E2E |

---

## 架构概览

```
                    Redis (可选)
                   /           \
          ┌─────────┐         ┌─────────┐
          │ Node-1  │◄══════►│ Node-2  │   ← Tight UDP 全网状
          │ :10000  │         │ :10004  │
          └────▲────┘         └────▲────┘
               │                   │
      ┌────────┴──┐          ┌────┴──────────┐
      │ Client    │          │ Service-1 Leaf │ Service-2 Leaf
      │ Leaf      │          │  :30016        │  :30021
      │ :30009    │          └───┬────┬───────┘
      │ (入口)    │              │    │
      └───────────┘         ┌────┘    └────┐
                         Backend-1    Backend-2
                          :30025       :30028
```

- **Client** 通过 gRPC 或 JSON-RPC 调用 Entry Leaf
- Entry Leaf 根据 `sid` 粘性选择目标 Leaf → 构造 `RoutedRequest` → 经 Node Mesh 转发
- Service Leaf 调用本地 Backend → 响应沿反向路径返回
- 后端离线后 3 秒内 Node 撤销端点并全网广播

---

## 快速开始

### 前置依赖

| 库 | 版本 | 说明 |
|---|---|---|
| gRPC / Protobuf | ≥ 1.40 | RPC 与序列化 |
| nlohmann/json | ≥ 3.0 | JSON 解析 |
| hiredis | ≥ 1.0 | Redis 客户端（可选） |
| GTest | ≥ 1.11 | 测试框架 |
| CMake | ≥ 3.24 | 构建系统 |
| C++20 编译器 | GCC 12+ / MinGW | |

### 构建

```bash
git clone https://github.com/anomalyco/creek.git
cd creek
mkdir build && cd build

# MinGW (Windows)
cmake --preset msys2-mingw64
cmake --build . --parallel

# Linux / macOS
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### 3 步跑通最小拓扑

**终端 1 — Node：**
```bash
creek_sidecar node --id node-1 --udp 127.0.0.1:10000 --token test
```

**终端 2 — Leaf + Backend：**
```bash
creek_sidecar leaf --id svc --udp 127.0.0.1:10001 \
    --parent node-1@127.0.0.1:10000 --grpc 127.0.0.1:9000 --token test

creek_hello_server --id b1 --listen 127.0.0.1:7001 --leaf 127.0.0.1:9000
```

**终端 3 — Client：**
```bash
creek_hello_client --target 127.0.0.1:9000 --name world --sid s1 --sticky true
```

输出：
```
backend-1  Hello, world from backend-1
```

---

## 测试

```bash
ctest --output-on-failure

# 全部 4 项测试
# ✅ creek_routing_metrics_test  — 路由与指标单元测试
# ✅ creek_tight_test            — Tight 协议 79 项断言
# ✅ creek_e2e                   — Python 多进程端到端（子进程化 Node/Leaf/Backend）
# ✅ creek_e2e_2node             — C++ 双 Node E2E（通过 Redis 服务发现）
```

---

## 项目结构

```
creek/
├── apps/                    # 可执行程序入口
│   ├── sidecar_main.cpp     # Node / Leaf 启动器
│   ├── hello_server.cpp     # 示例 Backend（gRPC Greeter 服务）
│   └── hello_client.cpp     # 示例 Client（粘性会话调用）
├── include/creek/           # 公开头文件
│   ├── types.hpp            # 基础类型（Address, RemotePeer, Bytes）
│   ├── tight.hpp            # Tight 传输协议 API
│   ├── runtime.hpp          # NodeConfig / LeafConfig / NodeRuntime / LeafRuntime
│   ├── routing.hpp          # EndpointDirectory + StickyBalancer
│   ├── redis.hpp            # RedisClient — 服务发现
│   ├── metrics.hpp          # MetricsStore + MetricsHttpServer
│   └── json_rpc.hpp         # JsonRpcHttpServer
├── src/                     # 核心实现
│   ├── tight.cpp            # 48 字节包头 / CRC32 / 分组 / FEC / Pacer
│   ├── runtime.cpp          # Node / Leaf 完整运行时
│   ├── routing.cpp          # LWW 目录合并 / 粘性选择
│   ├── metrics.cpp          # 分钟桶 / OpenMetrics / JSON
│   ├── redis.cpp            # hiredis 封装
│   ├── json_rpc.cpp         # WinSock2 HTTP JSON-RPC
│   └── types.cpp            # 地址解析 / 随机 ID
├── proto/
│   └── creek.proto          # Greeter / LeafControl / Admin / RoutedRequest
├── tests/
│   ├── tight_test.cpp       # 传输协议 79 项单测
│   ├── routing_metrics_test.cpp
│   ├── e2e_2node_test.cpp   # C++ E2E 集成
│   ├── e2e/
│   │   └── e2e.py            # Python E2E 集成
│   └── integration/         # Redis 集成测试
├── tools/                   # redis-cli / PowerShell 脚本
├── docs/                    # 详细文档
└── CMakeLists.txt
```

---

## 未来更新路线图

### 短期（v0.2）

- **TLS 加密传输** — Tight 握手阶段加入 ECDH 密钥交换和 AEAD 加密
- **OpenTelemetry OTLP 直接上报** — 当前仅有 HTTP Pull 模型，增加 gRPC OTLP Exporter
- **gRPC Reflection** — Leaf 暴露 Server Reflection，兼容 grpcurl / Postman

### 中期（v0.3）

- **WASM 插件过滤链** — Leaf 侧注入 WASM filter，可自定义路由、鉴权、限流逻辑
- **多父 Leaf** — Leaf 同时连接多个 Node，网络分区时自动切换
- **S3 / NATS 后端** — backend 可注册为非 gRPC 端点，Leaf 自动适配协议

### 长期（v1.0）

- **Cluster Federation** — 多 DataCenter 跨集群路由，通过 Node 层级实现区域感知
- **SQL 感知路由** — 解析请求中的分片键（tenant_id），路由到固定后端实例
- **eBPF 加速** — 热路径（UDP 分片 / ACK）卸载到 eBPF XDP，降低用户态开销
- **QoS / 优先级队列** — RPC 调用分级，高优先级请求优先获得带宽 token

---

## 许可证

MIT License © 2025 Creek Authors. 详见 [LICENSE](LICENSE)。

## 致谢

本项目由 ArrayForward构建。感谢 [nlohmann/json](https://github.com/nlohmann/json)、[gRPC](https://grpc.io/)、[hiredis](https://github.com/redis/hiredis) 和 [Google Test](https://github.com/google/googletest) 开源项目提供的坚实基础。
