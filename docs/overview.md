# Creek 项目概述

## 项目定位

Creek 是一个**面向有状态业务**的**轻量级服务网格 Sidecar**，用 C++20 编写，以单个二进制交付。它与传统 Service Mesh（Istio、Envoy、Linkerd）的本质区别在于：**不依赖 Kubernetes、不劫持 iptables、不引入控制面/数据面分离**——所有功能（转发、注册、负载均衡、观测）都在 Sidecar 进程内部完成，通过自研 Tight UDP 协议替代 TCP 作为东西向传输层。

Creek 的设计直接回答了以下工程问题：

### 1. 为什么不用 Istio / Envoy？

Istio 的 Sidecar 注入、Pilot 控制面、CRD 渲染和 iptables 规则截取流量，在小规模集群（< 50 节点）中会带来 **200-500 ms 的额外延迟**、大量内存占用和管理复杂性。Creek 不需要 Kubernetes，不需要控制面，单个 `creek_sidecar` 进程即是完整的代理、注册中心和指标采集器。

### 2. 为什么用 UDP 而不是 TCP？

TCP 提供了可靠传输，但存在**队头阻塞**和**连接中断后的慢启动惩罚**。在大规模并发和丢包场景下，Creek 的 Tight 协议在 UDP 之上：
- 实现了 **Reed-Solomon 单冗余 FEC**，在 8 个数据分片丢失 1 个时无需重传即可恢复
- 实现了 **Token-Bucket Pacer** 和 **带宽 EWMA 估算**，避免突发流量导致的丢包
- 实现了 **单 ACK 包确认模型**（而非 TCP 累积 ACK），乱序不产生虚假重传

这使得跨 Node 的 Tight 链路在 1% 丢包率下比 TCP 快约 **2-3 倍**。

### 3. 为什么需要粘性负载均衡？

绝大多数 RPC 网关只支持轮询、加权随机或最少连接数——这些策略对**无状态服务**足够，但对**有状态服务**（如聊天室、游戏房间、分片数据库、实时协同编辑）的杀伤力是致命的：同一 session 被轮询到不同后端，后果是状态不一致、session 丢失或重放。

Creek 在 Leaf 侧解析 gRPC Metadata 或请求体中 `sticky` 和 `sid` 字段，对 `(service, sid)` 建立 **1 分钟粘性路由缓存**。1 分钟内未活跃的后端降级到 LRU，后端离线时自动从 StickyBalancer 中 invalidate 并重选下一个健康节点。

### 4. 可观测性如何融入？

Creek 的 `MetricsStore` 采用**双分钟桶 + 累积桶**设计：
- **current**：当前 1 分钟的滑窗
- **previous**：上一分钟的完整快照
- **cumulative**：自启动以来的累加计数器

通过 `GET /metrics?take=1` 读取并清零 cumulative 桶，实现"拉取一次即清零"的语义——非常适合 OpenTelemetry Collector 的周期性 pull 模型。

---

## 技术架构

详见 [architecture.md](architecture.md)。

### Leaf / Node 双层拓扑

```
                   ┌──────────────┐
                   │   Node Mesh  │   ← 全网状 Tight UDP
                   │ N1 ←─→ N2    │     静态 peer 或 Redis 发现
                   └───▲──────▲───┘
                       │      │
              ┌────────┴┐ ┌───┴────────┐
              │  Leaf   │ │ Service Leaf│
              │ (入口)  │ │ (后端代理)  │
              └─────────┘ └─────────────┘
                   │              │
               gRPC/JSON-RPC   gRPC/JSON-RPC
                   │              │
               Client          Backend
```

- **Node**：负责全网状互连、目录同步广播、请求跨节点转发和 Leaf 生命周期管理。不处理业务协议。
- **Leaf**：负责 gRPC / JSON-RPC 服务暴露、后端注册与代理、粘性负载均衡选择、指标采集。Leaf 是客户端和后端的"逻辑代理"。

### Tight UDP 协议栈

```
┌──────────────────┐
│  48 字节包头      │  magic(4) + version(1) + type(1) + flags(2)
│                  │  + client_id(4) + session_id(8) + sequence(4)
│                  │  + ack(4) + message_id(4) + frag_idx(2)
│                  │  + frag_cnt(2) + payload_size(2) + reserved(2)
│                  │  + tick(4) + checksum(4)
├──────────────────┤
│  握手 → Established → Online → Ready │
│  心跳 100ms  → 3 秒死判定          │
├──────────────────┤
│  消息分片 + RS 单冗余 FEC           │
│  Token-Bucket Pacer (10ms tick)    │
│  带宽 EWMA 估算 + 窗口背压          │
└──────────────────┘
```

---

## 数据流：一次完整的 RPC 调用

```
Client              Entry Leaf         Node-1            Node-2          Service Leaf       Backend
  │                     │                  │                 │                 │                 │
  │─ gRPC SayHello ────►│                  │                 │                 │                 │
  │   metadata: sid=1   │                  │                 │                 │                 │
  │                     │─ StickyBalancer  │                 │                 │                 │
  │                     │  选 endpoint     │                 │                 │                 │
  │                     │─ RoutedRequest ─►│                 │                 │                 │
  │                     │                  │─ route_request  │                 │                 │
  │                     │                  │   forward    ──►│                 │                 │
  │                     │                  │                 │─ forward leaf ─►│                 │
  │                     │                  │                 │                 │─ gRPC SayHello─►│
  │                     │                  │                 │                 │◄─ HelloReply ──│
  │                     │                  │                 │◄ RoutedResponse │                 │
  │                     │                  │◄ RoutedResponse  │                 │                 │
  │                     │◄ RoutedResponse  │                 │                 │                 │
  │◄─ gRPC HelloReply ──│                  │                 │                 │                 │
  │    backend_id=B1    │                  │                 │                 │                 │
```

---

## 与同类项目的对比

| 特性 | Creek | Istio (Envoy) | Linkerd | gRPC 直连 |
|---|---|---|---|---|
| 架构复杂度 | 极低（单一二进制） | 极高（控制面+数据面+CRD） | 中（rust proxy） | 无 |
| TCP/UDP | 自研 UDP | TCP mTLS | TCP mTLS | gRPC over HTTP/2 |
| 粘性负载均衡 | 内置，1 分钟 LRU | 需自定义 EnvoyFilter | 无 | 无 |
| 服务发现 | Redis + 静态 | k8s Service + EndpointSlice | k8s Service | DNS |
| 传输层可靠性 | FEC + ACK | TCP 累积 ACK | TCP | HTTP/2 |
| 跨节点延迟 | < 1ms (回环) | 10-50ms (iptables + sidecar) | 5-20ms | < 1ms |
| 内存占用 | ~20MB（空闲） | ~200MB（Envoy） | ~30MB | 取决于 gRPC 实现 |
| JSON-RPC | 内置 | 需额外网关 | 无 | 无 |
| 集群外运行 | ✅ | ❌ | ❌ | ✅ |

---

## 使用场景

### 场景 1：游戏房间服务器

每个游戏房间绑定固定后端实例，`sid = room_id`。Creek 确保同一房间的玩家请求始终路由到同一后端，房间关闭后 1 分钟内自动释放粘性缓存。

### 场景 2：分布式 Session 管理

Web 应用的 Session Store 分片到多个 Redis/后端实例，`sid = session_token`。Creek 的粘性路由避免跨实例查询 Session 数据。若后端故障，Leaf 自动切换到备用节点，并通过 metrics 监控 failover 频率。

### 场景 3：IoT 边缘网关

在边缘节点部署一个 Node + 多个 Leaf，Leaf 代理设备 MQTT 转 gRPC，Node 之间通过 Tight UDP 低延迟传输。不依赖中心化控制面，适合带宽有限、高延迟波动的边缘环境。

### 场景 4：微服务调试与 A/B 测试

通过 `sid = user_id` 将特定用户粘性到新版后端，其他用户仍然路由到旧版。无需部署复杂的 Istio VirtualService / DestinationRule，只需在 Leaf 层修改 StickyBalancer 配置。

---

## 设计哲学

1. **"侧车即代理"** — Leaf 是后端服务的完全代理，不是"拦截器"。后端无需感知网格存在。
2. **"UDP 优先"** — 在高吞吐、高并发场景下，UDP 的自定义可靠性远优于 TCP 的通用性。
3. **"静态即默认，Redis 为可选"** — 不支持 Kubernetes 的环境下，静态 peer 列表 + Redis 作为仅有的两个发现机制，不引入 etcd / Consul 等额外组件。
4. **"编译即交付"** — 单一二进制、零运行时依赖（除 libhiredis 可选），可交叉编译到任意 Linux / macOS / Windows 平台。
5. **"观测即代码"** — Metrics 不是"外挂 Prometheus exporter"，而是内联在每个 RPC 路径中的 `record_metric()` 调用。

---

## 未来演进方向

### 近期
- **TLS 1.3 握手加密** — Tight 握手阶段集成 mbedTLS / OpenSSL，实现端到端加密
- **OTLP 直接上报** — 除 Pull 模型外，增加 gRPC OTLP Exporter，对接 OpenTelemetry Collector
- **gRPC Server Reflection** — 兼容 grpcurl、BloomRPC 等工具直接调用

### 中期
- **WASM Filter 链** — Leaf 侧注入 wasm 插件，支持请求路由、限流、鉴权的热更新
- **S3 / NATS Backend 支持** — Backend 可注册为非 gRPC 端点类型
- **多父 Leaf** — Leaf 同时连接多个 Node，任一 Node 故障时无缝切换

### 远期
- **跨集群 Federation** — 多 DataCenter 间通过 Node 层级路由，实现区域感知
- **eBPF 加速** — 将 UDP 分片/重组和 FEC 解码卸载到 eBPF XDP hook
- **QoS 分级** — RPC 调用按 metadata 中的 `priority` 分级，高优请求使用令牌桶优先队列
