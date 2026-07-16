# Creek 项目概述

## 项目定位

Creek 是一个**面向有状态业务**的**轻量级服务网格 Sidecar**，用 C++20 编写，以单个二进制交付。它与传统 Service Mesh（Istio、Envoy、Linkerd）的本质区别在于：**不依赖 Kubernetes、不劫持 iptables、不引入控制面/数据面分离**——所有功能（转发、注册、负载均衡、观测、沙箱执行）都在 Sidecar 进程内部完成，通过自研 Tight UDP 协议替代 TCP 作为东西向传输层。

Creek 的设计直接回答了以下工程问题：

### 1. 为什么不用 Istio / Envoy？

Istio 的 Sidecar 注入、Pilot 控制面、CRD 渲染和 iptables 规则截取流量，在小规模集群（< 50 节点）中会带来 **200-500 ms 的额外延迟**、大量内存占用和管理复杂性。Creek 不需要 Kubernetes，不需要控制面，单个 `creek_sidecar` 进程即是完整的代理、注册中心和指标采集器。

### 2. 为什么用 UDP 而不是 TCP？

TCP 提供了可靠传输，但存在**队头阻塞**和**连接中断后的慢启动惩罚**。Creek 的 Tight 协议在 UDP 之上：
- 实现了 **Reed-Solomon 单冗余 FEC**，在 N 个数据分片丢失 1 个时无需重传即可恢复
- 实现了 **Token-Bucket Pacer** 和**带宽 EWMA 估算**，避免突发流量导致的丢包
- 实现了**逐包 ACK 确认模型**（非 TCP 累积 ACK），乱序不产生虚假重传
- 握手阶段通过 **ECDH 密钥交换 + AES-256-GCM AEAD** 加密数据分组

这使得跨 Node 的 Tight 链路在 1% 丢包率下比 TCP 快约 **2-3 倍**。

### 3. 为什么需要粘性负载均衡？

绝大多数 RPC 网关只支持轮询、加权随机或最少连接数——这些策略对**无状态服务**足够，但对**有状态服务**的杀伤力是致命的：同一 session 被轮询到不同后端，后果是状态不一致、session 丢失或重放。

Creek 在 Leaf 侧解析 gRPC Metadata 或请求体中 `sticky` 和 `sid` 字段，对 `(service, sid)` 建立 **1 分钟粘性路由缓存**。同时支持 `shard_key` / `tenant_id` 哈希固定路由。后端离线时自动从 StickyBalancer 中 invalidate 并重选下一个健康节点。

### 4. 可观测性如何融入？

Creek 的 `MetricsStore` 采用**双分钟桶 + 累积桶**设计：
- **current**：当前 1 分钟的滑窗
- **previous**：上一分钟的完整快照
- **cumulative**：自启动以来的累加计数器

通过 `GET /metrics?take=1` 读取并清零 cumulative 桶，实现"拉取一次即清零"的语义。同时内置 **OTLP HTTP Exporter** 可将指标推送至 OpenTelemetry Collector。

完整的 **W3C Trace Context** 支持：`traceparent`/`tracestate` 从客户端注入后贯穿 Tight Mesh 全链路，每个 hop 生成唯一 `span_id`。

### 5. 如何保证后端高可用？

- **多父 Leaf**：Leaf 同时连接多个 Node，任一 Node 故障时自动切换
- **Circuit Breaker**：基于连续失败数、错误率和平均延迟的三级熔断，Half-Open 自动探活恢复
- **QoS 优先级**：RPC 调用三级优先（normal/high/critical），优先级队列调度

### 6. 如何实现流量控制和灰度发布？

通过 **声明式 Admin gRPC API** 热更新路由策略，无需重启：

- `SetStickyStrategy` — 热更新 StickyBalancer TTL 和分片策略
- `SetBreakerConfig` — 重置指定或全局熔断器
- `PushWasmModule` — 远程推送 .wasm 模块，Node 侧沙箱执行

内置 **JSON 配置管道** 支持四种策略：`delay`（故障注入）、`mirror`（流量镜像）、`canary`（灰度分流）、`reject`（限流熔断）。同时提供 **wasm3 WASM 运行时**，用户可编写 AssemblyScript/Rust 编译为 .wasm 并通过 gRPC 推送加载。

---

## 技术架构

详见 [architecture.md](architecture.md)。

### Leaf / Node 双层拓扑

```
                   ┌──────────────┐
                   │   Node Mesh  │   ← 全网状 Tight UDP (ECDH+AEAD)
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
- **Leaf**：负责 gRPC / JSON-RPC 服务暴露、后端注册与代理、粘性负载均衡、CircuitBreaker 保护、指标采集和 WASM 沙箱执行。

### 完整功能集

| 类别 | 已实现特性 |
|---|---|
| 传输 | Tight UDP、ECDH+AEAD 加密、R-S FEC、Token-Bucket Pacer、带宽估算 |
| 拓扑 | Leaf/Node 双层、多父 Leaf、静态 peer + Redis 服务发现 |
| 路由 | RoutedRequest/Response、hop_limit 防环、LWW 目录同步、W3C Trace Context |
| 负载均衡 | sid 粘性 1 分钟 LRU、shard_key 哈希路由、StickyBalancer 热更新 |
| 高可用 | Circuit Breaker（Closed/Open/HalfOpen）、QoS 三级优先级 |
| 控制面 | Admin gRPC API（5 接口）、creek_admin_client CLI、声明式路由规则 |
| 观测 | MetricsStore（分钟桶+take）、OTLP HTTP Exporter、W3C Trace Context |
| 沙箱 | wasm3 WASM 运行时、JSON 配置管道（delay/mirror/canary/reject）、gRPC 推送 .wasm |
| 协议 | gRPC Server Reflection、JSON-RPC HTTP、Header 驱动的路由参数传递 |
| 测试 | 5 项单元测试（200+ 断言）、gRPC + JSON-RPC 双 E2E |

---

## 与同类项目的对比

| 特性 | Creek | Istio (Envoy) | Linkerd | gRPC 直连 |
|---|---|---|---|---|
| 架构复杂度 | 极低（单一二进制） | 极高（控制面+数据面+CRD） | 中（rust proxy） | 无 |
| TCP/UDP | 自研 UDP + ECDH/AEAD | TCP mTLS | TCP mTLS | gRPC over HTTP/2 |
| 粘性负载均衡 | 内置 1 分钟 LRU + shard_key | 需自定义 EnvoyFilter | 无 | 无 |
| 服务发现 | Redis + 静态 | k8s Service + EndpointSlice | k8s Service | DNS |
| Circuit Breaker | 内置 | 需 Envoy outlier detection | 需额外配置 | 无 |
| WASM 沙箱 | 内置 wasm3 + Admin API 推送 | 需 Envoy wasm filter | 无 | 无 |
| 分布式追踪 | W3C Trace Context | 需 Envoy tracing | 需额外配置 | 需应用层实现 |
| 跨节点延迟 | < 1ms (回环) | 10-50ms | 5-20ms | < 1ms |
| JSON-RPC | 内置 + Header 路由 | 需额外网关 | 无 | 无 |
| 集群外运行 | ✅ | ❌ | ❌ | ✅ |

---

## 使用场景

### 场景 1：游戏房间服务器

每个游戏房间绑定固定后端实例，`sid = room_id`。Creek 确保同一房间的玩家请求始终路由到同一后端，房间关闭后 1 分钟内自动释放粘性缓存。CircuitBreaker 在房间服务器异常时自动熔断。

### 场景 2：分布式 Session 管理

Web 应用的 Session Store 分片到多个后端实例，`sid = session_token`。Creek 的粘性路由避免跨实例查询 Session 数据。

### 场景 3：IoT 边缘网关

边缘节点部署 Node + 多 Leaf，Leaf 代理 MQTT 转 gRPC，Node 间通过 Tight UDP 低延迟传输。不依赖中心化控制面，适合带宽有限的边缘环境。

### 场景 4：灰度发布与故障演练

通过 Admin API 推送 WASM 过滤器实现：canary 20% 流量到新版、delay 500ms 模拟后端延迟、mirror 影子流量到预发布环境。所有策略热更新不重启。

### 场景 5：多租户分片路由

通过 `shard_key = tenant_id` 将租户请求哈希到固定后端。新租户上线时自动均衡到健康节点，无需在应用层维护路由表。

---

## 设计哲学

1. **"侧车即代理"** — Leaf 是后端服务的完全代理，不是"拦截器"。后端无需感知网格存在。
2. **"UDP 优先"** — 在高吞吐、高并发场景下，UDP 的自定义可靠性远优于 TCP 的通用性。
3. **"静态即默认，Redis 为可选"** — 不依赖 Kubernetes 或 etcd，静态 peer + Redis 覆盖全部部署场景。
4. **"编译即交付"** — 单一二进制，可交叉编译到任意 Linux / macOS / Windows 平台。
5. **"观测即代码"** — Metrics 和 Trace 内联在每个 RPC 路径中，不需要外挂 exporter。
6. **"沙箱即安全"** — WASM 模块在 wasm3 解释器中隔离执行，死循环或非法内存访问不影响宿主进程。
