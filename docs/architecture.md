# Creek 架构设计

## 1. 总体架构

Creek 是一个两层 Sidecar 服务网格：

- **Node**：组成全网状（full mesh）拓扑，负责跨节点路由与目录同步
- **Leaf**：叶子节点，挂载到某一个 Node，提供 gRPC / JSON-RPC 业务入口

```mermaid
graph TB
    N1[(Node-1)]
    N2[(Node-2)]
    N1 <-->|Tight UDP| N2

    L1((Leaf entry))
    L2((Leaf service-1))
    L3((Leaf service-2))

    B1[Backend-1]
    B2[Backend-2]
    C[Client]

    C -->|gRPC| L1
    L1 -->|parent| N1
    L2 -->|parent| N2
    L3 -->|parent| N2
    L2 -->|gRPC| B1
    L3 -->|gRPC| B2
    N1 <-->|Directory Sync| N2
    L1 -.->|Register| N1
    B1 -.->|Register| L2
    B2 -.->|Register| L3
```

核心数据流：

```mermaid
sequenceDiagram
    participant C as Client
    participant EL as Entry Leaf
    participant N1 as Node-1
    participant N2 as Node-2
    participant SL as Service Leaf
    participant B as Backend

    C->>EL: gRPC SayHello(name, sid, sticky)
    EL->>EL: 查EndpointDirectory选StickyBalancer
    EL->>N1: WireMessage(RoutedRequest)
    N1->>N2: 转发 RoutedRequest
    N2->>SL: 路由到目标 Leaf
    SL->>B: gRPC SayHello
    B-->>SL: HelloReply
    SL-->>N2: WireMessage(RoutedResponse)
    N2-->>N1: 转发 RoutedResponse
    N1-->>EL: RoutedResponse
    EL-->>C: HelloReply
```

## 2. Leaf / Node 角色

### Node (NodeRuntime)

Node 维护全网状连接，负责：

- 与其他 Node 建立 Tight UDP 连接
- 接收并合并 Leaf 上报的 `DirectorySnapshot`
- 在同行 Node 之间广播目录变更
- 将 `RoutedRequest` 正确路由到目标 Leaf 或转发到其他 Node
- 支持可选的 Redis 服务发现

**关键配置 (`NodeConfig`)**：

| 字段 | 说明 |
|---|---|
| `id` | Node 唯一标识 |
| `udp_bind` | Tight 协议 UDP 绑定地址 |
| `peers` | 对等 Node 列表 (id@host:port) |
| `metrics_bind` | Metrics HTTP 服务地址 |
| `sync_interval` | 目录广播周期（默认 15s） |
| `metric_period` | 指标轮转周期（默认 60s） |
| `redis` | Redis 服务发现选项（可选） |

### Leaf (LeafRuntime)

Leaf 是后端服务的完全代理，负责：

- 暴露 gRPC `Greeter` 和 `LeafControl` 服务
- 接收 Backend 的注册与心跳，维护本地端点表
- 根据请求中的 `sid`/`shard_key` 进行粘性或分片负载均衡
- 将 RPC 调用转化为 `RoutedRequest` 发送到所属 Node
- 将本地端点变更通过 `DirectorySnapshot` 上报给 Node
- 启动 Metrics 服务（HTTP `GET /metrics`）

**关键配置 (`LeafConfig`)**：

| 字段 | 说明 |
|---|---|
| `id` | Leaf 唯一标识 |
| `udp_bind` | Tight 协议 UDP 绑定地址 |
| `grpc_bind` | gRPC 服务监听地址 |
| `json_bind` | JSON-RPC HTTP 服务地址（可选） |
| `parents` | 父 Node 列表 (id@host:port)，支持多父 |
| `rpc_timeout` | RPC 调用超时（默认 15s） |

## 3. Tight 传输协议

Tight 是 Creek 自研的基于 UDP 的可靠传输协议，专为服务网格内的高吞吐、低延迟通信设计。

### 协议栈

```mermaid
graph TB
    APP["Application Layer / WireMessage"]
    REL["Reliability Layer / ACK / Retransmit"]
    FEC["FEC Layer / Reed-Solomon"]
    FRAG["Fragmentation Layer / MTU"]
    RATE["Rate Control / Token Bucket + BWE"]
    LINK["Link Layer / Handshake / Heartbeat / CRC32"]
    UDP[UDP Socket]

    APP --> REL
    REL --> FEC
    FEC --> FRAG
    FRAG --> RATE
    RATE --> LINK
    LINK --> UDP
```

### 数据包格式

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Magic (0x54474854)                     |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Version |  Type  |               Flags                         |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Client ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Session ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Sequence                              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Acknowledgment                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          Message ID                            |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  Fragment Index |  Fragment Count|  Payload Size |   Reserved   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                            Tick                                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Checksum                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| 分组类型 | 值 | 说明 |
|---|---|---|
| `Handshake` | 1 | 握手请求 |
| `HandshakeAck` | 2 | 握手确认 |
| `Online` | 3 | 上线通知 |
| `Heartbeat` | 4 | 心跳 |
| `Bye` | 5 | 断开 |
| `Data` | 6 | 数据分片 |
| `Ack` | 7 | 确认 |
| `Parity` | 8 | FEC 奇偶校验分片 |

### 关键机制

**链路状态机：**

```mermaid
stateDiagram-v2
    [*] --> Handshake
    Handshake --> Established: Handshake ACK
    Established --> Online: Online 消息
    Online --> Closed: 超时/Bye
    Closed --> Handshake: 配置了重连
    Handshake --> Handshake: 定时重发
```

**FEC 自适应纠删码：**

每个消息被分为多个数据分片，根据当前 RTT 动态决定校验片数量：
- RTT < 5ms → 1 校验片（可恢复 1 个丢失分片）
- RTT 5-20ms → 2 校验片（旋转 XOR，可恢复多个丢失分片）
- 校验片通过 Reed-Solomon XOR 算法生成，旋转分组实现多片独立恢复

**Token Bucket 速率控制：**

发送端使用令牌桶 + 带宽估算 (BandwidthEstimator) 动态调整发送速率，基于 ACK 的字节数和 RTT 估算可用带宽。

**CRC32 校验：**

所有数据包末尾附 CRC32 校验和，用以检测传输错误。

### WireMessage

Tight 协议的上层载荷为 Protobuf 编码的 `WireMessage`：

```protobuf
message WireMessage {
  oneof body {
    DirectorySnapshot directory = 1;
    RoutedRequest request = 2;
    RoutedResponse response = 3;
  }
}
```

## 4. 目录同步 (Directory Sync)

`EndpointDirectory` 是全局服务发现的数据结构，存储所有活跃 Backend 的注册信息。

### 数据模型

```protobuf
message Endpoint {
  string endpoint_id = 1;
  string service = 2;
  string owner_leaf = 3;
  string owner_node = 4;
  string target = 5;
  uint64 version = 6;
  uint64 updated_ms = 7;
  bool alive = 8;
}

message DirectorySnapshot {
  string source_id = 1;
  uint64 version = 2;
  uint64 generated_ms = 3;
  repeated Endpoint endpoints = 4;
}
```

### 冲突解决

Endpoint 合并采用 **Last-Write-Wins** 策略，以 `version` 为主键、`updated_ms` 为副键：

1. 若新版本号 > 本地版本号 → 覆盖
2. 若版本号相同但时间戳更新 → 覆盖
3. 否则忽略

### 同步流程

```mermaid
sequenceDiagram
    participant B as Backend
    participant SL as Service Leaf
    participant N2 as Node-2
    participant N1 as Node-1

    B->>SL: Register(Endpoint)
    SL->>SL: upsert_local()
    SL->>N2: WireMessage(DirectorySnapshot)
    N2->>N2: merge(Snapshot)
    N2->>N1: 广播 Snapshot
    N1->>N1: merge(Snapshot)
    N1->>EL: 推送给入口 Leaf
    EL->>EL: merge(Snapshot)
```

## 5. 路由 (Routing)

### RoutedRequest / RoutedResponse

跨节点 RPC 通过 Node Mesh 转发：

```protobuf
message RoutedRequest {
  string request_id = 1;
  string origin_leaf = 2;
  string origin_node = 3;
  string destination_leaf = 4;
  string destination_node = 5;
  string endpoint_id = 6;
  string rpc_name = 7;
  map<string, string> metadata = 8;
  bytes body = 9;
  uint64 deadline_ms = 10;
  uint32 hop_limit = 11;
}
```

### 路由逻辑

```mermaid
graph TD
    REQ["RoutedRequest 到达"]
    CHK1{"destination_node 是当前Node?"}
    CHK2{"destination_leaf 存在?"}
    CHK3{"hop_limit > 0?"}
    CHK4{"目标Node已知?"}
    S1[转发给目标Leaf]
    S2[返回错误: leaf_not_found]
    S3[返回错误: hop_limit_exceeded]
    S4[返回错误: node_not_found]
    S5[hop_limit-1转发到目标Node]

    REQ --> CHK1
    CHK1 -->|是| CHK2
    CHK1 -->|否| CHK3
    CHK2 -->|是| S1
    CHK2 -->|否| S2
    CHK3 -->|是| CHK4
    CHK3 -->|否| S3
    CHK4 -->|是| S5
    CHK4 -->|否| S4
```

## 6. 粘性负载均衡 (StickyBalancer)

`StickyBalancer` 实现基于 `sid`（Session ID）的会话粘性路由。

### 算法

1. 从请求 Metadata 中提取 `sticky`（bool）和 `sid`（string）
2. 若非粘性请求 → 轮询选择
3. 若粘性请求：
   - 构造键 `service|sid`
   - 查找 LRU 缓存中的绑定关系
   - 命中且目标存活 → 返回
   - 未命中或目标离线 → 重新轮询选择并更新绑定

### 缓存管理

- 容量默认 4096 条
- TTL 默认 1 分钟
- 超过 TTL 未访问的条目进入 LRU 淘汰
- 调用 `invalidate()` 主动清除失败的绑定

```mermaid
graph TD
    PICK["pick(service, metadata)"]
    SHARD{"有 shard_key?"}
    HASH["shard_key哈希选固定endpoint"]
    STICKY{"sticky?"}
    HAS_SID{"有 sid?"}
    CACHED{"缓存命中?"}
    ALIVE{"目标存活?"}
    RR[轮询选择]
    REBIND[重新绑定]
    RET[返回 Endpoint]

    PICK --> SHARD
    SHARD -->|是| HASH
    SHARD -->|否| STICKY
    HASH --> RET
    STICKY -->|否| RR
    STICKY -->|是| HAS_SID
    HAS_SID -->|否| RR
    HAS_SID -->|是| CACHED
    CACHED -->|是| ALIVE
    CACHED -->|否| REBIND
    ALIVE -->|是| RET
    ALIVE -->|否| REBIND
    RR --> RET
    REBIND --> RET
```

## 7. 声明式路由控制面

### Admin gRPC API

Leaf 侧通过 `Admin` gRPC 服务暴露接口，供运维工具远程控制路由行为：

| 接口 | 说明 |
|---|---|
| `SetStickyStrategy` | 热更新粘性路由策略和 TTL |
| `SetBreakerConfig` | 重置指定或全部熔断器状态 |
| `PushWasmModule` | 向 Leaf 推送 .wasm 模块 |
| `ListWasmModules` | 查询已加载的模块列表 |
| `UnloadWasmModule` | 卸载指定 WASM 模块 |

### creek_admin_client CLI

```bash
creek_admin_client --target 127.0.0.1:9000 list-wasm
creek_admin_client --target 127.0.0.1:9000 push-wasm my_filter ./sample_filter.wasm
creek_admin_client --target 127.0.0.1:9000 sticky creek.v1.Greeter 0 120000
creek_admin_client --target 127.0.0.1:9000 breaker backend-1
```

### WASM 热加载流程

```mermaid
sequenceDiagram
    participant Admin as creek_admin_client
    participant Leaf as Leaf gRPC Admin
    participant RT as WasmRuntime

    Admin->>Leaf: PushWasm(name, wasm_bytes)
    Leaf->>RT: load_module(wasm_bytes)
    RT-->>Leaf: module_id
    Leaf-->>Admin: PushWasmReply(ok, module_id)
```

## 8. W3C Trace Context 分布式追踪

Creek 在 Tight Mesh 全链路中注入 W3C Trace Context，支持与 OpenTelemetry / Jaeger / Zipkin 集成。

### 格式

```
traceparent: 00-{trace_id(32 hex)}-{span_id(16 hex)}-{flags(2 hex)}
tracestate: vendor-specific key=value pairs
```

### 传播链路

```mermaid
sequenceDiagram
    participant C as Client
    participant EL as Entry Leaf
    participant N1 as Node-1
    participant N2 as Node-2
    participant SL as Service Leaf
    participant B as Backend

    C->>EL: traceparent
    EL->>EL: create child span1
    EL->>N1: RoutedRequest with metadata
    N1->>N2: 透传 metadata
    N2->>SL: 透传 metadata
    SL->>SL: create child span2
    SL->>B: gRPC SayHello
    B-->>SL: response
    SL-->>N2: RoutedResponse
    N2-->>N1: RoutedResponse
    N1-->>EL: RoutedResponse
    EL-->>C: HelloReply
```

### TraceContext API

```cpp
TraceSpan span = TraceContext::parse_traceparent(header);
TraceSpan child = TraceContext::create_child(span);
```

请求未携带 `traceparent` 时，Entry Leaf 自动生成 `trace_id` 和 root `span_id`。
