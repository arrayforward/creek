# Creek 端到端教程

本教程从源码编译开始，逐步启动完整的双 Node 集群，包含 Node、Leaf、Backend 和 Client，并演示粘性路由与故障转移。

## 环境准备

- GCC 12+ / Clang 15+ / MSVC 2022+（C++20）
- CMake >= 3.24
- Protobuf + gRPC >= 1.40
- nlohmann_json >= 3.0
- hiredis (可选，Redis 功能)
- 操作系统：Linux / macOS / Windows (MSYS2 MinGW64)

## 第一步：编译

```bash
git clone https://github.com/anomalyco/creek.git
cd creek
mkdir build && cd build

# Linux / macOS
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# Windows MSYS2 MinGW64
cmake .. -G "MinGW Makefiles" -DCMAKE_BUILD_TYPE=Release
mingw32-make -j$(nproc)
```

验证编译产物：

```bash
ls bin/

# 预期输出：
# creek_sidecar(.exe)
# creek_hello_server(.exe)
# creek_hello_client(.exe)
```

## 第二步：启动 Node 集群

启动两个 Node 组成全网状网络。**先启动 Node-1，再启动 Node-2。**

### 终端 1 - Node-1

```bash
./bin/creek_sidecar node \
    --id node-1 \
    --udp 127.0.0.1:10000 \
    --peer node-2@127.0.0.1:10001 \
    --metrics 127.0.0.1:20001 \
    --token my-cluster-token \
    --sync-ms 1000 \
    --heartbeat-ms 100 \
    --dead-timeout-ms 3000
```

### 终端 2 - Node-2

```bash
./bin/creek_sidecar node \
    --id node-2 \
    --udp 127.0.0.1:10001 \
    --peer node-1@127.0.0.1:10000 \
    --metrics 127.0.0.1:20002 \
    --token my-cluster-token \
    --sync-ms 1000 \
    --heartbeat-ms 100 \
    --dead-timeout-ms 3000
```

**预期：** 两个 Node 通过 Tight 协议握手上线，`stderr` 中可能出现 Tight 连接相关日志。

## 第三步：启动 Leaf

### 终端 3 - 入口 Leaf (client-leaf)

此 Leaf 是客户端请求的入口，挂载到 Node-1。

```bash
./bin/creek_sidecar leaf \
    --id client-leaf \
    --udp 127.0.0.1:10002 \
    --parent node-1@127.0.0.1:10000 \
    --grpc 127.0.0.1:9000 \
    --metrics 127.0.0.1:20010 \
    --token my-cluster-token \
    --sync-ms 1000 \
    --heartbeat-ms 100 \
    --dead-timeout-ms 3000 \
    --backend-timeout-ms 3000 \
    --rpc-timeout-ms 5000
```

预期日志：

```
[creek-leaf] ... register ep=...
```

### 终端 4 - 服务 Leaf 1 (service-leaf-1)

此 Leaf 挂载到 Node-2，负责托管 Backend-1。

```bash
./bin/creek_sidecar leaf \
    --id service-leaf-1 \
    --udp 127.0.0.1:10003 \
    --parent node-2@127.0.0.1:10001 \
    --grpc 127.0.0.1:9001 \
    --metrics 127.0.0.1:20011 \
    --token my-cluster-token \
    --sync-ms 1000 \
    --heartbeat-ms 100 \
    --dead-timeout-ms 3000 \
    --backend-timeout-ms 3000 \
    --rpc-timeout-ms 5000
```

### 终端 5 - 服务 Leaf 2 (service-leaf-2)

```bash
./bin/creek_sidecar leaf \
    --id service-leaf-2 \
    --udp 127.0.0.1:10004 \
    --parent node-2@127.0.0.1:10001 \
    --grpc 127.0.0.1:9002 \
    --metrics 127.0.0.1:20012 \
    --token my-cluster-token \
    --sync-ms 1000 \
    --heartbeat-ms 100 \
    --dead-timeout-ms 3000 \
    --backend-timeout-ms 3000 \
    --rpc-timeout-ms 5000
```

## 第四步：启动 Backend

### 终端 6 - Backend-1

注册到 service-leaf-1 (gRPC 地址 127.0.0.1:9001)。

```bash
./bin/creek_hello_server \
    --id backend-1 \
    --listen 127.0.0.1:7001 \
    --leaf 127.0.0.1:9001
```

预期输出：

```
[creek-leaf] register ep=backend-1 svc=creek.v1.Greeter target=127.0.0.1:7001
```

### 终端 7 - Backend-2

注册到 service-leaf-2 (gRPC 地址 127.0.0.1:9002)。

```bash
./bin/creek_hello_server \
    --id backend-2 \
    --listen 127.0.0.1:7002 \
    --leaf 127.0.0.1:9002
```

## 第五步：发起 Client 调用

Backend 注册后，目录同步将在数秒内完成。然后通过 client-leaf 发起请求。

### 终端 8 - 简单调用

```bash
./bin/creek_hello_client \
    --target 127.0.0.1:9000 \
    --name world \
    --sid session-1 \
    --sticky true
```

预期输出：

```
backend-1	Hello, world from backend-1
```

### 验证粘性路由

```bash
./bin/creek_hello_client \
    --target 127.0.0.1:9000 \
    --name world \
    --sid session-1 \
    --sticky true \
    --count 10
```

预期输出（所有 10 次请求命中同一个 Backend）：

```
backend-1	Hello, world from backend-1
backend-1	Hello, world from backend-1
...（共 10 条，backend_id 不变）
```

### 验证轮询（非粘性）

```bash
./bin/creek_hello_client \
    --target 127.0.0.1:9000 \
    --sid session-2 \
    --sticky false \
    --count 5
```

预期输出（后台在两个 Backend 之间轮询）：

```
backend-1	Hello, world from backend-1
backend-2	Hello, world from backend-2
backend-1	Hello, world from backend-1
backend-2	Hello, world from backend-2
backend-1	Hello, world from backend-1
```

## 第六步：故障转移

停掉当前粘性路由命中的 Backend 进程（Ctrl+C 在终端 6 或 7），观察调用是否自动切换到另一个 Backend。

```bash
# 假设之前粘性命中 backend-1，kill 掉 backend-1 后
./bin/creek_hello_client \
    --target 127.0.0.1:9000 \
    --name world \
    --sid session-1 \
    --sticky true
```

预期输出（自动切换到 backend-2）：

```
backend-2	Hello, world from backend-2
```

## 第七步：查看 Metrics

```bash
# OpenMetrics 格式
curl http://127.0.0.1:20010/metrics

# JSON 格式
curl http://127.0.0.1:20010/stats
curl "http://127.0.0.1:20010/stats?previous=1"
curl "http://127.0.0.1:20010/stats?take=1"

# 健康检查
curl http://127.0.0.1:20010/healthz
```

## 第八步：JSON-RPC 调用

如果 Leaf 启用了 `--json` 参数启动 JSON-RPC 服务：

```bash
curl -X POST http://127.0.0.1:9000/rpc \
    -H "Content-Type: application/json" \
    -d '{
        "jsonrpc": "2.0",
        "id": "1",
        "method": "SayHello",
        "params": {
            "name": "json-test",
            "sid": "session-1",
            "sticky": true
        }
    }'
```

预期输出：

```json
{
  "jsonrpc": "2.0",
  "id": "1",
  "result": {
    "message": "Hello, json-test from backend-1",
    "backend_id": "backend-1"
  }
}
```

## 拓扑验证总结

最终拓扑：

```
client-leaf (UDP:10002) ──parent──▶ node-1 (UDP:10000) ◀──peer──▶ node-2 (UDP:10001) ◀──parent── service-leaf-1 (UDP:10003)
                                        │                                                      │
                                        │                                                      └── backend-1 (TCP:7001)
                                        │
                                        └──parent── service-leaf-2 (UDP:10004) ── backend-2 (TCP:7002)

gRPC 入口: client-leaf :9000
```

完整的数据流：**Client → client-leaf (gRPC) → node-1 (Tight UDP) → node-2 (Tight UDP) → service-leaf-1/2 (gRPC) → Backend**

## 清理

在所有终端中按 `Ctrl+C` 停止进程。Node 和 Leaf 会优雅退出（发送 Bye 消息）。
