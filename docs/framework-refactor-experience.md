# Creek 反应式消息驱动框架重构经验总结

## 项目背景

Creek 是一个有状态服务网格边车，原有代码存在以下问题：
- `runtime.cpp` 1800+ 行，混杂 Node/Leaf 业务 + 3 个 gRPC 服务
- `tight.cpp` 1600+ 行，完整 UDP 可靠传输协议栈
- 成员变量命名不统一（`_` 后缀 vs `m_` 前缀）
- 线程循环使用 `while + sleep` 模式，缺乏统一调度

## 重构目标

1. 构建反应式消息驱动框架（Reactor 模式 + CSP 通道 + 黑板模式）
2. 拆分巨型文件，建立模块化目录结构
3. 统一代码风格（`m_` 前缀成员变量）
4. 将线程循环改为框架周期任务
5. 保持向后兼容，所有测试通过

## 完成内容

### 1. 框架骨架（14 头文件 + 12 源文件）

```
include/creek/framework/
├── time_source.hpp      # 虚拟时钟注入
├── task.hpp             # 任务命名/ID/慢任务检测
├── channel.hpp          # CSP 深拷贝队列
├── timer.hpp            # 高精度定时器 + 可跳过策略
├── blackboard.hpp       # 数据黑板 + 细粒度锁
├── message.hpp          # 阶段1→2 契约
├── change_set.hpp       # 阶段3→4 契约
├── heartbeat.hpp        # 心跳批处理
├── data_evolver.hpp     # ECA 单层演进
├── stage4.hpp           # 异步执行
├── reactor.hpp          # IO/CPU 双线程池
├── metrics.hpp          # 压力感知
└── framework.hpp        # 统一入口
```

**关键设计：**
- 四阶段架构（SEDA）：输入 → 通道 → 心跳批处理 → 执行
- 双轨驱动：消息驱动（显式）+ 数据演进（隐式）
- 值语义消息队列：禁止跨线程指针传递
- 单层数据演进：防止连锁反应

### 2. Runtime 拆分

```
include/creek/
├── node/node_runtime.hpp    # NodeRuntime 公共 API
├── leaf/leaf_runtime.hpp    # LeafRuntime 公共 API
└── rpc/                     # gRPC 服务类
    ├── greeter_service.hpp
    ├── leaf_control_service.hpp
    └── admin_service.hpp

src/
├── node/node_runtime.cpp    # Node 业务逻辑
├── leaf/leaf_runtime.cpp    # Leaf 业务逻辑
└── rpc/                     # gRPC 服务实现
```

**拆分策略：**
- 保持公共 API 不变（`creek/runtime.hpp` 仍可用）
- 内部声明移至 `src/*_impl.hpp`
- gRPC 服务通过 friend 访问 Impl 私有方法

### 3. Tight 协议库拆分

```
include/creek/tight/
├── tight.hpp          # 主公共 API
├── packet_codec.hpp   # 编解码 + CRC32
├── fec.hpp            # Reed-Solomon
├── bandwidth.hpp      # 带宽估算
└── transport.hpp      # TightTransport

src/tight/
├── transport.cpp      # 传输实现
├── packet_codec.cpp
├── fec.cpp
├── bandwidth.cpp
└── ...                # 平台相关
```

### 4. 周期任务改造

**原模式（线程 + sleep）：**
```cpp
void sync_loop() {
    while (m_running.load()) {
        std::this_thread::sleep_for(interval);
        do_work();
    }
}
```

**新模式（框架周期任务）：**
```cpp
if (m_framework) {
    m_sync_task_id = m_framework->reactor().schedule_periodic(
        "node_sync", [this] { do_sync_work(); }, interval,
        framework::TaskPriority::Normal, false);
} else {
    // 向后兼容：保留线程循环
    m_sync_thread = std::thread([this] { sync_loop(); });
}
```

## 经验教训

### 1. 大规模重构必须渐进

**错误做法：** 一次性重写所有文件
**正确做法：** 
- 先建框架骨架 + 测试
- 逐个模块迁移，每次验证编译和测试
- 保持向后兼容（新旧模式共存）

### 2. 使用 Task Agent 处理机械性重构

**适用场景：**
- 大文件拆分（1800 行 → 多个文件）
- 代码移动（保持逻辑不变）
- 批量重命名

**注意：** Task Agent 生成的代码需要人工审查，特别是：
- 成员变量命名规范
- 自引用 lambda 的悬垂引用问题
- Move-only 类型在容器中的使用

### 3. 正则批量重命名的陷阱

**问题：** `\bsize_\b` 会误匹配 `std::size_t`
**解决：** 
- 重命名后必须全文搜索 `std::m_\w+` 检查误替换
- 先小范围测试，再批量执行

### 4. C++ 特殊问题处理

**自引用 lambda：**
```cpp
// 错误：悬垂引用
auto fn = [&fn] { fn(); };

// 正确：std::function
std::function<void()> fn;
fn = [&fn] { fn(); };
```

**Move-only 类型在 priority_queue：**
```cpp
// 错误：top() 返回 const&，无法 move
TimerEntry top = heap_.top();

// 正确：使用 mutable 成员
struct TimerEntry {
    mutable Task task;  // 允许从 const& move
    TimePoint deadline;
};
```

### 5. Windows DLL 问题

**症状：** 测试退出码 `0xC0000135`（DLL 找不到）
**解决：** 
```powershell
$env:PATH = "build\bin;D:\msys64\mingw64\bin;$env:PATH"
ctest --test-dir build
```

### 6. 向后兼容设计

**关键：** 新功能是可选的，不破坏现有代码
```cpp
// 框架模式
if (m_framework) {
    // 使用框架周期任务
} else {
    // 保留原有线程循环
}
```

### 7. 测试策略

**单元测试：** 57 个框架测试
- 虚拟时钟：确定性时间控制
- 确定性重放：相同输入 = 相同输出
- 单层演进：验证不连锁

**集成测试：** e2e grpc / jsonrpc
- 验证重构不破坏现有功能
- Redis 服务发现需要外部依赖

## 最终目录结构

```
creek/
├── include/creek/
│   ├── framework/     # 反应式消息框架
│   ├── node/          # NodeRuntime API
│   ├── leaf/          # LeafRuntime API
│   ├── rpc/           # gRPC 服务
│   └── tight/         # 可复用网络库
├── src/
│   ├── framework/     # 框架实现
│   ├── node/          # Node 业务
│   ├── leaf/          # Leaf 业务
│   ├── rpc/           # gRPC 实现
│   └── tight/         # 传输实现
└── tests/unit/        # 框架测试
```

## 验证结果

- 6/6 单元测试通过
- e2e grpc 测试通过（14.5s）
- e2e jsonrpc 测试通过（13.5s）
- 57 个框架测试全部通过

## 提交信息

```
refactor: reactive message-driven framework + modular architecture

- Framework: Reactor, CSP Channel, Blackboard, Heartbeat, DataEvolver
- Runtime split: node/ + leaf/ + rpc/
- Tight split: reusable network library
- Periodic tasks: schedule_periodic/cancel_periodic
- Code style: m_ prefix member variables
- Tests: 57 framework + 6 unit + 2 e2e all pass
```
