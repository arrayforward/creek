---
name: cpp-large-refactor
description: >
  大型 C++ 项目重构 Skill：框架搭建、模块拆分、代码风格统一、向后兼容迁移。
  适用于 1000+ 行巨型文件拆分、反应式框架集成、线程模型改造等场景。
labels: [cpp, refactor, architecture, framework, migration]
---

# C++ 大型项目重构 Skill

## 1. 重构前评估

### 1.1 识别问题代码
- **巨型文件**：单文件 > 1000 行，混杂多个职责
- **命名混乱**：成员变量 `_` 后缀与 `m_` 前缀混用
- **线程滥用**：`while + sleep` 循环，缺乏统一调度
- **紧耦合**：业务逻辑与 RPC 服务、传输层混杂

### 1.2 制定拆分策略
```
原文件          →    目标结构
runtime.cpp     →    node/ + leaf/ + rpc/
tight.cpp       →    tight/ (可复用库)
blocking_queue  →    framework::CopyChannel
```

## 2. 渐进式重构流程

### 阶段1：框架骨架
1. 创建 `include/creek/framework/` 目录
2. 实现核心组件：
   - `TimeSource`（虚拟时钟，可测试性）
   - `CopyChannel`（CSP 深拷贝队列）
   - `Blackboard`（数据黑板 + 细粒度锁）
   - `Reactor`（IO/CPU 线程池）
   - `MessageHeartbeat`（批处理）
   - `DataEvolver`（单层 ECA）
3. 编写 50+ 单元测试
4. 验证编译和测试通过

### 阶段2：模块拆分
使用 **Task Agent** 处理机械性代码移动：
- 大文件拆分（1800 行 → 多个文件）
- 保持公共 API 不变
- 内部声明移至 `src/*_impl.hpp`

**人工审查要点：**
- 成员变量命名（`m_` 前缀）
- 自引用 lambda 悬垂引用
- Move-only 类型容器使用

### 阶段3：框架集成
```cpp
// 向后兼容设计
if (m_framework) {
    // 新模式：框架周期任务
    m_task_id = m_framework->reactor().schedule_periodic(
        "task_name", [this] { do_work(); }, interval, ...);
} else {
    // 旧模式：线程循环（保留）
    m_thread = std::thread([this] { loop(); });
}
```

### 阶段4：代码风格统一
批量重命名 `_` 后缀 → `m_` 前缀：
```powershell
# 正则替换后必须检查误替换
$pattern = "\b$([regex]::Escape($key))"
# 全文搜索 std::m_\w+ 验证
```

## 3. 关键设计模式

### 3.1 四阶段架构（SEDA）
```
阶段1: 外部输入 → 纯值消息
阶段2: 消息通道 → CSP 深拷贝队列
阶段3: 心跳批处理 → 单层数据演进
阶段4: 执行指令 → 异步外部调用
```

### 3.2 双轨驱动
- **消息驱动**：显式事件，心跳内批量处理
- **数据演进**：隐式条件，ECA 规则，单层触发防连锁

### 3.3 值语义消息队列
```cpp
template <typename T>
class CopyChannel {
    // 入队深拷贝，出队深拷贝，禁止指针传递
    void send(const T& item);   // 拷贝构造
    void send(T&& item);        // 移动构造
    std::optional<T> recv();    // 移动赋值
};
```

## 4. 常见问题与解决

### 4.1 自引用 Lambda 悬垂
```cpp
// 错误
auto fn = [&fn] { fn(); };  // fn 销毁后悬垂

// 正确
std::function<void()> fn;
fn = [&fn] { fn(); };
```

### 4.2 Move-only 类型在 priority_queue
```cpp
// 错误：top() 返回 const&
TimerEntry top = heap_.top();  // 需要拷贝

// 正确：mutable 成员允许 move
struct TimerEntry {
    mutable Task task;  // 可从 const& move
    TimePoint deadline;
};
```

### 4.3 Windows DLL 找不到
```powershell
# 症状：退出码 0xC0000135
# 解决：设置 PATH
$env:PATH = "build\bin;D:\msys64\mingw64\bin;$env:PATH"
```

### 4.4 正则重命名误匹配
```powershell
# 错误：\bsize_\b 匹配 std::size_t
# 解决：替换后全文搜索 std::m_\w+
```

## 5. 测试策略

### 5.1 单元测试（框架）
- **虚拟时钟**：`VirtualTimeSource::advance()` 确定性控制
- **确定性重放**：相同输入 + 相同初始状态 = 相同输出
- **单层演进验证**：演进产生的新变更留待下轮

### 5.2 集成测试（e2e）
- 保持现有 e2e 测试通过
- Redis 依赖需要外部服务

## 6. 提交规范

```
refactor: <scope>: <description>

## Framework (new)
- Add creek::framework reactive message-driven framework
- 57 unit tests

## Runtime Split
- Split runtime.cpp into node/ + leaf/ + rpc/

## Tight Split
- Split tight.cpp into reusable network library

## Code Style
- All member variables use m_ prefix
```

## 7. 检查清单

重构完成后验证：
- [ ] 所有单元测试通过
- [ ] e2e 测试通过
- [ ] 公共 API 向后兼容
- [ ] 无 `std::m_\w+` 误替换
- [ ] 成员变量统一 `m_` 前缀
- [ ] 新功能可选（不强制使用框架）
- [ ] 提交信息完整描述变更
