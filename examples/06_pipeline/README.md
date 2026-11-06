# 06 Pipeline：多级流水线（跨 namespace）

完整的三级流水线，涉及 2 个 namespace，展示 `TaskResult.emissions` 驱动的级联投递：

```
stockev_list:fetch → finance:calculate → stockev_list:store
```

## 运行（顺序关键：先下游后上游）

```bash
# 终端 1: store worker（末级，stockev namespace）
python examples/06_pipeline/stockev/store_worker.py

# 终端 2: calculate worker（中级，finance namespace）
python examples/06_pipeline/finance/calculate_worker.py

# 终端 3: fetch worker（首级，stockev namespace）
python examples/06_pipeline/stockev/fetch_worker.py

# 终端 4: 生成首批任务
python examples/06_pipeline/generator.py
```

## 文件

| 文件 | 角色 |
|---|---|
| `generator.py` | 生产者：只投递第一级，用确定性时间桶构造 `logical_key` |
| `stockev/fetch_worker.py` | 第 1 级：抓取行情，emissions fan-out 到 finance namespace |
| `finance/calculate_worker.py` | 第 2 级：计算 MA，emissions 投回 stockev namespace |
| `stockev/store_worker.py` | 末级：消费结果（示例用 stdout 代替幂等 upsert） |

## 学习点

- **启动顺序**：必须先启动下游 Worker，否则中间队列堆积
- **投递先于 ack**：Worker 先把 emissions 投到下游、再 ack 上游；"投递后 ack 前"崩溃会导致重跑重复投递。下游 `logical_key` 从父 task_id 派生，并用 `dedup_until` 保留两天；超过窗口的重复仍需业务存储的唯一约束或幂等处理。
- **跨 namespace**：`result_queue` 可以指向任意 namespace 的队列，血缘（`parent_task_id`/`trace_id`）保持不断
- **生产者只管第一级**：后两级由各阶段 Worker 通过 emissions 自动驱动，pipeline 拓扑分散在 Worker 代码中
