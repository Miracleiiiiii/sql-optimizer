# PRD: 基于 AI 大模型的 Spark 任务执行分析与参数优化系统

## 1. 文档信息

| 项目 | 内容 |
| --- | --- |
| 产品名称 | Spark AI Optimizer |
| 文档版本 | v1.1 |
| 创建日期 | 2026-05-10 |
| 适用范围 | Spark on YARN、Spark History Server、Hive Metastore、离线批处理任务 |
| 目标用户 | 数据平台工程师、数据开发工程师、运维工程师、任务负责人 |
| 当前阶段 | 可实施，建议先以“诊断建议系统”形态落地，暂不自动改生产参数 |

## 1.1 已确认环境

| 项目 | 已确认信息 |
| --- | --- |
| Spark 线下版本 | 3.3.1 |
| Spark 线上版本 | 3.3.2 |
| Spark History Server | 线下 `http://hadoop102:18080` 可访问 |
| YARN ResourceManager | 线下 `http://hadoop102:8088` 可访问 |
| Event Log | 已开启 |
| Event Log 目录 | `hdfs://hadoop101:8020/spark-history` |
| Event Log 保留周期 | 7 天 |
| MVP 主场景 | 独立 `spark-submit` 任务，按 applicationId 分析 |
| Hive Metastore | 通过 thrift 地址访问，地址参数化配置 |
| 调度平台 | MVP 暂不接入，用户传入 applicationId 分析 |
| 大模型接入方式 | MVP 先使用外部 OpenAI API，模型参数化配置为 GPT-5.4 或 GPT-5.5 |
| SQL 入模策略 | 允许 SQL 文本进入外部模型，保留脱敏和审计能力 |

## 2. 背景与问题

当前 Spark 任务运行在 YARN 集群上，由 YARN 进行资源调度，Hive 负责元数据管理。随着任务数量、数据规模和业务链路复杂度增长，任务执行慢、资源浪费、失败重跑、参数配置不合理等问题越来越常见。

现阶段 Spark 任务优化主要依赖人工查看 Spark UI、YARN 日志、SQL 执行计划和 Hive 表信息，存在以下痛点：

- 专家经验依赖强，普通开发难以准确判断瓶颈。
- 单任务分析过程繁琐，排查效率低。
- 调参缺少量化证据，容易凭经验反复试错。
- 历史执行信息没有沉淀为任务画像，优化经验难以复用。
- 优化前后缺少可追踪闭环，难以证明收益。

因此需要建设一个基于 Spark 历史执行数据、YARN 资源数据、Hive 元数据和 AI 大模型分析能力的任务诊断与参数优化系统。

## 3. 可实施性结论

该产品可实施，但需要明确边界：

- 第一阶段不建议做“自动调参上线”，应先做“结构化诊断 + AI 报告 + 人工采纳”。
- 不能直接依赖大模型读取 Spark UI HTML 做判断，应以 Spark History Server API、Event Log、YARN API 和 Hive 元数据为主。
- 大模型不负责原始指标计算，只负责基于结构化事实做归因解释、建议生成和报告组织。
- 参数推荐必须经过规则引擎约束，所有建议都要带指标证据、风险和验证方式。
- 是否能达到较高准确率，取决于历史任务指标完整性、Event Log 保留策略、Hive 统计信息质量和任务命名规范。

## 4. 产品目标

### 4.1 核心目标

通过读取历史 Spark 任务执行信息，自动识别性能瓶颈，并生成可解释、可验证、可追踪的 Spark 参数优化建议。

### 4.2 业务目标

- 降低 Spark 任务人工排查成本。
- 提升慢任务诊断效率。
- 减少资源浪费和无效重跑。
- 建立 Spark 任务历史画像和优化闭环。
- 为后续半自动调参和调度平台集成打基础。

### 4.3 成功指标

| 指标 | MVP 目标 | 完整版目标 |
| --- | --- | --- |
| 单任务诊断耗时 | 5 分钟以内 | 1 分钟以内 |
| 报告生成成功率 | >= 90% | >= 95% |
| 典型瓶颈识别准确率 | >= 75% | >= 85% |
| 专家复核一致率 | >= 70% | >= 80% |
| 推荐采纳后有效率 | 可统计 | >= 60% |

## 5. 用户角色

| 用户角色 | 主要诉求 |
| --- | --- |
| 数据开发工程师 | 快速知道任务为什么慢，应该优先调整什么 |
| 数据平台工程师 | 批量识别集群中的低效任务，沉淀优化经验 |
| 运维工程师 | 发现资源异常、队列拥塞、失败重试等问题 |
| 任务负责人 | 查看任务优化前后收益，评估是否采纳建议 |

## 6. 产品范围

### 6.1 MVP 范围

MVP 聚焦“单任务历史诊断”，包含：

- 输入独立 `spark-submit` 任务的 applicationId，生成单个 Spark 历史任务分析报告。
- 接入 Spark History Server REST API。
- 采集 application、job、stage、executor、SQL 基础指标。
- 采集 YARN application 基础状态、队列、资源和运行时间。
- 支持 Spark History Server、YARN ResourceManager、Hive Metastore、OpenAI API 等地址参数化配置。
- 抽取任务当前 Spark 参数快照。
- 支持 GC 压力、shuffle spill、数据倾斜、并行度不足四类诊断。
- 输出核心参数建议：`spark.executor.memory`、`spark.executor.cores`、`spark.executor.instances`、`spark.sql.shuffle.partitions`。
- 调用大模型生成可读报告。
- 保存分析报告、推荐记录和采纳状态。

### 6.2 P1/P2 扩展范围

- 读取 Spark Event Log，补齐 REST API 缺失指标。
- 接入 Hive Metastore 或离线表画像。
- 支持批量巡检。
- 支持同一任务多次执行基线对比。
- 支持 SQL 执行计划深度分析。
- 支持推荐采纳后的效果追踪。

### 6.3 暂不包含

- 生产任务参数自动修改并上线。
- 自动重跑任务验证优化效果。
- 完整调度平台改造。
- 调度平台任务与 applicationId 自动映射。
- 非 Spark 任务分析。
- Spark Streaming 深度诊断。
- Spark Thrift Server、DBeaver、JDBC 长连接场景下的单 SQL 粒度诊断。

## 7. 典型使用场景

### 7.1 单个慢任务诊断

数据开发发现某个离线任务运行时间从 40 分钟增长到 2 小时，希望快速知道原因。

用户输入 applicationId 后，系统输出：

- 任务整体耗时、状态、队列和参数配置。
- 最耗时 Stage 和异常 Stage。
- 是否存在数据倾斜、GC 压力、shuffle spill、并行度不足。
- 推荐参数、推荐理由、风险和验证方式。

### 7.2 批量低效任务巡检

平台工程师希望每天扫描前一天 Spark 任务，找出资源浪费和性能异常任务。

系统输出：

- 慢任务 Top N。
- GC 异常任务 Top N。
- Shuffle 异常任务 Top N。
- 数据倾斜任务 Top N。
- 资源浪费任务 Top N。

### 7.3 SQL 任务参数优化

系统结合 SQL 执行计划和 Hive 表统计信息，识别：

- shuffle 分区是否不合理。
- 小表是否可以广播。
- 分区裁剪是否生效。
- 是否存在 join 数据倾斜。
- 是否存在小文件导致 task 数异常。

## 8. 功能需求

### 8.1 任务数据采集

| 数据源 | MVP 是否必须 | 采集内容 |
| --- | --- | --- |
| Spark History Server API | 是 | application、job、stage、executor、SQL 基础信息 |
| YARN ResourceManager API | 是 | 队列、资源、状态、开始结束时间、失败原因 |
| Spark Event Log | 否，P1 增强 | task metrics、executor metrics、SQL metrics、事件明细 |
| Hive Metastore | 否，P1 增强 | 表、分区、文件数量、存储大小、统计信息 |

采集要求：

- 支持按 applicationId 采集。
- 支持按任务名称和时间范围检索历史任务。
- 支持失败重试和错误原因记录。
- 同一 applicationId 不重复生成基础画像。
- 采集结果保留原始 JSON 快照，便于问题追溯。

### 8.2 指标抽取

#### 8.2.1 应用级指标

- applicationId
- applicationName
- user
- queue
- startTime
- endTime
- duration
- finalStatus
- sparkVersion
- driver 配置
- executor 配置
- Spark 参数快照

#### 8.2.2 Executor 指标

- executor 数量
- executor cores
- executor memory
- executor runtime
- GC time
- GC ratio
- input bytes
- shuffle read bytes
- shuffle write bytes
- memory spill bytes
- disk spill bytes
- failed task 数量

#### 8.2.3 Stage 指标

- stageId
- attemptId
- stage name
- duration
- task count
- failed task count
- input/output bytes
- shuffle read/write bytes
- memory/disk spill bytes
- max task duration
- median task duration
- p95 task duration
- skew ratio

#### 8.2.4 SQL 指标

- executionId
- description
- SQL 文本摘要
- physical plan
- scan tables
- join types
- exchange count
- broadcast exchange count
- output rows

### 8.3 规则诊断

规则引擎用于给出可追溯的初步诊断，大模型只能基于规则和指标做解释增强。

| 问题类型 | 初始判断逻辑 | MVP |
| --- | --- | --- |
| GC 压力高 | GC time / executor runtime 超过阈值 | 是 |
| Shuffle spill 严重 | disk spill 或 memory spill 高于阈值 | 是 |
| 数据倾斜 | max task duration / median task duration 超过阈值 | 是 |
| 并行度不足 | task 数明显小于可用 executor cores | 是 |
| 分区数过少 | 单 task shuffle 数据量过大 | 是 |
| 分区数过多 | task 数过多且单 task 耗时很短 | P1 |
| 资源浪费 | executor 空闲明显或利用率低 | P1 |
| 广播 join 未生效 | 小表 join 未使用 BroadcastHashJoin | P1 |
| 小文件过多 | Hive 表文件数量高且平均文件小 | P1 |
| 队列资源不足 | YARN 排队时间过长或 container 分配慢 | P1 |

规则输出字段：

- ruleCode
- problemType
- severity
- confidence
- evidence
- suspectedCause
- tuningDirection

### 8.4 AI 大模型分析

大模型输入必须为结构化 JSON，包含：

- 任务基础信息。
- Spark 参数快照。
- 聚合后的核心指标。
- 规则诊断结果。
- 历史基线，若存在。
- SQL 摘要，若存在。

大模型输出必须为可解析 JSON：

```json
{
  "summary": "任务主要瓶颈是 shuffle spill 和 executor 内存压力。",
  "problems": [
    {
      "type": "shuffle_spill",
      "severity": "high",
      "evidence": "disk spill 320GB，shuffle write 910GB",
      "explanation": "当前分区数偏少，单 task 处理数据量过大。"
    }
  ],
  "recommendations": [
    {
      "param": "spark.sql.shuffle.partitions",
      "current": "200",
      "suggested": "800",
      "priority": "high",
      "reason": "shuffle 数据量接近 900GB，建议降低单分区数据量。",
      "risk": "分区数过高会增加调度开销。",
      "validation": "对比 task 中位数耗时、p95 耗时、spill 和总耗时。"
    }
  ]
}
```

### 8.5 参数推荐

| 参数 | MVP | 推荐依据 |
| --- | --- | --- |
| `spark.executor.memory` | 是 | GC、spill、OOM、单 task 数据量 |
| `spark.executor.cores` | 是 | task 并行度、GC 压力、CPU 使用模式 |
| `spark.executor.instances` | 是 | 总任务量、资源上限、队列约束 |
| `spark.sql.shuffle.partitions` | 是 | shuffle 数据量、task 数、单分区数据量 |
| `spark.driver.memory` | P1 | driver OOM、plan 大小、collect 行为 |
| `spark.default.parallelism` | P1 | RDD 并行度 |
| `spark.sql.autoBroadcastJoinThreshold` | P1 | 小表大小、join 类型 |
| `spark.dynamicAllocation.enabled` | P2 | executor 空闲、任务波峰波谷 |
| `spark.speculation` | P2 | 长尾 task、节点异常 |
| `spark.memory.fraction` | P2 | spill、缓存、执行内存压力 |

推荐必须包含：

- 当前值。
- 推荐值或推荐范围。
- 优先级。
- 证据指标。
- 风险说明。
- 验证方式。
- 是否允许自动应用，MVP 固定为 false。

### 8.6 报告展示

单任务报告包含：

- 任务基础信息。
- 本次执行摘要。
- 核心指标。
- 主要瓶颈结论。
- 异常 Stage 列表。
- Executor 资源分析。
- SQL 执行计划问题，若有。
- 参数推荐表。
- 风险与验证建议。
- 原始指标追溯入口。

### 8.7 历史画像与闭环

系统需要保存每次分析结果，用于后续对比：

- 同一任务多次执行对比。
- 优化前后参数对比。
- 优化前后耗时对比。
- 优化前后资源使用对比。
- 推荐是否采纳。
- 采纳后的收益统计。

## 9. 用户流程

### 9.1 单任务分析流程

1. 用户输入 applicationId。
2. 系统拉取 Spark History Server 和 YARN 数据。
3. 系统抽取并标准化指标。
4. 规则引擎生成初步诊断。
5. AI 大模型生成报告。
6. 用户查看参数建议。
7. 用户记录是否采纳。
8. 系统在后续任务执行后追踪效果。

### 9.2 批量巡检流程

1. 系统按天扫描历史任务。
2. 自动采集执行数据。
3. 识别异常任务。
4. 生成批量巡检报告。
5. 平台工程师选择重点任务。
6. 系统生成详细 AI 分析报告。

## 10. 非功能需求

### 10.1 性能

- MVP 单 application 分析时间小于 5 分钟。
- 常规任务报告生成时间小于 60 秒。
- P2 批量巡检每天支持至少 1000 个 Spark application。

### 10.2 稳定性

- Spark History Server 不可用时展示明确错误。
- 大模型调用失败时仍输出规则诊断结果。
- 部分指标缺失时允许降级分析，并在报告中标明缺失项。

### 10.3 安全

- 大模型输入不传递业务数据样本。
- SQL 文本允许进入外部模型，但需要支持脱敏、截断、开关控制和审计。
- 用户只能查看有权限的任务。
- 大模型调用日志可审计。
- API token 和模型密钥不得落库明文。

### 10.4 可解释性

- 所有参数推荐必须给出指标证据。
- 报告需要区分“指标事实”“规则判断”“AI 推理”。
- 高风险建议必须提示灰度验证。

## 11. MVP 验收标准

### 11.1 功能验收

- 输入有效 applicationId 后可以生成诊断报告。
- 报告包含任务基础信息、核心指标、瓶颈分析和参数建议。
- 对 GC 高、shuffle spill、数据倾斜、并行度不足四类问题能给出明确判断。
- 参数建议包含当前值、推荐值、理由、证据和风险。
- 大模型调用失败时仍能展示规则诊断结果。

### 11.2 数据验收

- Spark History Server 指标采集字段完整率 >= 90%。
- 同一 applicationId 不重复生成多份基础画像。
- 诊断结果与原始指标可以关联追溯。

### 11.3 效果验收

- 选取 10 个历史慢任务进行专家复核。
- 主要瓶颈判断一致率 >= 70%。
- 至少 3 个任务采纳建议后，运行耗时下降或资源使用下降。

## 12. 风险与应对

| 风险 | 应对 |
| --- | --- |
| History Server 数据不完整 | P1 增加 Event Log 解析能力 |
| Event Log 保留周期只有 7 天 | 推动开启长期归档或落地采集快照 |
| Hive 统计信息不准确 | 将 Hive 元数据作为辅助证据，不作为唯一判断 |
| 大模型输出不稳定 | 使用 JSON schema、规则兜底和输出校验 |
| 参数建议过于激进 | 引入推荐范围、风险等级和人工采纳 |
| SQL 敏感信息泄露 | SQL 脱敏、截断、访问控制和审计 |
| 不同 Spark 版本 API 差异 | 采集层做版本适配和字段缺失兼容 |

## 13. 里程碑计划

| 阶段 | 周期 | 目标 |
| --- | --- | --- |
| P0: 技术验证 | 1-2 周 | 拉取 Spark History Server 数据，生成单任务指标 JSON |
| P1: MVP | 3-5 周 | 完成单任务诊断、规则引擎、大模型报告 |
| P2: 批量巡检 | 3-4 周 | 支持每日任务扫描和异常任务榜单 |
| P3: 优化闭环 | 4-6 周 | 支持推荐采纳、效果追踪、历史基线 |
| P4: 半自动调参 | 视情况推进 | 与调度平台集成，支持灰度应用参数 |

## 14. 后续扩展

- 接入 Airflow、DolphinScheduler 或自研调度平台。
- 支持自动生成 spark-submit 参数片段。
- 支持同类任务聚类和模板化推荐。
- 支持任务失败原因分析。
- 支持 Spark Streaming 任务分析。
- 支持成本分析，如 CPU 小时、内存小时、队列资源占用。
- 支持企业内部知识库 RAG，结合历史优化案例增强报告质量。
