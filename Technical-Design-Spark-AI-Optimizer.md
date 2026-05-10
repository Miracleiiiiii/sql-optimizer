# 技术方案: Spark AI Optimizer

> 实施拆解、工程目录、排期和验收路径见 [Implementation-Plan-Spark-AI-Optimizer.md](D:\Project\chrome-scripts\Implementation-Plan-Spark-AI-Optimizer.md)。

## 1. 方案目标

建设一个面向 Spark on YARN 离线任务的执行分析与参数推荐系统。系统通过采集 Spark History Server、YARN ResourceManager、Spark Event Log 和 Hive 元数据，将任务执行过程转化为结构化指标，再通过规则引擎和 AI 大模型生成诊断报告与参数优化建议。

MVP 目标是完成单个 applicationId 的历史任务诊断，不自动修改生产参数。

## 1.1 已确认环境

| 项目 | 已确认信息 |
| --- | --- |
| Spark 线下版本 | 3.3.1 |
| Spark 线上版本 | 3.3.2 |
| Spark 部署模式 | Spark on YARN |
| 主要分析对象 | 独立 `spark-submit` 任务，按 applicationId 分析 |
| Spark History Server | `http://hadoop102:18080`，REST API 可访问 |
| YARN ResourceManager | `http://hadoop102:8088`，REST API 可访问 |
| Event Log | 已开启 |
| Event Log 目录 | `hdfs://hadoop101:8020/spark-history` |
| Event Log 保留周期 | 7 天 |
| Hadoop 鉴权 | 当前为 simple，History Server/YARN API 可直接访问 |
| Hive 支持 | `spark.sql.catalogImplementation=hive`，Hive Metastore 通过 thrift 地址访问 |
| 调度平台 | MVP 暂不接入调度平台，由用户传入 applicationId 分析 |
| 大模型接入 | MVP 先使用外部 OpenAI API，模型参数化配置为 GPT-5.4 或 GPT-5.5 |
| SQL 入模策略 | 允许 SQL 文本进入外部模型，仍需保留脱敏和审计能力 |

## 1.2 MVP 边界修订

MVP 优先支持独立 `spark-submit` 任务的 applicationId 级诊断。Spark Thrift Server、DBeaver、JDBC 长连接等场景会出现多个 SQL 共用同一个 applicationId 的情况，这类场景需要按 `applicationId + sqlExecutionId` 分析，暂放到 P1。

所有集群地址、Hive Metastore 地址、OpenAI API 地址和模型名称必须通过配置文件或环境变量注入，代码中不硬编码线下环境地址。

## 1.3 配置项设计

MVP 建议提供以下配置项：

```yaml
spark:
  history_server_url: "http://hadoop102:18080"
  event_log_dir: "hdfs://hadoop101:8020/spark-history"
  event_log_retention_days: 7

yarn:
  resource_manager_url: "http://hadoop102:8088"

hive:
  metastore_uri: "thrift://hive-metastore-host:9083"

llm:
  provider: "openai"
  api_base_url: "https://api.openai.com/v1"
  model: "gpt-5.4"
  api_key_env: "OPENAI_API_KEY"
  timeout_seconds: 60

analysis:
  enable_sql_to_llm: true
  enable_sql_masking: true
  default_mode: "application"
```

生产环境可将 `llm.model` 切换为 `gpt-5.5`。模型名称不写死在业务代码中。

## 2. 总体架构

```text
            +----------------------+
            | Spark History Server |
            +----------+-----------+
                       |
            +----------v-----------+
            |   Collector Service  |
            +----------+-----------+
                       |
+---------+  +---------v---------+  +----------------+
|  YARN   +->+ Metrics Normalizer +<-+ Spark EventLog |
+---------+  +---------+---------+  +----------------+
                       |
              +--------v--------+
              | Diagnosis Engine |
              +--------+--------+
                       |
              +--------v--------+
              |  LLM Analyzer   |
              +--------+--------+
                       |
              +--------v--------+
              | Report Service  |
              +--------+--------+
                       |
              +--------v--------+
              | Web/API Console |
              +-----------------+
```

## 3. 技术选型建议

| 模块 | 推荐技术 | 说明 |
| --- | --- | --- |
| 后端服务 | Python FastAPI 或 Java Spring Boot | Python 更适合快速接入大模型和数据分析，Java 更适合企业已有平台集成 |
| 采集任务 | Python APScheduler / Airflow / DolphinScheduler | MVP 可先用服务内任务，后续接调度平台 |
| 数据库 | PostgreSQL / MySQL | 存任务画像、诊断结果、报告和推荐记录 |
| 大量指标存储 | ClickHouse，可选 | 批量巡检或大规模 task 指标时使用 |
| 缓存 | Redis，可选 | 缓存 application 分析状态和报告 |
| 大模型调用 | OpenAI API / 私有化大模型 API | 需要 JSON schema 输出约束 |
| 前端 | Vue / React | 展示任务报告、异常 Stage 和推荐参数 |

MVP 建议优先选择 Python FastAPI + PostgreSQL，开发速度最快。

## 4. 核心模块设计

### 4.1 Collector Service

负责从外部系统采集原始数据。

#### 4.1.1 Spark History Server Adapter

输入：

- applicationId
- attemptId，可选

采集内容：

- `/api/v1/applications/{appId}`
- `/api/v1/applications/{appId}/jobs`
- `/api/v1/applications/{appId}/stages`
- `/api/v1/applications/{appId}/stages/{stageId}/{attemptId}`
- `/api/v1/applications/{appId}/executors`
- `/api/v1/applications/{appId}/sql`
- `/api/v1/applications/{appId}/environment`

输出：

- 原始 JSON 快照。
- 标准化 application、job、stage、executor、SQL 指标。
- Spark 参数快照和 resourceProfiles 信息。

#### 4.1.2 YARN Adapter

采集内容：

- application 状态。
- queue。
- allocatedMB。
- allocatedVCores。
- startedTime。
- finishedTime。
- finalStatus。
- diagnostics。

主要用于判断：

- 队列资源限制。
- 任务排队时间。
- YARN 层失败原因。

#### 4.1.3 Event Log Parser，P1

MVP 可先不实现。P1 用于补齐 History Server API 无法稳定提供的 task 明细。

当前 Event Log 已开启，目录为 `hdfs://hadoop101:8020/spark-history`，保留周期为 7 天。由于保留周期较短，P1 若依赖 Event Log 做深度 task 级分析，需要增加定时采集任务，将关键指标和原始快照提前落库，避免历史任务过期后无法追溯。

建议优先解析：

- `SparkListenerApplicationStart`
- `SparkListenerApplicationEnd`
- `SparkListenerEnvironmentUpdate`
- `SparkListenerExecutorAdded`
- `SparkListenerTaskEnd`
- `SparkListenerStageCompleted`
- `SparkListenerSQLExecutionStart`
- `SparkListenerSQLExecutionEnd`

#### 4.1.4 Hive Metadata Adapter，P1

采集内容：

- 表大小。
- 分区数量。
- 文件数量。
- 平均文件大小。
- 表统计信息更新时间。

用于识别：

- 小文件问题。
- 分区过多或过少。
- Hive 统计过期。
- 分区裁剪缺失。

Hive Metastore 通过 thrift 地址访问，地址以配置项 `hive.metastore_uri` 注入。MVP 可以先不依赖 Hive Metastore；若 SQL API 已能返回扫描表、写入表和计划节点，则先用 History Server SQL 信息做基础 SQL 诊断。

### 4.2 Metrics Normalizer

负责把不同来源的原始 JSON 转成统一指标模型。

标准化原则：

- 所有数据量统一为 bytes。
- 所有耗时统一为 milliseconds。
- 所有比例统一为 0-1 浮点数。
- 缺失字段保留为 null，并记录 missingFields。
- 保留 rawRef，指向原始采集快照。

核心输出：

```json
{
  "application": {},
  "executors": [],
  "stages": [],
  "sqlExecutions": [],
  "yarn": {},
  "sparkConf": {},
  "missingFields": []
}
```

### 4.3 Diagnosis Engine

规则引擎负责输出可解释诊断结果。

规则结构：

```json
{
  "ruleCode": "GC_HIGH",
  "problemType": "gc_pressure",
  "severity": "high",
  "confidence": 0.85,
  "evidence": {
    "gcRatio": 0.28,
    "threshold": 0.2
  },
  "suspectedCause": "executor memory may be insufficient or executor cores are too high",
  "tuningDirection": [
    "increase executor memory",
    "reduce executor cores",
    "check shuffle spill"
  ]
}
```

#### 4.3.1 MVP 规则

| 规则 | 判断逻辑示例 | 推荐方向 |
| --- | --- | --- |
| GC_HIGH | GC ratio >= 20% | 增加 executor memory，降低 executor cores |
| SHUFFLE_SPILL_HIGH | disk spill > 10GB 或 spill/shuffleRead >= 20% | 增加分区数，增加内存 |
| TASK_SKEW_HIGH | max task duration / median task duration >= 5 | 检查倾斜 key，开启 AQE 或 salting |
| PARALLELISM_LOW | task count < total executor cores * 2 | 增加分区数或调整输入切分 |
| SHUFFLE_PARTITION_TOO_LOW | shuffle bytes / partition > 256MB | 提高 `spark.sql.shuffle.partitions` |

阈值需要配置化，不写死在代码中。

### 4.4 Recommendation Engine

负责把规则诊断映射为参数建议。

推荐输出：

```json
{
  "param": "spark.sql.shuffle.partitions",
  "current": "200",
  "suggested": "600-1000",
  "priority": "high",
  "confidence": 0.8,
  "evidence": [
    "shuffle write bytes = 910GB",
    "single partition target = 128MB-256MB"
  ],
  "risk": "too many partitions may increase scheduler overhead",
  "autoApplicable": false
}
```

#### 4.4.1 推荐计算示例

`spark.sql.shuffle.partitions` 推荐：

```text
targetPartitionSize = 128MB ~ 256MB
recommendedPartitions = shuffleWriteBytes / targetPartitionSize
recommendedPartitions = clamp(recommendedPartitions, min=existingPartitions, max=5000)
```

`spark.executor.memory` 推荐：

```text
if gcRatio >= 20% and spillBytes high:
  suggest currentMemory * 1.5 or * 2
else if gcRatio low and utilization low:
  suggest currentMemory unchanged or lower priority review
```

`spark.executor.cores` 推荐：

```text
if gcRatio high and executor cores >= 4:
  suggest reduce cores to 2-3
if task parallelism insufficient:
  do not increase cores first; increase task partitions first
```

### 4.5 LLM Analyzer

负责将结构化诊断结果生成报告。

输入要求：

- 不传完整原始日志。
- 不传业务数据明细。
- SQL 文本允许进入外部模型，但需要保留脱敏、截断和审计开关。
- 输入中明确区分 facts、rules、recommendations。

Prompt 约束：

- 只能基于输入指标给出结论。
- 不得编造未提供的集群信息。
- 参数建议必须引用 evidence。
- 输出必须符合 JSON schema。
- 对高风险建议必须给出验证步骤。

输出字段：

- summary
- mainProblems
- diagnosisDetails
- recommendations
- risks
- validationPlan

MVP 使用外部 OpenAI API，模型通过配置选择 GPT-5.4 或 GPT-5.5。调用层需要抽象为 provider 接口，避免后续从外部 API 切换到私有化模型时影响诊断主流程。

### 4.6 Report Service

负责报告存储、查询和展示。

核心能力：

- 查询任务分析状态。
- 查询报告详情。
- 查询参数推荐记录。
- 标记建议是否采纳。
- 关联后续 application，做优化效果对比。

## 5. 数据模型设计

### 5.1 application_profile

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | Spark applicationId |
| attempt_id | varchar | attemptId |
| app_name | varchar | 任务名称 |
| user_name | varchar | 用户 |
| queue_name | varchar | YARN 队列 |
| spark_version | varchar | Spark 版本 |
| start_time | timestamp | 开始时间 |
| end_time | timestamp | 结束时间 |
| duration_ms | bigint | 总耗时 |
| final_status | varchar | 最终状态 |
| spark_conf | jsonb | Spark 参数快照 |
| raw_snapshot_id | bigint | 原始快照 ID |
| created_at | timestamp | 创建时间 |

### 5.2 stage_metrics

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | applicationId |
| stage_id | int | stageId |
| attempt_id | int | attemptId |
| name | varchar | stage 名称 |
| duration_ms | bigint | 耗时 |
| task_count | int | task 数 |
| failed_task_count | int | 失败 task 数 |
| input_bytes | bigint | 输入数据量 |
| shuffle_read_bytes | bigint | shuffle read |
| shuffle_write_bytes | bigint | shuffle write |
| memory_spill_bytes | bigint | memory spill |
| disk_spill_bytes | bigint | disk spill |
| max_task_duration_ms | bigint | 最大 task 耗时 |
| median_task_duration_ms | bigint | 中位数 task 耗时 |
| p95_task_duration_ms | bigint | p95 task 耗时 |
| skew_ratio | numeric | 倾斜比例 |

### 5.3 executor_metrics

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | applicationId |
| executor_id | varchar | executorId |
| host_port | varchar | executor 地址 |
| total_cores | int | cores |
| max_memory_bytes | bigint | 最大内存 |
| total_duration_ms | bigint | executor runtime |
| total_gc_time_ms | bigint | GC 时间 |
| gc_ratio | numeric | GC 占比 |
| input_bytes | bigint | input bytes |
| shuffle_read_bytes | bigint | shuffle read |
| shuffle_write_bytes | bigint | shuffle write |
| memory_spill_bytes | bigint | memory spill |
| disk_spill_bytes | bigint | disk spill |
| failed_tasks | int | 失败 task 数 |

### 5.4 diagnosis_result

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | applicationId |
| rule_code | varchar | 规则编码 |
| problem_type | varchar | 问题类型 |
| severity | varchar | 严重程度 |
| confidence | numeric | 置信度 |
| evidence | jsonb | 证据指标 |
| suspected_cause | text | 可能原因 |
| tuning_direction | jsonb | 调优方向 |
| created_at | timestamp | 创建时间 |

### 5.5 tuning_recommendation

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | applicationId |
| param_name | varchar | 参数名 |
| current_value | varchar | 当前值 |
| suggested_value | varchar | 推荐值或范围 |
| priority | varchar | 优先级 |
| confidence | numeric | 置信度 |
| evidence | jsonb | 证据 |
| risk | text | 风险 |
| validation | text | 验证方式 |
| auto_applicable | boolean | 是否自动应用 |
| accepted | boolean | 是否采纳 |
| created_at | timestamp | 创建时间 |

### 5.6 ai_report

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | bigint | 主键 |
| application_id | varchar | applicationId |
| model_name | varchar | 模型名称 |
| prompt_version | varchar | prompt 版本 |
| input_digest | varchar | 输入摘要 |
| report_json | jsonb | 报告 JSON |
| status | varchar | 成功或失败 |
| error_message | text | 错误信息 |
| created_at | timestamp | 创建时间 |

## 6. API 设计

### 6.1 提交分析

```http
POST /api/v1/analysis
Content-Type: application/json

{
  "applicationId": "application_1710000000000_12345",
  "forceRefresh": false
}
```

响应：

```json
{
  "analysisId": "ana_001",
  "applicationId": "application_1710000000000_12345",
  "status": "running"
}
```

### 6.2 查询分析状态

```http
GET /api/v1/analysis/{analysisId}
```

响应：

```json
{
  "analysisId": "ana_001",
  "status": "success",
  "applicationId": "application_1710000000000_12345",
  "reportId": "rep_001"
}
```

### 6.3 查询报告

```http
GET /api/v1/reports/{reportId}
```

响应：

```json
{
  "application": {},
  "summary": {},
  "problems": [],
  "recommendations": [],
  "validationPlan": {}
}
```

### 6.4 标记采纳

```http
POST /api/v1/recommendations/{id}/feedback
Content-Type: application/json

{
  "accepted": true,
  "comment": "下次调度手工调整参数验证"
}
```

## 7. 处理流程

```text
1. 用户提交 applicationId
2. 检查是否已有缓存报告
3. 调用 History Server Adapter 采集数据
4. 调用 YARN Adapter 补充资源与状态
5. 指标标准化并入库
6. 规则引擎生成 diagnosis_result
7. Recommendation Engine 生成参数建议
8. LLM Analyzer 生成报告
9. Report Service 存储结果
10. 用户查看报告并反馈是否采纳
```

## 8. LLM 输入输出设计

### 8.1 输入示例

```json
{
  "facts": {
    "applicationId": "application_xxx",
    "appName": "daily_user_profile",
    "durationSeconds": 4200,
    "sparkConf": {
      "spark.executor.memory": "4g",
      "spark.executor.cores": "4",
      "spark.executor.instances": "20",
      "spark.sql.shuffle.partitions": "200"
    },
    "metrics": {
      "shuffleReadGB": 860,
      "shuffleWriteGB": 910,
      "gcRatio": 0.28,
      "diskSpillGB": 320,
      "taskSkewRatio": 8.5
    }
  },
  "rules": [
    {
      "ruleCode": "GC_HIGH",
      "severity": "high",
      "evidence": {
        "gcRatio": 0.28,
        "threshold": 0.2
      }
    }
  ],
  "recommendations": [
    {
      "param": "spark.sql.shuffle.partitions",
      "current": "200",
      "suggested": "600-1000",
      "evidence": ["shuffleWriteGB=910"]
    }
  ]
}
```

### 8.2 输出 Schema

```json
{
  "summary": "string",
  "mainProblems": [
    {
      "type": "string",
      "severity": "low|medium|high",
      "evidence": "string",
      "explanation": "string"
    }
  ],
  "recommendations": [
    {
      "param": "string",
      "current": "string",
      "suggested": "string",
      "priority": "low|medium|high",
      "reason": "string",
      "risk": "string",
      "validation": "string"
    }
  ],
  "validationPlan": "string"
}
```

## 9. 部署方案

### 9.1 MVP 部署

```text
spark-ai-optimizer-api
  - FastAPI service
  - Collector adapters
  - Diagnosis engine
  - LLM analyzer

postgresql
  - profile
  - metrics
  - diagnosis
  - reports
```

### 9.2 网络访问要求

服务需要访问：

- Spark History Server，通过 `spark.history_server_url` 配置。
- YARN ResourceManager，通过 `yarn.resource_manager_url` 配置。
- 数据库。
- OpenAI API，通过 `llm.api_base_url` 和 `llm.model` 配置。
- HDFS Event Log 路径，P1 使用，通过 `spark.event_log_dir` 配置。
- Hive Metastore thrift 地址，P1 使用，通过 `hive.metastore_uri` 配置。

如大模型为外部 API，需要通过企业网关或代理，并记录审计日志。

## 10. 安全设计

- SQL 文本允许进入外部大模型，但默认保留脱敏开关。
- 大模型输入不包含业务数据样本。
- 所有外部 API token 使用密钥管理，不落库明文。
- 用户权限需要和现有平台账号打通。
- 报告查询需要校验用户是否有任务访问权限。
- 大模型请求和响应保留摘要，敏感字段不进入普通日志。

## 11. 测试方案

### 11.1 单元测试

- 指标标准化函数。
- 规则判断逻辑。
- 参数推荐计算。
- LLM 输出 JSON schema 校验。

### 11.2 集成测试

- 使用真实 applicationId 拉取 History Server 数据。
- 使用失败 applicationId 验证错误处理。
- 使用不同 Spark 版本应用验证字段兼容。

### 11.3 验收测试

- 选择 10 个历史慢任务。
- 专家人工判断主要瓶颈。
- 对比系统诊断结果。
- 选 3 个低风险建议灰度验证收益。

## 12. 里程碑拆解

### P0: 技术验证，1-2 周

- 打通 Spark History Server API。
- 打通 YARN ResourceManager API。
- 生成统一指标 JSON。
- 完成 2-3 条规则验证。

### P1: MVP，3-5 周

- 完成数据入库。
- 完成 5 条 MVP 规则。
- 完成 4 个核心参数推荐。
- 完成 AI 报告生成。
- 完成报告查询 API。

### P2: 批量巡检，3-4 周

- 支持按日期扫描任务。
- 支持异常任务榜单。
- 支持巡检日报。

### P3: 优化闭环，4-6 周

- 支持推荐采纳记录。
- 支持优化前后对比。
- 支持同任务历史基线。

## 13. 待确认问题

- 线上 Spark History Server、YARN ResourceManager、Hive Metastore 地址的实际配置值。
- OpenAI API 的网络访问方式、代理配置、密钥管理方式和调用额度。
- SQL 入模虽然允许，但仍需确认是否需要表名、字段名、库名脱敏规则。
- Event Log 7 天保留周期是否满足排查要求，是否需要额外归档。
