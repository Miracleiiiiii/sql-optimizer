# 实施方案: Spark AI Optimizer

## 1. 实施目标

基于现有 PRD 和技术方案，第一阶段落地一个可运行的 Spark 任务诊断与参数推荐 MVP。MVP 以用户传入 applicationId 为入口，采集 Spark History Server 和 YARN ResourceManager 数据，完成指标标准化、规则诊断、参数推荐和 OpenAI 报告生成。

MVP 不接入调度平台，不自动修改生产参数，不做 Spark Thrift Server / DBeaver / JDBC 长连接下的单 SQL 粒度诊断。

## 2. 已确认实施条件

| 项目 | 结果 |
| --- | --- |
| Spark 版本 | 线下 3.3.1，线上 3.3.2 |
| 部署模式 | Spark on YARN |
| 主分析对象 | 独立 spark-submit 任务 |
| 分析粒度 | applicationId |
| History Server | 参数化配置，线下验证地址 `http://hadoop102:18080` |
| YARN ResourceManager | 参数化配置，线下验证地址 `http://hadoop102:8088` |
| Event Log | 已开启，目录 `hdfs://hadoop101:8020/spark-history` |
| Event Log 保留周期 | 7 天 |
| Hive Metastore | thrift 地址参数化配置 |
| 大模型 | 外部 OpenAI API，模型配置为 GPT-5.4 或 GPT-5.5 |
| SQL 入模 | 允许 SQL 文本进入外部模型，保留脱敏和审计能力 |

## 3. 实施原则

- 先跑通 applicationId 单任务诊断闭环，再扩展批量巡检。
- 先使用 Spark History Server REST API 和 YARN API，Event Log 解析放到 P1。
- 规则引擎是诊断主干，大模型只做解释、组织报告和建议表达。
- 所有外部地址、模型名称、密钥环境变量、阈值均配置化。
- 所有推荐只输出建议，不自动应用生产参数。
- 原始采集数据保留快照，便于排查和回放。

## 4. MVP 功能清单

| 模块 | 功能 | 优先级 |
| --- | --- | --- |
| 配置管理 | 加载 History Server、YARN、Hive、OpenAI、规则阈值配置 | P0 |
| Analysis API | 提交 applicationId 分析任务 | P0 |
| History Collector | 采集 application、jobs、stages、executors、environment、sql | P0 |
| YARN Collector | 采集 application 状态、队列、资源、diagnostics | P0 |
| Raw Snapshot | 保存原始 JSON 快照 | P0 |
| Metrics Normalizer | 标准化 application、stage、executor、sql、sparkConf 指标 | P0 |
| Diagnosis Engine | 实现 GC、spill、倾斜、并行度、分区过少诊断规则 | P0 |
| Recommendation Engine | 输出 4 类核心 Spark 参数建议 | P0 |
| LLM Analyzer | 调用 OpenAI API 生成结构化报告 | P0 |
| Report API | 查询分析状态和报告详情 | P0 |
| Feedback API | 标记建议是否采纳 | P1 |
| Hive Adapter | 通过 thrift 读取表元数据 | P1 |
| Event Log Parser | 解析 Event Log 补充 task 明细 | P1 |

## 5. 推荐工程结构

```text
spark-ai-optimizer/
  app/
    main.py
    api/
      analysis.py
      reports.py
      recommendations.py
    core/
      config.py
      logging.py
      errors.py
    collectors/
      spark_history.py
      yarn.py
      hive_metastore.py
      event_log.py
    normalizers/
      application.py
      stage.py
      executor.py
      sql.py
    diagnosis/
      engine.py
      rules.py
      thresholds.py
    recommendation/
      engine.py
      calculators.py
    llm/
      provider.py
      openai_provider.py
      prompts.py
      schemas.py
    reports/
      builder.py
      serializer.py
    storage/
      models.py
      repositories.py
      migrations/
    tests/
      unit/
      integration/
  config/
    application.yaml
    rules.yaml
  scripts/
    smoke_test_history_api.py
    smoke_test_yarn_api.py
  README.md
```

## 6. 配置设计

```yaml
spark:
  history_server_url: "http://hadoop102:18080"
  event_log_dir: "hdfs://hadoop101:8020/spark-history"
  event_log_retention_days: 7

yarn:
  resource_manager_url: "http://hadoop102:8088"

hive:
  metastore_uri: "thrift://hive-metastore-host:9083"
  enabled: false

llm:
  provider: "openai"
  api_base_url: "https://api.openai.com/v1"
  model: "gpt-5.4"
  api_key_env: "OPENAI_API_KEY"
  timeout_seconds: 60

analysis:
  default_mode: "application"
  enable_sql_to_llm: true
  enable_sql_masking: true
  cache_existing_report: true

rules:
  gc_high_ratio: 0.2
  task_skew_ratio: 5
  spill_bytes_high: 10737418240
  spill_to_shuffle_ratio: 0.2
  target_shuffle_partition_mb_min: 128
  target_shuffle_partition_mb_max: 256
  max_shuffle_partitions: 5000
```

## 7. 核心流程

```text
用户提交 applicationId
        |
        v
创建 analysis 记录
        |
        v
采集 Spark History Server 数据
        |
        v
采集 YARN ResourceManager 数据
        |
        v
保存 raw snapshot
        |
        v
标准化指标
        |
        v
执行诊断规则
        |
        v
生成参数推荐
        |
        v
调用 OpenAI 生成报告
        |
        v
保存报告并返回结果
```

## 8. API 实施设计

### 8.1 提交分析

```http
POST /api/v1/analysis
Content-Type: application/json

{
  "applicationId": "application_1778340258140_0024",
  "forceRefresh": false
}
```

返回：

```json
{
  "analysisId": "ana_202605100001",
  "applicationId": "application_1778340258140_0024",
  "status": "running"
}
```

### 8.2 查询分析状态

```http
GET /api/v1/analysis/{analysisId}
```

返回：

```json
{
  "analysisId": "ana_202605100001",
  "applicationId": "application_1778340258140_0024",
  "status": "success",
  "reportId": "rep_202605100001",
  "errorMessage": null
}
```

### 8.3 查询报告

```http
GET /api/v1/reports/{reportId}
```

返回：

```json
{
  "application": {},
  "metricsSummary": {},
  "diagnosis": [],
  "recommendations": [],
  "aiReport": {},
  "rawRefs": []
}
```

### 8.4 标记采纳

```http
POST /api/v1/recommendations/{recommendationId}/feedback
Content-Type: application/json

{
  "accepted": true,
  "comment": "下次手工调整参数验证"
}
```

## 9. 数据库实施顺序

MVP 最小表集合：

1. `analysis_task`
2. `raw_snapshot`
3. `application_profile`
4. `stage_metrics`
5. `executor_metrics`
6. `sql_metrics`
7. `yarn_metrics`
8. `diagnosis_result`
9. `tuning_recommendation`
10. `ai_report`

建议第一版 PostgreSQL 使用 JSONB 保存半结构化字段。后续批量巡检或 task 明细规模变大时，再引入 ClickHouse。

## 10. 规则实施顺序

### 10.1 第一批规则

| ruleCode | 问题 | 实施依据 |
| --- | --- | --- |
| GC_HIGH | GC 压力高 | `jvmGcTime / executorRunTime` 或 executor `totalGCTime / totalDuration` |
| SHUFFLE_SPILL_HIGH | shuffle spill 严重 | `memoryBytesSpilled`、`diskBytesSpilled` |
| TASK_SKEW_HIGH | task 倾斜 | stage task duration 的 max / median |
| PARALLELISM_LOW | 并行度不足 | task 数与 executor cores 对比 |
| SHUFFLE_PARTITION_TOO_LOW | shuffle 分区过少 | shuffle bytes / partitions |

### 10.2 第一版降级策略

如果 History Server API 缺少 task 明细，`TASK_SKEW_HIGH` 规则先降级：

- 使用 stage 层指标判断长尾风险。
- 在报告中标明 task 明细缺失，建议 P1 通过 Event Log 补齐。

## 11. 推荐引擎实施顺序

### 11.1 核心参数

| 参数 | MVP 是否实现 | 说明 |
| --- | --- | --- |
| `spark.sql.shuffle.partitions` | 是 | 基于 shuffle bytes 和目标分区大小计算 |
| `spark.executor.memory` | 是 | 基于 GC、spill、OOM 风险推荐 |
| `spark.executor.cores` | 是 | 基于 GC 和并行度风险推荐 |
| `spark.executor.instances` | 是 | 基于 task 总量、当前 executor 数和 YARN 资源建议 |

### 11.2 推荐输出约束

- 推荐值优先输出范围，而不是绝对值。
- 必须包含 evidence。
- 必须包含 risk。
- 必须包含 validation。
- `autoApplicable` 在 MVP 固定为 `false`。

## 12. OpenAI 接入实施

### 12.1 Provider 抽象

```text
LLMProvider
  - generate_report(input_json) -> report_json

OpenAIProvider implements LLMProvider
```

### 12.2 输入内容

```json
{
  "facts": {},
  "diagnosisRules": [],
  "recommendations": [],
  "sqlSummary": {},
  "missingFields": []
}
```

### 12.3 输出校验

必须校验：

- 是否为合法 JSON。
- 是否包含 `summary`、`mainProblems`、`recommendations`、`validationPlan`。
- 推荐参数是否来自 Recommendation Engine。
- 报告中是否出现输入中不存在的关键事实。

如果校验失败：

- 保存失败原因。
- 返回规则诊断报告。
- 标记 `ai_report.status = failed`。

## 13. 测试实施方案

### 13.1 P0 冒烟测试

准备 3 个 applicationId：

- 成功的 Spark SQL / ETL 任务。
- 失败或取消的任务。
- 有 shuffle 或 stage 的任务。

验证：

- History Server API 可采集。
- YARN API 可采集。
- environment 能解析 sparkConf 和 resourceProfiles。
- SQL API 有数据时能解析 planDescription 和 nodes。

### 13.2 单元测试

- URL 拼接和异常处理。
- 指标单位转换。
- Spark conf 解析。
- resourceProfiles 解析。
- GC ratio 计算。
- spill ratio 计算。
- 推荐范围计算。
- LLM JSON schema 校验。

### 13.3 集成测试

- 传入真实 applicationId，生成完整报告。
- 传入不存在的 applicationId，返回可读错误。
- OpenAI API 不可用时，返回规则诊断报告。
- History Server 部分接口失败时，生成降级报告。

## 14. 里程碑与排期

### 阶段 0: 工程初始化，2 天

- 初始化 FastAPI 工程。
- 增加配置加载。
- 增加数据库连接。
- 增加基础日志和错误模型。

交付物：

- 可启动的 API 服务。
- `/health` 健康检查接口。
- 配置文件模板。

### 阶段 1: 数据采集，4 天

- 实现 Spark History Server Collector。
- 实现 YARN Collector。
- 保存 raw snapshot。
- 支持 applicationId 采集。

交付物：

- 采集接口。
- 原始 JSON 快照入库。
- 采集失败错误记录。

### 阶段 2: 指标标准化，4 天

- 标准化 application 指标。
- 标准化 stage 指标。
- 标准化 executor 指标。
- 标准化 SQL 指标。
- 解析 sparkConf 和 resourceProfiles。

交付物：

- application_profile、stage_metrics、executor_metrics、sql_metrics 入库。
- 指标 JSON 输出。

### 阶段 3: 规则诊断与推荐，5 天

- 实现第一批 MVP 规则。
- 实现推荐引擎。
- 实现规则阈值配置化。
- 生成诊断结果和参数推荐记录。

交付物：

- diagnosis_result。
- tuning_recommendation。
- 不依赖大模型的基础诊断报告。

### 阶段 4: OpenAI 报告，4 天

- 实现 OpenAIProvider。
- 实现 prompt 模板。
- 实现输出 JSON schema 校验。
- 实现 AI 失败降级。

交付物：

- ai_report。
- 完整自然语言诊断报告。

### 阶段 5: API 与验收，5 天

- 完成分析提交 API。
- 完成状态查询 API。
- 完成报告查询 API。
- 完成推荐采纳 API。
- 使用真实 applicationId 做验收。

交付物：

- MVP 可演示版本。
- 接口文档。
- 验收报告。

总周期建议：4 周左右。

## 15. 人员分工建议

| 角色 | 人数 | 主要职责 |
| --- | --- | --- |
| 后端工程师 | 1-2 | API、采集、入库、报告服务 |
| 数据平台工程师 | 1 | Spark/YARN/Hive 指标解释、规则阈值校准 |
| 算法/AI 工程师 | 1 | LLM prompt、输出 schema、报告质量评估 |
| 前端工程师 | 0-1 | 报告页面，可后置 |
| 测试/平台工程师 | 1 | 真实任务样本、集成测试、验收 |

如果资源有限，MVP 可以先不做前端，使用 API + Markdown/JSON 报告完成验证。

## 16. 验收标准

### 16.1 技术验收

- 可以通过配置切换 History Server、YARN、Hive、OpenAI 地址。
- 输入有效 applicationId 后能生成报告。
- 采集失败、模型失败、字段缺失时能降级处理。
- 原始快照、标准化指标、诊断结果、AI 报告均可追溯。

### 16.2 业务验收

- 选取 10 个历史任务做专家复核。
- 主要瓶颈判断一致率达到 70% 以上。
- 至少 3 个任务能给出可执行的低风险调参建议。
- 报告能明确说明证据、风险和验证方式。

## 17. 上线策略

MVP 建议采用内网灰度：

1. 仅开放给数据平台工程师和少量数据开发。
2. 只读访问 Spark/YARN/Hive，不写生产配置。
3. OpenAI 调用加额度限制和审计。
4. 收集 10-20 个任务反馈后再扩大范围。
5. 所有参数建议先人工验证，不自动应用。

## 18. 后续演进

### P1

- Event Log 解析。
- Hive Metastore 表画像。
- `applicationId + sqlExecutionId` 粒度 SQL 诊断。
- 推荐采纳与效果对比。

### P2

- 批量巡检。
- 慢任务榜单。
- 资源浪费榜单。
- 每日巡检报告。

### P3

- 对接调度平台。
- 任务名称与 applicationId 自动映射。
- 生成 spark-submit 参数片段。
- 半自动灰度调参。

