from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.models import DiagnosisResult, NormalizedMetrics, Recommendation


def build_llm_payload(
    metrics: NormalizedMetrics,
    diagnoses: list[DiagnosisResult],
    recommendations: list[Recommendation],
    include_sql: bool = True,
) -> dict[str, Any]:
    primary_sql = [item for item in metrics.sql_executions if item.get("isPrimaryExecution")]
    ignored_sql = [item for item in metrics.sql_executions if item.get("isCommandResult")]
    sql_summary = [
        {
            "executionId": item.get("executionId"),
            "status": item.get("status"),
            "description": item.get("description") if include_sql else None,
            "durationMs": item.get("durationMs"),
            "scanTables": item.get("scanTables"),
            "joinTypes": item.get("joinTypes"),
            "successJobIds": item.get("successJobIds"),
            "failedJobIds": item.get("failedJobIds"),
            "isPrimaryExecution": item.get("isPrimaryExecution"),
        }
        for item in primary_sql
    ]
    return {
        "facts": {
            "application": metrics.application,
            "yarn": {
                "queue": metrics.yarn.get("queue"),
                "state": metrics.yarn.get("state"),
                "finalStatus": metrics.yarn.get("finalStatus"),
                "diagnostics": metrics.yarn.get("diagnostics"),
            },
            "sparkConf": {
                key: metrics.spark_conf.get(key)
                for key in (
                    "spark.executor.memory",
                    "spark.executor.cores",
                    "spark.executor.instances",
                    "spark.sql.shuffle.partitions",
                    "spark.dynamicAllocation.enabled",
                )
                if key in metrics.spark_conf
            },
            "metricsSummary": {
                "stageCount": len(metrics.stages),
                "executorCount": len(metrics.executors),
                "sqlExecutionCount": len(metrics.sql_executions),
                "primarySqlExecutionCount": len(primary_sql),
                "ignoredCommandResultCount": len(ignored_sql),
            },
        },
        "diagnosisRules": [asdict(item) for item in diagnoses],
        "recommendations": [asdict(item) for item in recommendations],
        "sqlSummary": sql_summary,
        "ignoredSqlExecutions": [
            {
                "executionId": item.get("executionId"),
                "reason": "CommandResult 或结果包装记录，不作为真实重复 SQL 执行分析",
                "durationMs": item.get("durationMs"),
            }
            for item in ignored_sql
        ],
        "missingFields": metrics.missing_fields,
    }
