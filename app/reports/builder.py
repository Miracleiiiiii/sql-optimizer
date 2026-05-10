from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.models import DiagnosisResult, NormalizedMetrics, Recommendation


def build_rule_report(
    metrics: NormalizedMetrics,
    diagnoses: list[DiagnosisResult],
    recommendations: list[Recommendation],
    ai_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "application": metrics.application,
        "metricsSummary": {
            "stageCount": len(metrics.stages),
            "executorCount": len(metrics.executors),
            "sqlExecutionCount": len(metrics.sql_executions),
            "totalShuffleReadBytes": sum(int(stage.get("shuffleReadBytes") or 0) for stage in metrics.stages),
            "totalShuffleWriteBytes": sum(int(stage.get("shuffleWriteBytes") or 0) for stage in metrics.stages),
            "totalMemorySpillBytes": sum(int(stage.get("memoryBytesSpilled") or 0) for stage in metrics.stages),
            "totalDiskSpillBytes": sum(int(stage.get("diskBytesSpilled") or 0) for stage in metrics.stages),
        },
        "diagnosis": [asdict(item) for item in diagnoses],
        "recommendations": [asdict(item) for item in recommendations],
        "aiReport": ai_report,
        "missingFields": metrics.missing_fields,
    }

