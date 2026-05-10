from __future__ import annotations

import math

from app.core.config import RuleConfig
from app.models import DiagnosisResult, NormalizedMetrics, Recommendation


class RecommendationEngine:
    def __init__(self, config: RuleConfig) -> None:
        self.config = config

    def recommend(self, metrics: NormalizedMetrics, diagnoses: list[DiagnosisResult]) -> list[Recommendation]:
        recommendations: list[Recommendation] = []
        rule_codes = {item.rule_code for item in diagnoses}
        if "SHUFFLE_SPILL_HIGH" in rule_codes or "SHUFFLE_PARTITION_TOO_LOW" in rule_codes or "PARALLELISM_LOW" in rule_codes:
            maybe = self._shuffle_partitions(metrics)
            if maybe:
                recommendations.append(maybe)
        if "GC_HIGH" in rule_codes or "SHUFFLE_SPILL_HIGH" in rule_codes:
            maybe = self._executor_memory(metrics)
            if maybe:
                recommendations.append(maybe)
        if "GC_HIGH" in rule_codes:
            maybe = self._executor_cores(metrics)
            if maybe:
                recommendations.append(maybe)
        maybe = self._executor_instances(metrics)
        if maybe:
            recommendations.append(maybe)
        return recommendations

    def _shuffle_partitions(self, metrics: NormalizedMetrics) -> Recommendation | None:
        shuffle_bytes = sum(int(stage.get("shuffleWriteBytes") or 0) for stage in metrics.stages)
        current = _parse_int(metrics.spark_conf.get("spark.sql.shuffle.partitions")) or 200
        if shuffle_bytes <= 0:
            return None
        min_target = self.config.target_shuffle_partition_mb_min * 1024 * 1024
        max_target = self.config.target_shuffle_partition_mb_max * 1024 * 1024
        high = min(math.ceil(shuffle_bytes / min_target), self.config.max_shuffle_partitions)
        low = min(math.ceil(shuffle_bytes / max_target), self.config.max_shuffle_partitions)
        low = max(low, current)
        high = max(high, low)
        suggested = str(low) if low == high else f"{low}-{high}"
        return Recommendation(
            param="spark.sql.shuffle.partitions",
            current=str(current),
            suggested=suggested,
            priority="high",
            confidence=0.82,
            evidence=[
                f"shuffleWriteBytes={shuffle_bytes}",
                f"targetPartitionSize={self.config.target_shuffle_partition_mb_min}-{self.config.target_shuffle_partition_mb_max}MB",
            ],
            risk="too many partitions may increase scheduler overhead",
            validation="compare total duration, p95 task duration, shuffle spill, and scheduler overhead after one gray run",
        )

    def _executor_memory(self, metrics: NormalizedMetrics) -> Recommendation | None:
        current = metrics.spark_conf.get("spark.executor.memory")
        if not current:
            return None
        current_mb = _memory_to_mb(current)
        if not current_mb:
            return None
        suggested_mb = int(math.ceil(current_mb * 1.5 / 512) * 512)
        return Recommendation(
            param="spark.executor.memory",
            current=current,
            suggested=f"{suggested_mb}m",
            priority="medium",
            confidence=0.72,
            evidence=["GC or spill related diagnosis triggered", f"currentExecutorMemory={current}"],
            risk="larger executor memory increases YARN queue resource usage",
            validation="compare GC ratio, disk spill, container allocation time, and total duration",
        )

    def _executor_cores(self, metrics: NormalizedMetrics) -> Recommendation | None:
        current = _parse_int(metrics.spark_conf.get("spark.executor.cores"))
        if current is None or current <= 2:
            return None
        suggested = max(2, current - 1)
        return Recommendation(
            param="spark.executor.cores",
            current=str(current),
            suggested=str(suggested),
            priority="medium",
            confidence=0.65,
            evidence=["GC pressure triggered and executor cores are greater than 2"],
            risk="lower cores may require more executors to keep total parallelism",
            validation="compare executor GC ratio, task throughput, and queue resource usage",
        )

    def _executor_instances(self, metrics: NormalizedMetrics) -> Recommendation | None:
        current = _parse_int(metrics.spark_conf.get("spark.executor.instances")) or len(metrics.executors)
        total_tasks = sum(int(stage.get("taskCount") or 0) for stage in metrics.stages)
        if not current or not total_tasks:
            return None
        total_cores = sum(int(executor.get("totalCores") or 0) for executor in metrics.executors)
        if total_cores and total_tasks >= total_cores * 2:
            return None
        return Recommendation(
            param="spark.executor.instances",
            current=str(current),
            suggested=f"{current}-{max(current, current * 2)}",
            priority="low",
            confidence=0.55,
            evidence=[f"totalTasks={total_tasks}", f"currentExecutors={current}"],
            risk="more executors may increase queue pressure; prefer fixing partition count first if task count is low",
            validation="compare pending time, executor utilization, and total duration",
        )


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _memory_to_mb(value: str) -> int | None:
    raw = value.strip().lower()
    try:
        if raw.endswith("g"):
            return int(float(raw[:-1]) * 1024)
        if raw.endswith("gb"):
            return int(float(raw[:-2]) * 1024)
        if raw.endswith("m"):
            return int(float(raw[:-1]))
        if raw.endswith("mb"):
            return int(float(raw[:-2]))
        return int(raw)
    except ValueError:
        return None

