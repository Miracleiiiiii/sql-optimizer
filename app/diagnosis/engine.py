from __future__ import annotations

import statistics

from app.core.config import RuleConfig
from app.models import DiagnosisResult, NormalizedMetrics


class DiagnosisEngine:
    def __init__(self, config: RuleConfig) -> None:
        self.config = config

    def diagnose(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        results: list[DiagnosisResult] = []
        results.extend(self._gc_high(metrics))
        results.extend(self._shuffle_spill_high(metrics))
        results.extend(self._task_skew_high(metrics))
        results.extend(self._parallelism_low(metrics))
        results.extend(self._shuffle_partition_too_low(metrics))
        results.extend(self._failed_or_cancelled(metrics))
        return results

    def _gc_high(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        max_stage_gc = max((float(stage.get("gcRatio") or 0) for stage in metrics.stages), default=0)
        max_executor_gc = max((float(executor.get("gcRatio") or 0) for executor in metrics.executors), default=0)
        gc_ratio = max(max_stage_gc, max_executor_gc)
        if gc_ratio < self.config.gc_high_ratio:
            return []
        return [
            DiagnosisResult(
                rule_code="GC_HIGH",
                problem_type="gc_pressure",
                severity="high" if gc_ratio >= 0.35 else "medium",
                confidence=0.85,
                evidence={"gcRatio": round(gc_ratio, 4), "threshold": self.config.gc_high_ratio},
                suspected_cause="executor memory may be insufficient, executor cores may be too high, or shuffle pressure is high",
                tuning_direction=["review spark.executor.memory", "review spark.executor.cores", "check shuffle spill"],
            )
        ]

    def _task_skew_high(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        results: list[DiagnosisResult] = []
        tasks_by_stage: dict[tuple[int | None, int | None], list[dict]] = {}
        for task in metrics.tasks:
            tasks_by_stage.setdefault((task.get("stageId"), task.get("attemptId")), []).append(task)

        for (stage_id, attempt_id), tasks in tasks_by_stage.items():
            if len(tasks) < 10:
                continue
            duration_result = _max_to_median(tasks, "durationMs")
            shuffle_result = _max_to_median(tasks, "shuffleReadBytes")
            memory_result = _max_to_median(tasks, "peakExecutionMemory")
            if not duration_result and not shuffle_result and not memory_result:
                continue

            primary = duration_result or shuffle_result or memory_result
            assert primary is not None
            if primary["ratio"] < self.config.task_skew_ratio:
                continue
            skewed_task = primary["task"]
            results.append(
                DiagnosisResult(
                    rule_code="TASK_SKEW_HIGH",
                    problem_type="data_skew",
                    severity="high" if primary["ratio"] >= self.config.task_skew_ratio * 2 else "medium",
                    confidence=0.86,
                    evidence={
                        "stageId": stage_id,
                        "attemptId": attempt_id,
                        "taskCount": len(tasks),
                        "skewedTaskId": skewed_task.get("taskId"),
                        "durationSkewRatio": _ratio_value(duration_result),
                        "maxDurationMs": _max_value(duration_result),
                        "medianDurationMs": _median_value(duration_result),
                        "shuffleReadSkewRatio": _ratio_value(shuffle_result),
                        "maxShuffleReadBytes": _max_value(shuffle_result),
                        "medianShuffleReadBytes": _median_value(shuffle_result),
                        "peakMemorySkewRatio": _ratio_value(memory_result),
                        "maxPeakExecutionMemory": _max_value(memory_result),
                        "medianPeakExecutionMemory": _median_value(memory_result),
                    },
                    suspected_cause="one or more tasks in the same stage consume disproportionately more time, shuffle data, or execution memory",
                    tuning_direction=[
                        "inspect join/group by key distribution",
                        "consider salting hot keys",
                        "enable Spark AQE skew join handling when applicable",
                    ],
                )
            )
        return results

    def _shuffle_spill_high(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        memory_spill = sum(int(stage.get("memoryBytesSpilled") or 0) for stage in metrics.stages)
        disk_spill = sum(int(stage.get("diskBytesSpilled") or 0) for stage in metrics.stages)
        shuffle_read = sum(int(stage.get("shuffleReadBytes") or 0) for stage in metrics.stages)
        shuffle_write = sum(int(stage.get("shuffleWriteBytes") or 0) for stage in metrics.stages)
        spill = memory_spill + disk_spill
        shuffle = max(shuffle_read + shuffle_write, 1)
        spill_ratio = spill / shuffle
        if spill < self.config.spill_bytes_high and spill_ratio < self.config.spill_to_shuffle_ratio:
            return []
        return [
            DiagnosisResult(
                rule_code="SHUFFLE_SPILL_HIGH",
                problem_type="shuffle_spill",
                severity="high" if disk_spill >= self.config.spill_bytes_high else "medium",
                confidence=0.8,
                evidence={
                    "memoryBytesSpilled": memory_spill,
                    "diskBytesSpilled": disk_spill,
                    "spillToShuffleRatio": round(spill_ratio, 4),
                },
                suspected_cause="single shuffle partition may be too large or executor execution memory may be insufficient",
                tuning_direction=["increase spark.sql.shuffle.partitions", "review spark.executor.memory"],
            )
        ]

    def _parallelism_low(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        total_tasks = sum(int(stage.get("taskCount") or 0) for stage in metrics.stages)
        total_cores = sum(int(executor.get("totalCores") or 0) for executor in metrics.executors)
        if not total_tasks or not total_cores:
            return []
        threshold = total_cores * 2
        if total_tasks >= threshold:
            return []
        return [
            DiagnosisResult(
                rule_code="PARALLELISM_LOW",
                problem_type="parallelism_low",
                severity="medium",
                confidence=0.75,
                evidence={"totalTasks": total_tasks, "executorCores": total_cores, "expectedMinTasks": threshold},
                suspected_cause="task count is too small compared with available executor cores",
                tuning_direction=["increase input partitions", "review spark.sql.shuffle.partitions"],
            )
        ]

    def _shuffle_partition_too_low(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        shuffle_bytes = sum(int(stage.get("shuffleWriteBytes") or 0) for stage in metrics.stages)
        current = _parse_int(metrics.spark_conf.get("spark.sql.shuffle.partitions"))
        if not shuffle_bytes or not current:
            return []
        per_partition = shuffle_bytes / current
        threshold = self.config.target_shuffle_partition_mb_max * 1024 * 1024
        if per_partition <= threshold:
            return []
        return [
            DiagnosisResult(
                rule_code="SHUFFLE_PARTITION_TOO_LOW",
                problem_type="shuffle_partition_too_low",
                severity="medium",
                confidence=0.78,
                evidence={
                    "shuffleWriteBytes": shuffle_bytes,
                    "currentPartitions": current,
                    "bytesPerPartition": int(per_partition),
                    "targetMaxBytes": threshold,
                },
                suspected_cause="shuffle partition count is low for the written shuffle volume",
                tuning_direction=["increase spark.sql.shuffle.partitions"],
            )
        ]

    def _failed_or_cancelled(self, metrics: NormalizedMetrics) -> list[DiagnosisResult]:
        failed_stages = [stage for stage in metrics.stages if stage.get("status") in {"FAILED", "KILLED"}]
        if not failed_stages:
            return []
        return [
            DiagnosisResult(
                rule_code="APPLICATION_FAILED_OR_CANCELLED",
                problem_type="execution_failed",
                severity="high",
                confidence=0.9,
                evidence={
                    "failedStages": len(failed_stages),
                    "reasons": [stage.get("failureReason") for stage in failed_stages if stage.get("failureReason")],
                },
                suspected_cause="application did not complete successfully; performance tuning should wait until failure cause is resolved",
                tuning_direction=["inspect YARN diagnostics", "inspect failed stage reason", "avoid parameter tuning from failed-only sample"],
            )
        ]


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except ValueError:
        return None


def _max_to_median(tasks: list[dict], field: str) -> dict | None:
    values = [(int(task.get(field) or 0), task) for task in tasks if int(task.get(field) or 0) > 0]
    if len(values) < 2:
        return None
    numbers = [value for value, _ in values]
    median = statistics.median(numbers)
    if median <= 0:
        return None
    max_value, max_task = max(values, key=lambda item: item[0])
    return {"field": field, "max": max_value, "median": median, "ratio": max_value / median, "task": max_task}


def _ratio_value(result: dict | None) -> float | None:
    return round(float(result["ratio"]), 4) if result else None


def _max_value(result: dict | None) -> int | None:
    return int(result["max"]) if result else None


def _median_value(result: dict | None) -> float | int | None:
    if not result:
        return None
    median = float(result["median"])
    return int(median) if median.is_integer() else median
