from __future__ import annotations

from typing import Any

from app.models import NormalizedMetrics


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, list) and value:
        first = value[0]
        return first if isinstance(first, dict) else {}
    return value if isinstance(value, dict) else {}


def _properties_to_dict(items: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    if not isinstance(items, list):
        return result
    for item in items:
        if isinstance(item, list) and len(item) >= 2:
            result[str(item[0])] = str(item[1])
    return result


def _resource_profile_defaults(resource_profiles: list[dict[str, Any]]) -> dict[str, str]:
    if not resource_profiles:
        return {}
    profile = resource_profiles[0]
    executor_resources = profile.get("executorResources", {})
    defaults: dict[str, str] = {}
    memory = executor_resources.get("memory", {}).get("amount")
    cores = executor_resources.get("cores", {}).get("amount")
    if memory is not None:
        defaults["spark.executor.memory"] = f"{memory}m"
    if cores is not None:
        defaults["spark.executor.cores"] = str(cores)
    return defaults


def _normalize_application(app: dict[str, Any], yarn: dict[str, Any], spark_conf: dict[str, str]) -> dict[str, Any]:
    attempts = app.get("attempts") if isinstance(app.get("attempts"), list) else []
    latest_attempt = attempts[-1] if attempts else {}
    return {
        "applicationId": app.get("id") or spark_conf.get("spark.app.id") or yarn.get("id"),
        "applicationName": app.get("name") or spark_conf.get("spark.app.name") or yarn.get("name"),
        "user": latest_attempt.get("sparkUser") or yarn.get("user"),
        "queue": yarn.get("queue"),
        "startTime": latest_attempt.get("startTime") or yarn.get("startedTime"),
        "endTime": latest_attempt.get("endTime") or yarn.get("finishedTime"),
        "duration": latest_attempt.get("duration") or yarn.get("elapsedTime"),
        "finalStatus": latest_attempt.get("completed") or yarn.get("finalStatus"),
        "sparkVersion": app.get("sparkVersion"),
    }


def _normalize_executors(executors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in executors:
        if item.get("id") == "driver":
            continue
        total_duration = int(item.get("totalDuration") or 0)
        total_gc = int(item.get("totalGCTime") or 0)
        normalized.append(
            {
                "executorId": item.get("id"),
                "hostPort": item.get("hostPort"),
                "totalCores": item.get("totalCores"),
                "maxMemoryBytes": item.get("maxMemory"),
                "totalDurationMs": total_duration,
                "totalGcTimeMs": total_gc,
                "gcRatio": total_gc / total_duration if total_duration > 0 else 0,
                "totalInputBytes": item.get("totalInputBytes") or 0,
                "totalShuffleRead": item.get("totalShuffleRead") or 0,
                "totalShuffleWrite": item.get("totalShuffleWrite") or 0,
                "failedTasks": item.get("failedTasks") or 0,
                "totalTasks": item.get("totalTasks") or 0,
            }
        )
    return normalized


def _normalize_stages(stages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in stages:
        executor_runtime = int(item.get("executorRunTime") or 0)
        gc_time = int(item.get("jvmGcTime") or item.get("peakExecutorMetrics", {}).get("TotalGCTime") or 0)
        normalized.append(
            {
                "stageId": item.get("stageId"),
                "attemptId": item.get("attemptId"),
                "name": item.get("name"),
                "status": item.get("status"),
                "durationMs": _duration_from_times(item.get("submissionTime"), item.get("completionTime")),
                "taskCount": item.get("numTasks") or 0,
                "failedTaskCount": item.get("numFailedTasks") or 0,
                "killedTaskCount": item.get("numKilledTasks") or 0,
                "executorRunTimeMs": executor_runtime,
                "jvmGcTimeMs": gc_time,
                "gcRatio": gc_time / executor_runtime if executor_runtime > 0 else 0,
                "inputBytes": item.get("inputBytes") or 0,
                "outputBytes": item.get("outputBytes") or 0,
                "shuffleReadBytes": item.get("shuffleReadBytes") or 0,
                "shuffleWriteBytes": item.get("shuffleWriteBytes") or 0,
                "memoryBytesSpilled": item.get("memoryBytesSpilled") or 0,
                "diskBytesSpilled": item.get("diskBytesSpilled") or 0,
                "failureReason": item.get("failureReason"),
                "details": item.get("details"),
            }
        )
    return normalized


def _duration_from_times(start: str | None, end: str | None) -> int | None:
    # Spark already provides executorRunTime; wall-clock stage duration parsing
    # is intentionally deferred to avoid timezone edge cases in MVP.
    return None if not (start and end) else None


def _normalize_sql(sql_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for item in sql_items:
        nodes = item.get("nodes") if isinstance(item.get("nodes"), list) else []
        plan = item.get("planDescription")
        node_names = [str(node.get("nodeName", "")) for node in nodes if isinstance(node, dict)]
        success_job_ids = item.get("successJobIds") or []
        is_command_result = bool(plan and "CommandResult" in plan) or node_names == ["CommandResult"]
        is_primary_execution = bool(success_job_ids) and not is_command_result
        normalized.append(
            {
                "executionId": item.get("id"),
                "status": item.get("status"),
                "description": item.get("description"),
                "planDescription": plan,
                "durationMs": item.get("duration"),
                "successJobIds": success_job_ids,
                "failedJobIds": item.get("failedJobIds") or [],
                "scanTables": _extract_plan_tokens(plan, "Scan hive "),
                "joinTypes": _extract_join_types(plan),
                "nodes": nodes,
                "nodeNames": node_names,
                "isCommandResult": is_command_result,
                "isPrimaryExecution": is_primary_execution,
            }
        )
    return normalized


def _extract_plan_tokens(plan: str | None, prefix: str) -> list[str]:
    if not plan:
        return []
    values: list[str] = []
    for line in plan.splitlines():
        marker = prefix
        if marker in line:
            value = line.split(marker, 1)[1].strip().split(" ", 1)[0]
            if value and value not in values:
                values.append(value)
    return values


def _extract_join_types(plan: str | None) -> list[str]:
    if not plan:
        return []
    known = ["BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin", "BroadcastNestedLoopJoin"]
    found: list[str] = []
    for join_type in known:
        if join_type in plan and join_type not in found:
            found.append(join_type)
    return found


def normalize(history_payload: dict[str, Any], yarn_payload: dict[str, Any]) -> NormalizedMetrics:
    environment = history_payload.get("environment", {})
    spark_conf = _properties_to_dict(environment.get("sparkProperties"))
    resource_profiles = environment.get("resourceProfiles") if isinstance(environment.get("resourceProfiles"), list) else []
    spark_conf = {**_resource_profile_defaults(resource_profiles), **spark_conf}

    app = _as_dict(history_payload.get("application"))
    jobs = history_payload.get("jobs") if isinstance(history_payload.get("jobs"), list) else []
    stages = history_payload.get("stages") if isinstance(history_payload.get("stages"), list) else []
    executors = history_payload.get("executors") if isinstance(history_payload.get("executors"), list) else []
    sql_items = history_payload.get("sql") if isinstance(history_payload.get("sql"), list) else []

    missing = []
    for key in ("application", "jobs", "stages", "executors", "environment"):
        if key not in history_payload:
            missing.append(f"spark_history.{key}")

    return NormalizedMetrics(
        application=_normalize_application(app, yarn_payload, spark_conf),
        yarn=yarn_payload,
        spark_conf=spark_conf,
        resource_profiles=resource_profiles,
        jobs=jobs,
        stages=_normalize_stages(stages),
        executors=_normalize_executors(executors),
        sql_executions=_normalize_sql(sql_items),
        missing_fields=missing,
    )
