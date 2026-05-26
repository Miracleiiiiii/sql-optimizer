from __future__ import annotations

from typing import Any
from urllib.parse import quote

from app.core.config import SparkConfig
from app.core.errors import UpstreamApiError
from app.core.http import get_json


class SparkHistoryCollector:
    def __init__(self, config: SparkConfig, timeout: int = 30) -> None:
        self.config = config
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.config.history_server_url}{path}"

    def collect_application(self, application_id: str) -> dict[str, Any]:
        app_id = quote(application_id, safe="")
        stages = get_json(self._url(f"/api/v1/applications/{app_id}/stages"), self.timeout) or []
        task_list_by_stage, missing_fields = self._collect_task_lists(app_id, stages)
        payload = {
            "application": get_json(self._url(f"/api/v1/applications/{app_id}"), self.timeout),
            "jobs": get_json(self._url(f"/api/v1/applications/{app_id}/jobs"), self.timeout) or [],
            "stages": stages,
            "taskListByStage": task_list_by_stage,
            "executors": get_json(self._url(f"/api/v1/applications/{app_id}/executors"), self.timeout) or [],
            "environment": get_json(self._url(f"/api/v1/applications/{app_id}/environment"), self.timeout) or {},
            "sql": get_json(self._url(f"/api/v1/applications/{app_id}/sql"), self.timeout) or [],
            "missingFields": missing_fields,
        }
        return payload

    def _collect_task_lists(self, app_id: str, stages: Any) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
        task_list_by_stage: dict[str, list[dict[str, Any]]] = {}
        missing_fields: list[str] = []
        if not isinstance(stages, list):
            return task_list_by_stage, missing_fields
        for stage in stages:
            if not isinstance(stage, dict):
                continue
            stage_id = stage.get("stageId")
            attempt_id = stage.get("attemptId", 0)
            if stage_id is None:
                continue
            key = f"{stage_id}:{attempt_id}"
            try:
                tasks = get_json(
                    self._url(f"/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList"),
                    self.timeout,
                )
            except UpstreamApiError:
                missing_fields.append(f"spark_history.stages.{stage_id}.{attempt_id}.taskList")
                continue
            if isinstance(tasks, list):
                task_list_by_stage[key] = tasks
        return task_list_by_stage, missing_fields

    def list_applications(self, limit: int = 20) -> list[dict[str, Any]]:
        payload = get_json(self._url(f"/api/v1/applications?limit={limit}"), self.timeout)
        return payload or []
