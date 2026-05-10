from __future__ import annotations

from typing import Any
from urllib.parse import quote

from app.core.config import SparkConfig
from app.core.http import get_json


class SparkHistoryCollector:
    def __init__(self, config: SparkConfig, timeout: int = 30) -> None:
        self.config = config
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.config.history_server_url}{path}"

    def collect_application(self, application_id: str) -> dict[str, Any]:
        app_id = quote(application_id, safe="")
        payload = {
            "application": get_json(self._url(f"/api/v1/applications/{app_id}"), self.timeout),
            "jobs": get_json(self._url(f"/api/v1/applications/{app_id}/jobs"), self.timeout) or [],
            "stages": get_json(self._url(f"/api/v1/applications/{app_id}/stages"), self.timeout) or [],
            "executors": get_json(self._url(f"/api/v1/applications/{app_id}/executors"), self.timeout) or [],
            "environment": get_json(self._url(f"/api/v1/applications/{app_id}/environment"), self.timeout) or {},
            "sql": get_json(self._url(f"/api/v1/applications/{app_id}/sql"), self.timeout) or [],
        }
        return payload

    def list_applications(self, limit: int = 20) -> list[dict[str, Any]]:
        payload = get_json(self._url(f"/api/v1/applications?limit={limit}"), self.timeout)
        return payload or []

