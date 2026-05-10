from __future__ import annotations

from typing import Any
from urllib.parse import quote

from app.core.config import YarnConfig
from app.core.http import get_json


class YarnCollector:
    def __init__(self, config: YarnConfig, timeout: int = 30) -> None:
        self.config = config
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.config.resource_manager_url}{path}"

    def collect_application(self, application_id: str) -> dict[str, Any]:
        app_id = quote(application_id, safe="")
        payload = get_json(self._url(f"/ws/v1/cluster/apps/{app_id}"), self.timeout)
        if isinstance(payload, dict) and "app" in payload:
            return payload["app"] or {}
        return payload or {}

    def list_applications(self, limit: int = 20) -> list[dict[str, Any]]:
        payload = get_json(self._url(f"/ws/v1/cluster/apps?limit={limit}"), self.timeout)
        if isinstance(payload, dict):
            return payload.get("apps", {}).get("app", []) or []
        return []

