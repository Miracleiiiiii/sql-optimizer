from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from app.core.errors import UpstreamApiError


def get_json(url: str, timeout: int = 30) -> Any:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise UpstreamApiError(f"GET {url} failed with HTTP {exc.code}: {exc.reason}") from exc
    except urllib.error.URLError as exc:
        raise UpstreamApiError(f"GET {url} failed: {exc.reason}") from exc
    if not body.strip():
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise UpstreamApiError(f"GET {url} returned non-JSON response") from exc


def post_json(url: str, payload: dict[str, Any], headers: dict[str, str], timeout: int = 60) -> Any:
    data = json.dumps(payload).encode("utf-8")
    merged_headers = {"Content-Type": "application/json", "Accept": "application/json", **headers}
    request = urllib.request.Request(url, data=data, headers=merged_headers, method="POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise UpstreamApiError(f"POST {url} failed with HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise UpstreamApiError(f"POST {url} failed: {exc.reason}") from exc
    return json.loads(body) if body.strip() else None
