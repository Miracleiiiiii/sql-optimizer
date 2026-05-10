from __future__ import annotations

import json
import os
from typing import Any

from app.core.config import LlmConfig
from app.core.errors import LlmError
from app.core.http import post_json
from app.llm.prompts import SPARK_REPORT_SYSTEM_PROMPT_ZH


class OpenAIProvider:
    def __init__(self, config: LlmConfig) -> None:
        self.config = config

    def generate_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env)
        if not self.config.enabled:
            raise LlmError("LLM is disabled")
        if not api_key:
            raise LlmError(f"Missing API key config or env var: {self.config.api_key_env}")

        request_payload = {
            "model": self.config.model,
            "input": [
                {
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": SPARK_REPORT_SYSTEM_PROMPT_ZH,
                        }
                    ],
                },
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}],
                },
            ],
        }
        url = f"{self.config.api_base_url}/responses"
        response = post_json(url, request_payload, {"Authorization": f"Bearer {api_key}"}, self.config.timeout_seconds)
        text = _extract_response_text(response)
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise LlmError("OpenAI response was not valid JSON") from exc
        _validate_report(parsed)
        return parsed


def _extract_response_text(response: Any) -> str:
    if not isinstance(response, dict):
        raise LlmError("OpenAI response is not an object")
    if isinstance(response.get("output_text"), str):
        return response["output_text"]
    output = response.get("output")
    if isinstance(output, list):
        parts: list[str] = []
        for item in output:
            for content in item.get("content", []) if isinstance(item, dict) else []:
                if isinstance(content, dict) and isinstance(content.get("text"), str):
                    parts.append(content["text"])
        if parts:
            return "\n".join(parts)
    raise LlmError("OpenAI response did not contain text")


def _validate_report(report: dict[str, Any]) -> None:
    required = {"summary", "mainProblems", "recommendations", "validationPlan"}
    missing = sorted(required - set(report.keys()))
    if missing:
        raise LlmError(f"LLM report missing required fields: {', '.join(missing)}")
