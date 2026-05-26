from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawSnapshot:
    application_id: str
    source: str
    payload: Any


@dataclass
class NormalizedMetrics:
    application: dict[str, Any]
    yarn: dict[str, Any]
    spark_conf: dict[str, str]
    resource_profiles: list[dict[str, Any]]
    jobs: list[dict[str, Any]]
    stages: list[dict[str, Any]]
    tasks: list[dict[str, Any]]
    executors: list[dict[str, Any]]
    sql_executions: list[dict[str, Any]]
    missing_fields: list[str] = field(default_factory=list)


@dataclass
class DiagnosisResult:
    rule_code: str
    problem_type: str
    severity: str
    confidence: float
    evidence: dict[str, Any]
    suspected_cause: str
    tuning_direction: list[str]


@dataclass
class Recommendation:
    param: str
    current: str | None
    suggested: str
    priority: str
    confidence: float
    evidence: list[str]
    risk: str
    validation: str
    auto_applicable: bool = False
