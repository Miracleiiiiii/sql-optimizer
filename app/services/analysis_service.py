from __future__ import annotations

import uuid
from dataclasses import asdict
from typing import Any

from app.collectors.spark_history import SparkHistoryCollector
from app.collectors.yarn import YarnCollector
from app.core.config import Settings
from app.core.errors import LlmError, SparkAiOptimizerError
from app.diagnosis.engine import DiagnosisEngine
from app.llm.openai_provider import OpenAIProvider
from app.llm.payload import build_llm_payload
from app.normalizers.spark import normalize
from app.recommendation.engine import RecommendationEngine
from app.reports.builder import build_rule_report
from app.storage.sqlite import SQLiteStore


class AnalysisService:
    def __init__(self, settings: Settings, store: SQLiteStore) -> None:
        self.settings = settings
        self.store = store
        self.history = SparkHistoryCollector(settings.spark)
        self.yarn = YarnCollector(settings.yarn)
        self.diagnosis = DiagnosisEngine(settings.rules)
        self.recommendation = RecommendationEngine(settings.rules)
        self.llm = OpenAIProvider(settings.llm)

    def submit(self, application_id: str) -> str:
        analysis_id = f"ana_{uuid.uuid4().hex[:16]}"
        self.store.create_analysis(analysis_id, application_id)
        return analysis_id

    def run(self, analysis_id: str, application_id: str) -> None:
        try:
            report_id, report = self.analyze(application_id)
            self.store.save_report(report_id, application_id, report)
            self.store.update_analysis(analysis_id, "success", report_id=report_id)
        except Exception as exc:  # service boundary should capture all failures
            self.store.update_analysis(analysis_id, "failed", error=str(exc))

    def analyze(self, application_id: str) -> tuple[str, dict[str, Any]]:
        history_payload = self.history.collect_application(application_id)
        yarn_payload = self.yarn.collect_application(application_id)
        self.store.save_snapshot(application_id, "spark_history", history_payload)
        self.store.save_snapshot(application_id, "yarn", yarn_payload)

        metrics = normalize(history_payload, yarn_payload)
        diagnoses = self.diagnosis.diagnose(metrics)
        recommendations = self.recommendation.recommend(metrics, diagnoses)

        ai_report = None
        ai_error = None
        try:
            payload = build_llm_payload(
                metrics,
                diagnoses,
                recommendations,
                include_sql=self.settings.analysis.enable_sql_to_llm,
            )
            ai_report = self.llm.generate_report(payload)
        except (LlmError, SparkAiOptimizerError) as exc:
            ai_error = str(exc)

        report = build_rule_report(metrics, diagnoses, recommendations, ai_report)
        report["analysisMeta"] = {
            "applicationId": application_id,
            "aiStatus": "success" if ai_report else "failed_or_skipped",
            "aiError": ai_error,
            "diagnosisCount": len(diagnoses),
            "recommendationCount": len(recommendations),
        }
        report["debug"] = {
            "sparkConf": metrics.spark_conf,
            "yarn": metrics.yarn,
            "sqlExecutions": metrics.sql_executions,
        }
        report_id = f"rep_{uuid.uuid4().hex[:16]}"
        return report_id, report

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        return self.store.get_analysis(analysis_id)

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        return self.store.get_report(report_id)

    def feedback(self, recommendation_id: str, accepted: bool, comment: str | None = None) -> dict[str, Any]:
        # Feedback persistence is intentionally lightweight in MVP because
        # recommendations are embedded in reports. P1 can normalize this table.
        return {"recommendationId": recommendation_id, "accepted": accepted, "comment": comment}


def serialize_dataclasses(items: list[Any]) -> list[dict[str, Any]]:
    return [asdict(item) for item in items]

