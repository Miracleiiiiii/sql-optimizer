from __future__ import annotations

import unittest
from unittest.mock import patch

from app.core.config import RuleConfig
from app.core.config import SparkConfig
from app.core.errors import UpstreamApiError
from app.collectors.spark_history import SparkHistoryCollector
from app.diagnosis.engine import DiagnosisEngine
from app.llm.payload import build_llm_payload
from app.models import DiagnosisResult
from app.normalizers.spark import normalize
from app.recommendation.engine import RecommendationEngine


class CorePipelineTest(unittest.TestCase):
    def test_normalize_diagnose_and_recommend(self) -> None:
        history_payload = {
            "application": {"id": "application_1", "name": "demo", "sparkVersion": "3.3.1"},
            "jobs": [],
            "stages": [
                {
                    "stageId": 1,
                    "attemptId": 0,
                    "status": "COMPLETE",
                    "numTasks": 4,
                    "executorRunTime": 100000,
                    "jvmGcTime": 25000,
                    "memoryBytesSpilled": 1024,
                    "diskBytesSpilled": 11 * 1024 * 1024 * 1024,
                    "shuffleReadBytes": 100 * 1024 * 1024,
                    "shuffleWriteBytes": 900 * 1024 * 1024 * 1024,
                }
            ],
            "executors": [
                {
                    "id": "1",
                    "hostPort": "hadoop102:1234",
                    "totalCores": 4,
                    "maxMemory": 4 * 1024 * 1024 * 1024,
                    "totalDuration": 100000,
                    "totalGCTime": 25000,
                    "totalInputBytes": 0,
                    "totalShuffleRead": 0,
                    "totalShuffleWrite": 0,
                    "failedTasks": 0,
                    "totalTasks": 4,
                }
            ],
            "environment": {
                "sparkProperties": [
                    ["spark.app.id", "application_1"],
                    ["spark.executor.memory", "4g"],
                    ["spark.executor.cores", "4"],
                    ["spark.executor.instances", "1"],
                    ["spark.sql.shuffle.partitions", "200"],
                ],
                "resourceProfiles": [],
            },
            "sql": [],
        }
        yarn_payload = {"id": "application_1", "queue": "default", "finalStatus": "SUCCEEDED"}

        metrics = normalize(history_payload, yarn_payload)
        diagnoses = DiagnosisEngine(RuleConfig()).diagnose(metrics)
        recommendations = RecommendationEngine(RuleConfig()).recommend(metrics, diagnoses)

        self.assertEqual(metrics.application["applicationId"], "application_1")
        self.assertIn("GC_HIGH", {item.rule_code for item in diagnoses})
        self.assertIn("SHUFFLE_SPILL_HIGH", {item.rule_code for item in diagnoses})
        self.assertIn("spark.sql.shuffle.partitions", {item.param for item in recommendations})
        self.assertTrue(all(item.auto_applicable is False for item in recommendations))

    def test_command_result_sql_is_not_primary_execution(self) -> None:
        history_payload = {
            "application": {"id": "application_sql", "name": "sql-demo"},
            "jobs": [],
            "stages": [],
            "executors": [],
            "environment": {"sparkProperties": [["spark.app.id", "application_sql"]], "resourceProfiles": []},
            "sql": [
                {
                    "id": 0,
                    "status": "COMPLETED",
                    "description": "insert overwrite table t select * from s",
                    "planDescription": "== Physical Plan ==\nExecute InsertIntoHiveTable\n+- BroadcastHashJoin\n:- Scan hive default.s\n",
                    "duration": 1000,
                    "successJobIds": [0],
                    "nodes": [{"nodeId": 1, "nodeName": "BroadcastHashJoin", "metrics": []}],
                },
                {
                    "id": 1,
                    "status": "COMPLETED",
                    "description": "insert overwrite table t select * from s",
                    "planDescription": "== Physical Plan ==\nCommandResult\n+- Execute InsertIntoHiveTable\n",
                    "duration": 20,
                    "successJobIds": [],
                    "nodes": [{"nodeId": 0, "nodeName": "CommandResult", "metrics": []}],
                },
            ],
        }
        metrics = normalize(history_payload, {"id": "application_sql", "finalStatus": "SUCCEEDED"})
        payload = build_llm_payload(metrics, [], [], include_sql=True)

        self.assertTrue(metrics.sql_executions[0]["isPrimaryExecution"])
        self.assertFalse(metrics.sql_executions[1]["isPrimaryExecution"])
        self.assertTrue(metrics.sql_executions[1]["isCommandResult"])
        self.assertEqual(len(payload["sqlSummary"]), 1)
        self.assertEqual(len(payload["ignoredSqlExecutions"]), 1)

    def test_task_level_skew_is_diagnosed_from_task_list(self) -> None:
        normal_tasks = [
            {
                "taskId": task_id,
                "duration": 1000,
                "executorRunTime": 900,
                "executorId": "1",
                "host": "hadoop101",
                "taskMetrics": {
                    "shuffleReadMetrics": {"localBytesRead": 1024, "remoteBytesRead": 1024},
                    "shuffleWriteMetrics": {"bytesWritten": 0},
                    "memoryBytesSpilled": 0,
                    "diskBytesSpilled": 0,
                    "peakExecutionMemory": 64 * 1024 * 1024,
                },
            }
            for task_id in range(25)
        ]
        skewed_task = {
            "taskId": 26,
            "duration": 8600,
            "executorRunTime": 8200,
            "executorId": "2",
            "host": "hadoop102",
            "taskMetrics": {
                "shuffleReadMetrics": {"localBytesRead": 21 * 1024 * 1024, "remoteBytesRead": 21 * 1024 * 1024},
                "shuffleWriteMetrics": {"bytesWritten": 0},
                "memoryBytesSpilled": 0,
                "diskBytesSpilled": 0,
                "peakExecutionMemory": 768 * 1024 * 1024,
            },
        }
        history_payload = {
            "application": {"id": "application_skew", "name": "skew-demo"},
            "jobs": [],
            "stages": [
                {
                    "stageId": 2,
                    "attemptId": 0,
                    "status": "COMPLETE",
                    "numTasks": 26,
                    "executorRunTime": 17217,
                    "jvmGcTime": 0,
                    "memoryBytesSpilled": 0,
                    "diskBytesSpilled": 0,
                    "shuffleReadBytes": 51 * 1024 * 1024,
                    "shuffleWriteBytes": 0,
                }
            ],
            "taskListByStage": {"2:0": normal_tasks + [skewed_task]},
            "executors": [],
            "environment": {"sparkProperties": [["spark.app.id", "application_skew"]], "resourceProfiles": []},
            "sql": [],
        }

        metrics = normalize(history_payload, {"id": "application_skew", "finalStatus": "SUCCEEDED"})
        diagnoses = DiagnosisEngine(RuleConfig(task_skew_ratio=5)).diagnose(metrics)

        self.assertEqual(len(metrics.tasks), 26)
        skew = next(item for item in diagnoses if item.rule_code == "TASK_SKEW_HIGH")
        self.assertEqual(skew.problem_type, "data_skew")
        self.assertEqual(skew.evidence["stageId"], 2)
        self.assertEqual(skew.evidence["skewedTaskId"], 26)
        self.assertGreaterEqual(skew.evidence["durationSkewRatio"], 5)

        payload = build_llm_payload(metrics, diagnoses, [], include_sql=True)
        self.assertEqual(payload["facts"]["metricsSummary"]["taskCount"], 26)
        self.assertEqual(payload["taskSkewSummary"][0]["skewedTaskId"], 26)

    def test_task_skew_recommendations_are_sql_and_aqe_focused(self) -> None:
        metrics = normalize(
            {
                "application": {"id": "application_skew"},
                "jobs": [],
                "stages": [],
                "executors": [],
                "environment": {"sparkProperties": [["spark.app.id", "application_skew"]], "resourceProfiles": []},
                "sql": [],
            },
            {"id": "application_skew"},
        )
        skew = DiagnosisResult(
            rule_code="TASK_SKEW_HIGH",
            problem_type="data_skew",
            severity="high",
            confidence=0.86,
            evidence={"stageId": 2, "skewedTaskId": 26, "durationSkewRatio": 8.6},
            suspected_cause="one task is much slower than the median task",
            tuning_direction=["inspect join/group by key distribution"],
        )
        recommendations = RecommendationEngine(RuleConfig()).recommend(
            metrics,
            [skew],
        )
        self.assertIn("sql", {item.param for item in recommendations})
        self.assertIn("spark.sql.adaptive.enabled", {item.param for item in recommendations})

    def test_spark_history_collector_fetches_stage_task_lists(self) -> None:
        responses = {
            "http://history/api/v1/applications/application_1": {"id": "application_1"},
            "http://history/api/v1/applications/application_1/jobs": [],
            "http://history/api/v1/applications/application_1/stages": [
                {"stageId": 2, "attemptId": 0},
                {"stageId": 3, "attemptId": 1},
            ],
            "http://history/api/v1/applications/application_1/executors": [],
            "http://history/api/v1/applications/application_1/environment": {},
            "http://history/api/v1/applications/application_1/sql": [],
            "http://history/api/v1/applications/application_1/stages/2/0/taskList": [{"taskId": 26}],
            "http://history/api/v1/applications/application_1/stages/3/1/taskList": [{"taskId": 7}],
        }

        with patch("app.collectors.spark_history.get_json", side_effect=lambda url, timeout: responses[url]):
            payload = SparkHistoryCollector(SparkConfig(history_server_url="http://history")).collect_application("application_1")

        self.assertEqual(payload["taskListByStage"], {"2:0": [{"taskId": 26}], "3:1": [{"taskId": 7}]})

    def test_spark_history_collector_keeps_analysis_when_task_list_fetch_fails(self) -> None:
        def fake_get_json(url: str, timeout: int):
            if url.endswith("/stages/2/0/taskList"):
                raise UpstreamApiError("task list unavailable")
            if url.endswith("/stages"):
                return [{"stageId": 2, "attemptId": 0}]
            if url.endswith("/applications/application_1"):
                return {"id": "application_1"}
            return [] if not url.endswith("/environment") else {}

        with patch("app.collectors.spark_history.get_json", side_effect=fake_get_json):
            payload = SparkHistoryCollector(SparkConfig(history_server_url="http://history")).collect_application("application_1")

        self.assertEqual(payload["stages"], [{"stageId": 2, "attemptId": 0}])
        self.assertEqual(payload["taskListByStage"], {})
        self.assertIn("spark_history.stages.2.0.taskList", payload["missingFields"])


if __name__ == "__main__":
    unittest.main()
