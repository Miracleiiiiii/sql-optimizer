from __future__ import annotations

import unittest

from app.core.config import RuleConfig
from app.diagnosis.engine import DiagnosisEngine
from app.llm.payload import build_llm_payload
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


if __name__ == "__main__":
    unittest.main()
