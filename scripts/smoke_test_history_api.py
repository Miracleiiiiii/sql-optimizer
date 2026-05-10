from __future__ import annotations

import argparse
import json

from app.collectors.spark_history import SparkHistoryCollector
from app.core.config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("application_id")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    settings = load_settings(args.config)
    collector = SparkHistoryCollector(settings.spark)
    payload = collector.collect_application(args.application_id)
    summary = {
        "applicationKeys": sorted(payload.get("application", {}).keys()) if isinstance(payload.get("application"), dict) else [],
        "jobs": len(payload.get("jobs") or []),
        "stages": len(payload.get("stages") or []),
        "executors": len(payload.get("executors") or []),
        "sql": len(payload.get("sql") or []),
        "hasEnvironment": bool(payload.get("environment")),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

