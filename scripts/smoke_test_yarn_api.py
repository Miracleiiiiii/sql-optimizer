from __future__ import annotations

import argparse
import json

from app.collectors.yarn import YarnCollector
from app.core.config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("application_id")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    settings = load_settings(args.config)
    collector = YarnCollector(settings.yarn)
    payload = collector.collect_application(args.application_id)
    summary = {
        "id": payload.get("id"),
        "name": payload.get("name"),
        "queue": payload.get("queue"),
        "state": payload.get("state"),
        "finalStatus": payload.get("finalStatus"),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

