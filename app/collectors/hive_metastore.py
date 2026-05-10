from __future__ import annotations

from app.core.config import HiveConfig


class HiveMetastoreCollector:
    """Placeholder for P1 thrift integration.

    MVP does not require Hive Metastore because Spark SQL API already exposes
    scanned tables, written tables, physical plans, and node metrics.
    """

    def __init__(self, config: HiveConfig) -> None:
        self.config = config

    def enabled(self) -> bool:
        return self.config.enabled and bool(self.config.metastore_uri)

