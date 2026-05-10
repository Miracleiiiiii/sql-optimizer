from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - runtime dependency in service mode
    yaml = None


ROOT_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SparkConfig:
    history_server_url: str
    event_log_dir: str = ""
    event_log_retention_days: int = 7


@dataclass(frozen=True)
class YarnConfig:
    resource_manager_url: str


@dataclass(frozen=True)
class HiveConfig:
    metastore_uri: str = ""
    enabled: bool = False


@dataclass(frozen=True)
class LlmConfig:
    provider: str = "openai"
    api_base_url: str = "https://api.openai.com/v1"
    model: str = "gpt-5.4"
    api_key_env: str = "OPENAI_API_KEY"
    api_key: str | None = field(default=None, repr=False)
    timeout_seconds: int = 60
    enabled: bool = True


@dataclass(frozen=True)
class AnalysisConfig:
    default_mode: str = "application"
    enable_sql_to_llm: bool = True
    enable_sql_masking: bool = True
    cache_existing_report: bool = True
    sqlite_path: str = "data/spark_ai_optimizer_mvp.sqlite3"


@dataclass(frozen=True)
class RuleConfig:
    gc_high_ratio: float = 0.2
    task_skew_ratio: float = 5.0
    spill_bytes_high: int = 10 * 1024 * 1024 * 1024
    spill_to_shuffle_ratio: float = 0.2
    target_shuffle_partition_mb_min: int = 128
    target_shuffle_partition_mb_max: int = 256
    max_shuffle_partitions: int = 5000


@dataclass(frozen=True)
class Settings:
    spark: SparkConfig
    yarn: YarnConfig
    hive: HiveConfig
    llm: LlmConfig
    analysis: AnalysisConfig
    rules: RuleConfig


def _deep_get(data: dict[str, Any], section: str) -> dict[str, Any]:
    value = data.get(section, {})
    return value if isinstance(value, dict) else {}


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if yaml is None:
        return _load_simple_yaml(path)
    with path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a YAML object: {path}")
    return loaded


def _load_simple_yaml(path: Path) -> dict[str, Any]:
    """Tiny YAML subset parser for the bundled config file.

    It supports top-level sections and scalar key/value pairs. Production
    deployments should install PyYAML from requirements.txt.
    """
    data: dict[str, Any] = {}
    current_section: str | None = None
    with path.open("r", encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.split("#", 1)[0].rstrip()
            if not line.strip():
                continue
            if not line.startswith(" ") and line.endswith(":"):
                current_section = line[:-1].strip()
                data[current_section] = {}
                continue
            if current_section and ":" in line:
                key, value = line.strip().split(":", 1)
                data[current_section][key.strip()] = _parse_scalar(value.strip())
    return data


def _parse_scalar(value: str) -> Any:
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_settings(config_path: str | None = None) -> Settings:
    path = Path(config_path or os.getenv("SAO_CONFIG", ROOT_DIR / "config" / "application.yaml"))
    if not path.exists():
        path = ROOT_DIR / "config" / "application.example.yaml"
    data = _load_yaml(path)

    spark = _deep_get(data, "spark")
    yarn = _deep_get(data, "yarn")
    hive = _deep_get(data, "hive")
    llm = _deep_get(data, "llm")
    analysis = _deep_get(data, "analysis")
    rules = _deep_get(data, "rules")

    return Settings(
        spark=SparkConfig(
            history_server_url=str(spark.get("history_server_url", "")).rstrip("/"),
            event_log_dir=str(spark.get("event_log_dir", "")),
            event_log_retention_days=int(spark.get("event_log_retention_days", 7)),
        ),
        yarn=YarnConfig(resource_manager_url=str(yarn.get("resource_manager_url", "")).rstrip("/")),
        hive=HiveConfig(
            metastore_uri=str(hive.get("metastore_uri", "")),
            enabled=bool(hive.get("enabled", False)),
        ),
        llm=LlmConfig(
            provider=str(llm.get("provider", "openai")),
            api_base_url=str(llm.get("api_base_url", "https://api.openai.com/v1")).rstrip("/"),
            model=str(llm.get("model", "gpt-5.4")),
            api_key_env=str(llm.get("api_key_env", "OPENAI_API_KEY")),
            api_key=str(llm.get("api_key")) if llm.get("api_key") else None,
            timeout_seconds=int(llm.get("timeout_seconds", 60)),
            enabled=bool(llm.get("enabled", True)),
        ),
        analysis=AnalysisConfig(
            default_mode=str(analysis.get("default_mode", "application")),
            enable_sql_to_llm=bool(analysis.get("enable_sql_to_llm", True)),
            enable_sql_masking=bool(analysis.get("enable_sql_masking", True)),
            cache_existing_report=bool(analysis.get("cache_existing_report", True)),
            sqlite_path=str(analysis.get("sqlite_path", "data/spark_ai_optimizer_mvp.sqlite3")),
        ),
        rules=RuleConfig(
            gc_high_ratio=float(rules.get("gc_high_ratio", 0.2)),
            task_skew_ratio=float(rules.get("task_skew_ratio", 5)),
            spill_bytes_high=int(rules.get("spill_bytes_high", 10 * 1024 * 1024 * 1024)),
            spill_to_shuffle_ratio=float(rules.get("spill_to_shuffle_ratio", 0.2)),
            target_shuffle_partition_mb_min=int(rules.get("target_shuffle_partition_mb_min", 128)),
            target_shuffle_partition_mb_max=int(rules.get("target_shuffle_partition_mb_max", 256)),
            max_shuffle_partitions=int(rules.get("max_shuffle_partitions", 5000)),
        ),
    )


settings = load_settings()
