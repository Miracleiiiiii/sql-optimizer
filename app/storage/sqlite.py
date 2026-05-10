from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class SQLiteStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            self.path = Path.cwd() / self.path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        # The Codex Windows workspace can reject SQLite's default rollback
        # journal rename flow. These pragmas keep the MVP store usable locally;
        # production deployments can switch to PostgreSQL as planned.
        conn.execute("pragma journal_mode=OFF")
        conn.execute("pragma synchronous=OFF")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                create table if not exists analysis_task (
                    analysis_id text primary key,
                    application_id text not null,
                    status text not null,
                    report_id text,
                    error_message text,
                    created_at text default current_timestamp,
                    updated_at text default current_timestamp
                );
                create table if not exists raw_snapshot (
                    id integer primary key autoincrement,
                    application_id text not null,
                    source text not null,
                    payload_json text not null,
                    created_at text default current_timestamp
                );
                create table if not exists report (
                    report_id text primary key,
                    application_id text not null,
                    report_json text not null,
                    created_at text default current_timestamp
                );
                """
            )

    def create_analysis(self, analysis_id: str, application_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "insert into analysis_task (analysis_id, application_id, status) values (?, ?, ?)",
                (analysis_id, application_id, "running"),
            )

    def update_analysis(self, analysis_id: str, status: str, report_id: str | None = None, error: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                update analysis_task
                set status = ?, report_id = coalesce(?, report_id), error_message = ?, updated_at = current_timestamp
                where analysis_id = ?
                """,
                (status, report_id, error, analysis_id),
            )

    def get_analysis(self, analysis_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from analysis_task where analysis_id = ?", (analysis_id,)).fetchone()
        return dict(row) if row else None

    def save_snapshot(self, application_id: str, source: str, payload: Any) -> None:
        with self.connect() as conn:
            conn.execute(
                "insert into raw_snapshot (application_id, source, payload_json) values (?, ?, ?)",
                (application_id, source, json.dumps(payload, ensure_ascii=False)),
            )

    def save_report(self, report_id: str, application_id: str, report: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                "insert or replace into report (report_id, application_id, report_json) values (?, ?, ?)",
                (report_id, application_id, json.dumps(report, ensure_ascii=False)),
            )

    def get_report(self, report_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("select * from report where report_id = ?", (report_id,)).fetchone()
        if not row:
            return None
        data = dict(row)
        data["report_json"] = json.loads(data["report_json"])
        return data
