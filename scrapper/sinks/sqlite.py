from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RunInfo:
    run_id: str
    started_at: float


class SqliteSink:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._ensure_db()

    def start_run(self, config_snapshot: dict[str, Any]) -> RunInfo:
        run_id = str(uuid.uuid4())
        started_at = time.time()
        with self._conn() as c:
            c.execute(
                "INSERT INTO runs(run_id, started_at, config_json) VALUES(?,?,?)",
                (run_id, started_at, json.dumps(config_snapshot, ensure_ascii=True)),
            )
        return RunInfo(run_id=run_id, started_at=started_at)

    def finish_run(self, run_id: str, ended_at: float) -> None:
        with self._conn() as c:
            c.execute("UPDATE runs SET ended_at=? WHERE run_id=?", (ended_at, run_id))

    def record_request(
        self,
        run_id: str,
        site_id: str,
        flow_id: str,
        url: str,
        attempt: int,
        status: int | None,
        latency_s: float | None,
        result: str,
        error: str | None,
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO requests(
                  run_id, site_id, flow_id, url, attempt, status, latency_s, result, error, ts
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (run_id, site_id, flow_id, url, attempt, status, latency_s, result, error, time.time()),
            )

    def write_record(
        self,
        run_id: str,
        schema_id: str,
        site_id: str,
        flow_id: str,
        url: str,
        fetched_at: float,
        dedupe_key: str,
        data: dict[str, Any],
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT OR IGNORE INTO records(
                  run_id, schema_id, site_id, flow_id, url, fetched_at, dedupe_key, data_json
                )
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    schema_id,
                    site_id,
                    flow_id,
                    url,
                    fetched_at,
                    dedupe_key,
                    json.dumps(data, ensure_ascii=True),
                ),
            )

    def write_quarantine(
        self,
        run_id: str,
        schema_id: str,
        site_id: str,
        flow_id: str,
        url: str,
        error: dict[str, Any],
        raw: dict[str, Any],
    ) -> None:
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO quarantine(
                  run_id, schema_id, site_id, flow_id, url, error_json, raw_json, ts
                )
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    schema_id,
                    site_id,
                    flow_id,
                    url,
                    json.dumps(error, ensure_ascii=True),
                    json.dumps(raw, ensure_ascii=True),
                    time.time(),
                ),
            )

    def _ensure_db(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.execute("PRAGMA journal_mode=WAL;")
            c.execute("PRAGMA synchronous=NORMAL;")
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS runs(
                  run_id TEXT PRIMARY KEY,
                  started_at REAL NOT NULL,
                  ended_at REAL,
                  config_json TEXT NOT NULL
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS requests(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  site_id TEXT NOT NULL,
                  flow_id TEXT NOT NULL,
                  url TEXT NOT NULL,
                  attempt INTEGER NOT NULL,
                  status INTEGER,
                  latency_s REAL,
                  result TEXT NOT NULL,
                  error TEXT,
                  ts REAL NOT NULL
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS records(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  schema_id TEXT NOT NULL,
                  site_id TEXT NOT NULL,
                  flow_id TEXT NOT NULL,
                  url TEXT NOT NULL,
                  fetched_at REAL NOT NULL,
                  dedupe_key TEXT NOT NULL,
                  data_json TEXT NOT NULL,
                  UNIQUE(schema_id, dedupe_key)
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS idx_records_schema_flow ON records(schema_id, flow_id)"
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS quarantine(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id TEXT NOT NULL,
                  schema_id TEXT NOT NULL,
                  site_id TEXT NOT NULL,
                  flow_id TEXT NOT NULL,
                  url TEXT NOT NULL,
                  error_json TEXT NOT NULL,
                  raw_json TEXT NOT NULL,
                  ts REAL NOT NULL
                )
                """
            )

    def _conn(self) -> sqlite3.Connection:
        # SQLite connections are not thread-safe by default; guard access with a lock
        # and open a new connection per call.
        return _ConnCtx(self.path, self._lock)


class _ConnCtx:
    """Context manager returning a sqlite3 connection guarded by a lock."""

    def __init__(self, path: str, lock: threading.Lock) -> None:
        self._path = path
        self._lock = lock
        self._conn: sqlite3.Connection | None = None

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        try:
            c = sqlite3.connect(self._path, timeout=30)
            c.row_factory = sqlite3.Row
            self._conn = c
            return c
        except Exception:
            self._lock.release()
            raise

    def __exit__(self, exc_type, exc, tb) -> None:
        assert self._conn is not None
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()
            self._conn = None
            self._lock.release()

