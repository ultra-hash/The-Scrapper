from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    retry_on_status: tuple[int, ...] = (429, 500, 502, 503, 504)
    base_backoff_s: float = 0.5
    max_backoff_s: float = 20.0


@dataclass(frozen=True)
class PolicyConfig:
    concurrency: int = 50
    per_domain_rps: float = 2.0
    per_domain_burst: int = 5
    timeout_s: float = 20.0
    max_queue_size: int = 5000
    sync_threadpool_workers: int = 32
    retry: RetryConfig = RetryConfig()
    circuit_breaker_fail_threshold: int = 20
    circuit_breaker_cooldown_s: float = 60.0


@dataclass(frozen=True)
class SqliteSinkConfig:
    path: str = "./data/scraped.db"


@dataclass(frozen=True)
class SinkConfig:
    type: str = "sqlite"
    sqlite: SqliteSinkConfig = SqliteSinkConfig()


@dataclass(frozen=True)
class JobConfig:
    site: str
    flow: str
    schema: str
    input: dict[str, Any]
    policies: PolicyConfig = PolicyConfig()
    sink: SinkConfig = SinkConfig()


def load_job_config(path: str | Path) -> JobConfig:
    p = Path(path)
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))

    retry = raw.get("policies", {}).get("retry", {}) or {}
    policies_raw = raw.get("policies", {}) or {}
    policies = PolicyConfig(
        concurrency=int(policies_raw.get("concurrency", PolicyConfig.concurrency)),
        per_domain_rps=float(policies_raw.get("per_domain_rps", PolicyConfig.per_domain_rps)),
        per_domain_burst=int(policies_raw.get("per_domain_burst", PolicyConfig.per_domain_burst)),
        timeout_s=float(policies_raw.get("timeout_s", PolicyConfig.timeout_s)),
        max_queue_size=int(policies_raw.get("max_queue_size", PolicyConfig.max_queue_size)),
        sync_threadpool_workers=int(
            policies_raw.get("sync_threadpool_workers", PolicyConfig.sync_threadpool_workers)
        ),
        retry=RetryConfig(
            max_attempts=int(retry.get("max_attempts", RetryConfig.max_attempts)),
            retry_on_status=tuple(retry.get("retry_on_status", list(RetryConfig.retry_on_status))),
            base_backoff_s=float(retry.get("base_backoff_s", RetryConfig.base_backoff_s)),
            max_backoff_s=float(retry.get("max_backoff_s", RetryConfig.max_backoff_s)),
        ),
        circuit_breaker_fail_threshold=int(
            policies_raw.get(
                "circuit_breaker_fail_threshold", PolicyConfig.circuit_breaker_fail_threshold
            )
        ),
        circuit_breaker_cooldown_s=float(
            policies_raw.get("circuit_breaker_cooldown_s", PolicyConfig.circuit_breaker_cooldown_s)
        ),
    )

    sink_raw = raw.get("sink", {}) or {}
    sink_type = sink_raw.get("type", "sqlite")
    sqlite_raw = sink_raw.get("sqlite", {}) or {}
    sink = SinkConfig(
        type=sink_type,
        sqlite=SqliteSinkConfig(path=str(sqlite_raw.get("path", SqliteSinkConfig.path))),
    )

    return JobConfig(
        site=str(raw["site"]),
        flow=str(raw["flow"]),
        schema=str(raw["schema"]),
        input=dict(raw.get("input", {}) or {}),
        policies=policies,
        sink=sink,
    )
