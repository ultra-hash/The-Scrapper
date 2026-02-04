from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import asdict, is_dataclass
from typing import Any

import httpx
import requests
from prometheus_client import start_http_server
from pydantic import ValidationError

from scrapper.config import JobConfig
from scrapper.http.clients import fetch_async, fetch_sync
from scrapper.http.policies import CircuitBreaker, RateLimiter, RetryPolicy, domain_of, retry_after_seconds
from scrapper.logging import get_logger, setup_logging
from scrapper.monitoring.metrics import record_item, record_request
from scrapper.normalization.registry import SchemaRegistry
from scrapper.registry import PluginRegistry
from scrapper.schemas import load_schemas
from scrapper.plugins.base import ExtractedItem
from scrapper.sinks.sqlite import SqliteSink

log = get_logger("scrapper.engine")


class _Stop(Exception):
    pass


def _snapshot(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, dict):
        return {k: _snapshot(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_snapshot(v) for v in obj]
    return obj


def _dedupe_key(schema_id: str, site_id: str, flow_id: str, item: dict[str, Any]) -> str:
    natural = (
        item.get("id")
        or item.get("url")
        or item.get("source_url")
        or f"{item.get('title','')}/{item.get('author','')}/{item.get('text','')}"
    )
    s = json.dumps({"schema": schema_id, "site": site_id, "flow": flow_id, "k": natural}, sort_keys=True)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


async def run_job(job: JobConfig, plugins_dir: str = "plugins", metrics_port: int = 9009) -> None:
    setup_logging()

    if metrics_port and metrics_port > 0:
        start_http_server(metrics_port)
        log.info("metrics server started", extra={"extra_fields": {"port": metrics_port}})

    # Load plugins
    registry = PluginRegistry()
    registry.load_from_plugins_dir(plugins_dir)
    reg_flow = registry.get_flow(job.site, job.flow)

    # Load schemas
    schemas = SchemaRegistry()
    load_schemas(schemas)

    # Validate job schema aligns with plugin (allow override but warn)
    if job.schema != reg_flow.schema_id:
        log.warning(
            "job.schema differs from plugin.schema_id; using job.schema",
            extra={
                "extra_fields": {
                    "job_schema": job.schema,
                    "plugin_schema": reg_flow.schema_id,
                    "site": job.site,
                    "flow": job.flow,
                }
            },
        )

    # Sink
    if job.sink.type != "sqlite":
        raise ValueError(f"Unsupported sink.type={job.sink.type} (only sqlite implemented)")
    sink = SqliteSink(job.sink.sqlite.path)
    run = await asyncio.to_thread(sink.start_run, _snapshot(job))

    # Policies
    retry = RetryPolicy(
        max_attempts=job.policies.retry.max_attempts,
        retry_on_status=job.policies.retry.retry_on_status,
        base_backoff_s=job.policies.retry.base_backoff_s,
        max_backoff_s=job.policies.retry.max_backoff_s,
    )
    limiter = RateLimiter(rps=job.policies.per_domain_rps, burst=job.policies.per_domain_burst)
    breaker = CircuitBreaker(
        fail_threshold=job.policies.circuit_breaker_fail_threshold,
        cooldown_s=job.policies.circuit_breaker_cooldown_s,
    )
    sem = asyncio.Semaphore(job.policies.concurrency)

    # Queues
    request_q: asyncio.Queue[tuple[Any, int]] = asyncio.Queue(maxsize=job.policies.max_queue_size)
    item_q: asyncio.Queue[ExtractedItem] = asyncio.Queue(maxsize=job.policies.max_queue_size)

    async def enqueue_request(req: Any, attempt: int) -> None:
        await request_q.put((req, attempt))

    async def enqueue_item(item: ExtractedItem) -> None:
        await item_q.put(item)

    # Seed
    try:
        if reg_flow.is_async:
            async for r in reg_flow.plugin.start_requests(job.input):  # type: ignore[attr-defined]
                await enqueue_request(r, 1)
        else:
            for r in reg_flow.plugin.start_requests(job.input):  # type: ignore[attr-defined]
                await enqueue_request(r, 1)
    except Exception as e:  # noqa: BLE001
        await asyncio.to_thread(sink.finish_run, run.run_id, time.time())
        raise

    async def fetch_and_parse_worker(worker_id: int) -> None:
        async with httpx.AsyncClient() as async_client:
            sync_session = requests.Session()
            loop = asyncio.get_running_loop()
            while True:
                req, attempt = await request_q.get()
                try:
                    async with sem:
                        url = req.url
                        dom = domain_of(url)
                        breaker.pre_request(dom)
                        await limiter.acquire(dom)

                        t0 = time.perf_counter()
                        try:
                            if reg_flow.is_async:
                                resp = await fetch_async(async_client, req, job.policies.timeout_s)
                            else:
                                resp = await loop.run_in_executor(
                                    None, lambda: fetch_sync(sync_session, req, job.policies.timeout_s)
                                )
                            latency = time.perf_counter() - t0
                        except Exception as e:  # noqa: BLE001
                            latency = time.perf_counter() - t0
                            breaker.on_failure(dom)
                            record_request(job.site, job.flow, status=0, result="error", latency=latency)
                            await asyncio.to_thread(
                                sink.record_request,
                                run.run_id,
                                job.site,
                                job.flow,
                                url,
                                attempt,
                                None,
                                latency,
                                "error",
                                f"{type(e).__name__}:{e}",
                            )
                            if attempt < retry.max_attempts:
                                await asyncio.sleep(retry.backoff_s(attempt))
                                await enqueue_request(req, attempt + 1)
                            continue

                        # Request accounting
                        status = resp.status
                        result = "ok" if 200 <= status < 400 else "http_error"
                        record_request(job.site, job.flow, status=status, result=result, latency=latency)
                        await asyncio.to_thread(
                            sink.record_request,
                            run.run_id,
                            job.site,
                            job.flow,
                            resp.url,
                            attempt,
                            status,
                            latency,
                            result,
                            None if result == "ok" else f"status={status}",
                        )

                        # Retry on status (429/5xx)
                        if (status == 429 or retry.should_retry_status(status)) and attempt < retry.max_attempts:
                            breaker.on_failure(dom)
                            ra = retry_after_seconds(dict(resp.headers))
                            if ra is not None:
                                await asyncio.sleep(min(ra, retry.max_backoff_s))
                            else:
                                await asyncio.sleep(retry.backoff_s(attempt))
                            await enqueue_request(req, attempt + 1)
                            continue

                        if 200 <= status < 400:
                            breaker.on_success(dom)
                        else:
                            breaker.on_failure(dom)
                            continue

                        # Parse
                        try:
                            if reg_flow.is_async:
                                async for out in reg_flow.plugin.parse(resp):  # type: ignore[attr-defined]
                                    if hasattr(out, "url"):
                                        await enqueue_request(out, 1)
                                    else:
                                        await enqueue_item(out)
                            else:
                                outs = reg_flow.plugin.parse(resp)  # type: ignore[attr-defined]
                                for out in outs:
                                    if hasattr(out, "url"):
                                        await enqueue_request(out, 1)
                                    else:
                                        await enqueue_item(out)
                        except Exception as e:  # noqa: BLE001
                            log.exception(
                                "parse failed",
                                extra={"extra_fields": {"site": job.site, "flow": job.flow, "url": resp.url}},
                            )
                            # parsing errors aren't retried automatically; plugin should emit followups explicitly
                            continue
                finally:
                    request_q.task_done()

    async def item_worker() -> None:
        while True:
            extracted = await item_q.get()
            try:
                raw = extracted.raw
                meta = extracted.meta
                src_url = str(meta.get("url") or raw.get("url") or raw.get("source_url") or "")
                fetched_at = float(meta.get("fetched_at") or time.time())

                try:
                    model = schemas.validate(job.schema, raw)
                    data = model.model_dump(mode="json")
                    dk = _dedupe_key(job.schema, job.site, job.flow, data)
                    await asyncio.to_thread(
                        sink.write_record,
                        run.run_id,
                        job.schema,
                        job.site,
                        job.flow,
                        src_url,
                        fetched_at,
                        dk,
                        data,
                    )
                    record_item(job.site, job.flow, job.schema, result="ok")
                except ValidationError as ve:
                    await asyncio.to_thread(
                        sink.write_quarantine,
                        run.run_id,
                        job.schema,
                        job.site,
                        job.flow,
                        src_url,
                        {"error": "validation_failed", "details": json.loads(ve.json())},
                        raw,
                    )
                    record_item(job.site, job.flow, job.schema, result="invalid")
            finally:
                item_q.task_done()

    workers = [asyncio.create_task(fetch_and_parse_worker(i)) for i in range(max(1, job.policies.concurrency // 5))]
    item_workers = [asyncio.create_task(item_worker()) for _ in range(2)]

    try:
        await request_q.join()
        await item_q.join()
    finally:
        for t in workers + item_workers:
            t.cancel()
        await asyncio.gather(*workers, *item_workers, return_exceptions=True)
        await asyncio.to_thread(sink.finish_run, run.run_id, time.time())

