from prometheus_client import Counter, Histogram

REQUESTS_TOTAL = Counter(
    "scrapper_requests_total",
    "Total HTTP requests attempted",
    ["site", "flow", "status", "result"],
)

ITEMS_TOTAL = Counter(
    "scrapper_items_total",
    "Total items processed",
    ["site", "flow", "schema", "result"],
)

REQUEST_LATENCY = Histogram(
    "scrapper_request_latency_seconds",
    "Latency of HTTP requests",
    ["site", "flow"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10),
)


def record_request(site: str, flow: str, status: int, result: str, latency: float) -> None:
    REQUESTS_TOTAL.labels(site=site, flow=flow, status=str(status), result=result).inc()
    REQUEST_LATENCY.labels(site=site, flow=flow).observe(latency)


def record_item(site: str, flow: str, schema: str, result: str) -> None:
    ITEMS_TOTAL.labels(site=site, flow=flow, schema=schema, result=result).inc()

