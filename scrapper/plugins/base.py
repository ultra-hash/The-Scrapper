from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Iterable, Mapping, Protocol, runtime_checkable


@dataclass(frozen=True)
class Request:
    url: str
    method: str = "GET"
    headers: Mapping[str, str] | None = None
    params: Mapping[str, str] | None = None
    data: Any = None
    timeout_s: float | None = None
    use_browser: bool = False


@dataclass(frozen=True)
class Response:
    url: str
    status: int
    headers: Mapping[str, str]
    text: str | None = None
    content: bytes | None = None


@dataclass(frozen=True)
class ExtractedItem:
    raw: dict[str, Any]
    meta: dict[str, Any]


@runtime_checkable
class SitePlugin(Protocol):
    site_id: str
    domains: tuple[str, ...]

    def default_headers(self) -> Mapping[str, str] | None: ...


class FlowPlugin(Protocol):
    site_id: str
    flow_id: str
    schema_id: str  # e.g., "Product@v3"


class SyncSitePlugin(SitePlugin, Protocol):
    pass


class AsyncSitePlugin(SitePlugin, Protocol):
    pass


@runtime_checkable
class SyncFlowPlugin(FlowPlugin, Protocol):
    def start_requests(self, job_input: dict[str, Any]) -> Iterable[Request]: ...

    def parse(self, response: Response) -> Iterable[Request | ExtractedItem]: ...


@runtime_checkable
class AsyncFlowPlugin(FlowPlugin, Protocol):
    async def start_requests(self, job_input: dict[str, Any]) -> AsyncIterator[Request]: ...

    async def parse(self, response: Response) -> AsyncIterator[Request | ExtractedItem]: ...


class SyncFlowPlugin(Protocol):
    site_id: str
    flow_id: str
    schema_id: str
    kind: FlowKind

    def start_requests(self, job_input: dict[str, Any]) -> Iterable[Request]: ...

    def parse(self, response: Response) -> Iterable[Request | ExtractedItem]: ...

