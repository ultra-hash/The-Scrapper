from __future__ import annotations

from dataclasses import asdict
from typing import Any, Mapping

import httpx
import requests

from scrapper.plugins.base import Request, Response


def _headers(h: Mapping[str, str] | None) -> dict[str, str] | None:
    return dict(h) if h else None


async def fetch_async(client: httpx.AsyncClient, req: Request, default_timeout_s: float) -> Response:
    timeout = req.timeout_s if req.timeout_s is not None else default_timeout_s
    r = await client.request(
        method=req.method,
        url=req.url,
        headers=_headers(req.headers),
        params=req.params,
        content=req.data,
        timeout=timeout,
        follow_redirects=True,
    )
    # Decode text lazily; keep both bytes and text
    content = r.content
    text: str | None
    try:
        text = r.text
    except Exception:
        text = None
    return Response(url=str(r.url), status=r.status_code, headers=dict(r.headers), text=text, content=content)


def fetch_sync(session: requests.Session, req: Request, default_timeout_s: float) -> Response:
    timeout = req.timeout_s if req.timeout_s is not None else default_timeout_s
    r = session.request(
        method=req.method,
        url=req.url,
        headers=_headers(req.headers),
        params=req.params,
        data=req.data,
        timeout=timeout,
        allow_redirects=True,
    )
    content = r.content
    try:
        text = r.text
    except Exception:
        text = None
    return Response(url=str(r.url), status=r.status_code, headers=dict(r.headers), text=text, content=content)

