from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, AsyncIterator

from bs4 import BeautifulSoup

from scrapper.plugins.base import AsyncFlowPlugin, ExtractedItem, Request, Response


class ExampleArticleTitleAsync(AsyncFlowPlugin):
    site_id = "example_site"
    flow_id = "article_title_async"
    schema_id = "ExampleArticle@v1"

    async def start_requests(self, job_input: dict[str, Any]) -> AsyncIterator[Request]:
        urls = job_input.get("urls") or ["https://example.com/"]
        for u in urls:
            yield Request(url=str(u))

    async def parse(self, response: Response) -> AsyncIterator[Request | ExtractedItem]:
        soup = BeautifulSoup(response.text or "", "lxml")
        title = (soup.title.text if soup.title else "").strip() or "unknown"
        yield ExtractedItem(
            raw={
                "url": response.url,
                "title": title,
                "fetched_at": datetime.now(timezone.utc),
            },
            meta={"url": response.url, "fetched_at": datetime.now(timezone.utc).timestamp()},
        )


FLOW_PLUGINS = [ExampleArticleTitleAsync()]

