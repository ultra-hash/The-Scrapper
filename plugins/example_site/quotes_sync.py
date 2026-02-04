from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from bs4 import BeautifulSoup

from scrapper.plugins.base import ExtractedItem, Request, Response, SyncFlowPlugin


class ExampleQuotesSync(SyncFlowPlugin):
    site_id = "example_site"
    flow_id = "quotes_sync"
    schema_id = "ExampleQuote@v1"

    def start_requests(self, job_input: dict[str, Any]) -> Iterable[Request]:
        url = job_input.get("url") or "https://quotes.toscrape.com/"
        yield Request(url=str(url))

    def parse(self, response: Response) -> Iterable[Request | ExtractedItem]:
        soup = BeautifulSoup(response.text or "", "lxml")
        for q in soup.select(".quote"):
            text = (q.select_one(".text").get_text(strip=True) if q.select_one(".text") else "").strip()
            author = (q.select_one(".author").get_text(strip=True) if q.select_one(".author") else "").strip()
            tags = [t.get_text(strip=True) for t in q.select(".tags .tag")]
            if not text or not author:
                continue
            yield ExtractedItem(
                raw={
                    "text": text,
                    "author": author,
                    "tags": tags,
                    "source_url": response.url,
                    "fetched_at": datetime.now(timezone.utc),
                },
                meta={"url": response.url, "fetched_at": datetime.now(timezone.utc).timestamp()},
            )


FLOW_PLUGINS = [ExampleQuotesSync()]

