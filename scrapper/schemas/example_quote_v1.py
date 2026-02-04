from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from scrapper.normalization.registry import schema


@schema("ExampleQuote@v1")
class ExampleQuoteV1(BaseModel):
    text: str = Field(min_length=1)
    author: str = Field(min_length=1)
    tags: list[str] = Field(default_factory=list)
    source_url: str = Field(min_length=1)
    fetched_at: datetime

