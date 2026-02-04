from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl

from scrapper.normalization.registry import schema


@schema("ExampleArticle@v1")
class ExampleArticleV1(BaseModel):
    url: HttpUrl
    title: str = Field(min_length=1)
    fetched_at: datetime

