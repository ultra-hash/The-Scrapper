from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel

from scrapper.exceptions import SchemaNotFoundError


@dataclass(frozen=True)
class SchemaEntry:
    schema_id: str
    model: type[BaseModel]


class SchemaRegistry:
    def __init__(self) -> None:
        self._schemas: dict[str, SchemaEntry] = {}

    def register(self, schema_id: str, model: type[BaseModel]) -> None:
        self._schemas[schema_id] = SchemaEntry(schema_id=schema_id, model=model)

    def get(self, schema_id: str) -> type[BaseModel]:
        if schema_id not in self._schemas:
            known = ", ".join(sorted(self._schemas.keys())) or "(none)"
            raise SchemaNotFoundError(f"Unknown schema_id={schema_id}. Known: {known}")
        return self._schemas[schema_id].model

    def validate(self, schema_id: str, raw: dict[str, Any]) -> BaseModel:
        model = self.get(schema_id)
        return model.model_validate(raw)


def schema(schema_id: str) -> Callable[[type[BaseModel]], type[BaseModel]]:
    """
    Decorator used by schema modules. Registration is done in `load_schemas()`.
    """

    def _inner(cls: type[BaseModel]) -> type[BaseModel]:
        setattr(cls, "__schema_id__", schema_id)
        return cls

    return _inner

