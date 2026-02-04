from __future__ import annotations

import importlib
import pkgutil
from typing import Iterable

from pydantic import BaseModel

from scrapper.normalization.registry import SchemaRegistry


def load_schemas(registry: SchemaRegistry) -> None:
    """
    Imports all modules under `scrapper.schemas` and registers models decorated with @schema(...).
    """
    pkg = __name__
    for m in _iter_modules():
        importlib.import_module(f"{pkg}.{m}")

    # Register any BaseModel subclasses with __schema_id__
    for cls in _iter_schema_models():
        schema_id = getattr(cls, "__schema_id__", None)
        if schema_id:
            registry.register(schema_id, cls)


def _iter_modules() -> Iterable[str]:
    for info in pkgutil.iter_modules(__path__):  # type: ignore[name-defined]
        if info.ispkg:
            continue
        if info.name.startswith("_"):
            continue
        if info.name == "__init__":
            continue
        yield info.name


def _iter_schema_models() -> Iterable[type[BaseModel]]:
    # Walk loaded modules to find schema models; keep simple and explicit.
    import sys

    for mod_name, mod in list(sys.modules.items()):
        if not mod_name.startswith(__name__ + "."):
            continue
        for obj in vars(mod).values():
            try:
                if isinstance(obj, type) and issubclass(obj, BaseModel):
                    yield obj
            except Exception:
                continue

