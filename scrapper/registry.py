from __future__ import annotations

import importlib
import importlib.util
import inspect
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from scrapper.exceptions import PluginLoadError
from scrapper.plugins.base import AsyncFlowPlugin, FlowPlugin, SyncFlowPlugin


@dataclass(frozen=True)
class RegisteredFlow:
    site_id: str
    flow_id: str
    schema_id: str
    plugin: FlowPlugin
    is_async: bool


class PluginRegistry:
    """
    Discovers flow plugins from:
    - Development: local `plugins/` directory (imported as modules)
    - Production: python entry points (TODO: can be added later without changing plugin API)

    A plugin module should expose either:
    - `FLOW_PLUGINS`: an iterable of flow plugin instances
    - `get_flow_plugins()`: a function returning an iterable of flow plugin instances
    """

    def __init__(self) -> None:
        self._flows: dict[tuple[str, str], RegisteredFlow] = {}

    def register(self, plugin: FlowPlugin) -> None:
        key = (plugin.site_id, plugin.flow_id)
        # Protocol isinstance checks are structural and can't reliably distinguish sync vs async.
        # Use coroutine detection on the parse/start methods.
        try:
            is_async = inspect.iscoroutinefunction(getattr(plugin, "start_requests")) or inspect.iscoroutinefunction(
                getattr(plugin, "parse")
            )
        except Exception as e:  # noqa: BLE001
            raise PluginLoadError(f"Failed to introspect flow plugin {plugin!r}: {e}") from e
        self._flows[key] = RegisteredFlow(
            site_id=plugin.site_id,
            flow_id=plugin.flow_id,
            schema_id=plugin.schema_id,
            plugin=plugin,
            is_async=is_async,
        )

    def get_flow(self, site_id: str, flow_id: str) -> RegisteredFlow:
        key = (site_id, flow_id)
        if key not in self._flows:
            known = ", ".join(sorted([f"{s}:{f}" for (s, f) in self._flows.keys()])) or "(none)"
            raise PluginLoadError(f"Unknown flow {site_id}:{flow_id}. Known: {known}")
        return self._flows[key]

    def all_flows(self) -> list[RegisteredFlow]:
        return list(self._flows.values())

    def load_from_plugins_dir(self, plugins_dir: str | Path = "plugins") -> None:
        base = Path(plugins_dir)
        if not base.exists():
            return

        for py in base.rglob("*.py"):
            if py.name.startswith("_"):
                continue
            self._load_module_from_path(py)

    def _load_module_from_path(self, path: Path) -> None:
        # Build a stable module name under `plugins.*` so relative imports work.
        # Example: plugins/example_site/product_detail_async.py -> plugins.example_site.product_detail_async
        try:
            rel = path.with_suffix("").as_posix()
            if rel.startswith("./"):
                rel = rel[2:]
            mod_name = rel.replace("/", ".").replace("\\", ".")

            # Ensure repo root is importable
            root = str(Path(".").resolve())
            if root not in sys.path:
                sys.path.insert(0, root)

            spec = importlib.util.spec_from_file_location(mod_name, path)
            if spec is None or spec.loader is None:
                raise PluginLoadError(f"Could not create import spec for {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[mod_name] = module
            spec.loader.exec_module(module)  # type: ignore[assignment]

            plugins = _extract_flow_plugins(module)
            for p in plugins:
                self.register(p)
        except Exception as e:  # noqa: BLE001 - surface plugin failures clearly
            raise PluginLoadError(f"Failed to load plugin module {path}: {e}") from e


def _extract_flow_plugins(module) -> Iterable[FlowPlugin]:
    if hasattr(module, "get_flow_plugins"):
        plugins = module.get_flow_plugins()
        return list(plugins)
    if hasattr(module, "FLOW_PLUGINS"):
        return list(module.FLOW_PLUGINS)
    return []
