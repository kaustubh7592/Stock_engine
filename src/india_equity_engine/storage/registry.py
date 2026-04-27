"""Source registry backed by configs/sources/*.yaml."""

from __future__ import annotations

from pathlib import Path

import yaml

from india_equity_engine.core.exceptions import ConfigError
from india_equity_engine.core.schemas.contracts import SourceConfig


class SourceRegistry:
    """Load and query configured Stage A sources."""

    def __init__(self, config_dir: str | Path) -> None:
        self.config_dir = Path(config_dir)
        self.sources = self._load_sources()

    def _load_sources(self) -> dict[str, SourceConfig]:
        sources_dir = self.config_dir / "sources"
        if not sources_dir.exists():
            raise ConfigError(f"Missing sources directory: {sources_dir}")

        output: dict[str, SourceConfig] = {}
        for path in sorted(sources_dir.glob("*.yaml")):
            with path.open("r", encoding="utf-8") as handle:
                raw = yaml.safe_load(handle) or {}
            for item in raw.get("sources", []):
                source = SourceConfig.model_validate(item)
                if source.code in output:
                    raise ConfigError(f"Duplicate source code {source.code} in {path}")
                output[source.code] = source
        return output

    def get(self, code: str) -> SourceConfig:
        try:
            return self.sources[code]
        except KeyError as exc:
            raise ConfigError(f"Unknown source code: {code}") from exc

    def enabled(self) -> list[SourceConfig]:
        return [source for source in self.sources.values() if source.enabled]
