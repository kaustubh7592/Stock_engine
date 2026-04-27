"""Configuration loading for local Stage A runs."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from india_equity_engine.core.exceptions import ConfigError


@dataclass(frozen=True)
class Settings:
    """Resolved application settings."""

    config_dir: Path
    data_root: Path
    raw_root: Path
    silver_root: Path
    gold_root: Path
    cache_root: Path
    logs_root: Path
    duckdb_path: Path
    http_timeout_seconds: float
    user_agent: str
    use_system_cert_store: bool
    ca_bundle: Path | None
    parser_version: str

    @classmethod
    def load(cls, config_dir: str | Path | None = None) -> Settings:
        root = Path(config_dir or os.getenv("IEE_CONFIG_DIR", "configs")).resolve()
        app_config_path = root / "app.yaml"
        if not app_config_path.exists():
            raise ConfigError(f"Missing config file: {app_config_path}")

        with app_config_path.open("r", encoding="utf-8") as handle:
            raw: dict[str, Any] = yaml.safe_load(handle) or {}

        data_root = Path(os.getenv("IEE_DATA_ROOT", raw.get("data_root", "data"))).resolve()
        warehouse = raw.get("warehouse", {})
        http = raw.get("http", {})
        pipeline = raw.get("pipeline", {})

        def path_from_config(key: str, default: str) -> Path:
            return Path(raw.get(key, default)).resolve()

        return cls(
            config_dir=root,
            data_root=data_root,
            raw_root=path_from_config("raw_root", str(data_root / "raw")),
            silver_root=path_from_config("silver_root", str(data_root / "silver")),
            gold_root=path_from_config("gold_root", str(data_root / "gold")),
            cache_root=path_from_config("cache_root", str(data_root / "cache")),
            logs_root=path_from_config("logs_root", str(data_root / "logs")),
            duckdb_path=Path(
                warehouse.get("duckdb_path", data_root / "warehouse" / "india_equity_engine.duckdb")
            ).resolve(),
            http_timeout_seconds=float(http.get("timeout_seconds", 30)),
            user_agent=str(http.get("user_agent", "india-equity-engine/0.1")),
            use_system_cert_store=bool(http.get("use_system_cert_store", True)),
            ca_bundle=Path(http["ca_bundle"]).resolve() if http.get("ca_bundle") else None,
            parser_version=str(pipeline.get("parser_version", "unknown")),
        )

    def ensure_runtime_dirs(self) -> None:
        """Create local runtime directories."""

        for path in (
            self.data_root,
            self.raw_root,
            self.silver_root,
            self.gold_root,
            self.cache_root,
            self.logs_root,
            self.duckdb_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)
