"""Logging setup."""

from __future__ import annotations

import logging
import logging.config
from pathlib import Path

import yaml


def configure_logging(config_dir: str | Path = "configs") -> None:
    """Configure logging from configs/logging.yaml when present."""

    path = Path(config_dir) / "logging.yaml"
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            logging.config.dictConfig(yaml.safe_load(handle))
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
