"""Deterministic identifier helpers."""

from __future__ import annotations

import re
import uuid

_NAMESPACE = uuid.UUID("7c45609a-3dd4-4c1e-8fb6-22091a3c85f6")
_TOKEN_RE = re.compile(r"[^A-Za-z0-9]+")


def normalize_token(value: object) -> str:
    """Normalize a value for deterministic ID generation."""

    text = "" if value is None else str(value)
    text = _TOKEN_RE.sub("_", text.strip().upper())
    return text.strip("_")


def stable_id(prefix: str, *parts: object) -> str:
    """Create a stable text ID from business keys."""

    normalized_prefix = normalize_token(prefix)
    body = "|".join(normalize_token(part) for part in parts)
    digest = uuid.uuid5(_NAMESPACE, f"{normalized_prefix}|{body}")
    return f"{normalized_prefix}_{digest.hex[:16]}"
