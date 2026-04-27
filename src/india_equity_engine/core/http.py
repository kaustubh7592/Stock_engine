"""HTTP client helpers."""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import Any

from india_equity_engine.core.settings import Settings


def build_http_verify(settings: Settings) -> bool | str | ssl.SSLContext:
    """Build the httpx TLS verification setting.

    Python distributions on Windows often use certifi rather than the system trust store.
    In corporate environments, the system store is commonly the right source of trust.
    """

    if settings.ca_bundle:
        return str(Path(settings.ca_bundle))

    if settings.use_system_cert_store:
        context = _truststore_context()
        if context is not None:
            return context

    return True


def _truststore_context() -> ssl.SSLContext | None:
    try:
        import truststore
    except ImportError:
        return None

    context: Any = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    return context
