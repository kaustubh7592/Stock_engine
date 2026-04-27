"""Minimal local FastAPI app."""

from __future__ import annotations

from fastapi import FastAPI

from india_equity_engine import __version__

app = FastAPI(title="India Equity Research Engine", version=__version__)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": __version__}
