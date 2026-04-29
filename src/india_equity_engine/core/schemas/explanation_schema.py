"""Validated explanation output schema."""

from __future__ import annotations

from pydantic import BaseModel, Field


class StockExplainerOutput(BaseModel):
    """Strict explanation payload emitted after stock_snapshot validation."""

    short_term_outlook: str
    long_term_outlook: str
    likes: list[str] = Field(default_factory=list)
    dislikes: list[str] = Field(default_factory=list)
    major_risks: list[str] = Field(default_factory=list)
    conflicting_or_missing_evidence: list[str] = Field(default_factory=list)

