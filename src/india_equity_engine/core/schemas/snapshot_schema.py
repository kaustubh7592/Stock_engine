"""Pydantic model for deterministic stock snapshot JSON."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

Classification = Literal["bullish", "neutral", "bearish", "abstain"]
Direction = Literal["positive", "negative", "mixed", "unknown"]


class InstrumentSnapshot(BaseModel):
    instrument_id: str
    isin: str | None = None
    name: str
    nse_symbol: str | None = None
    bse_scrip_code: str | None = None
    sector: str | None = None
    industry: str | None = None


class SnapshotMeta(BaseModel):
    as_of_date: date
    generated_at: datetime
    data_freshness: dict[str, str] = Field(default_factory=dict)


class SignalSnapshot(BaseModel):
    name: str
    direction: Direction
    confidence: float = Field(ge=0, le=1)
    evidence_ids: list[str] = Field(default_factory=list)


class HorizonScore(BaseModel):
    technical: float | None = Field(default=None, ge=0, le=1)
    fundamental: float | None = Field(default=None, ge=0, le=1)
    governance: float | None = Field(default=None, ge=0, le=1)
    macro: float | None = Field(default=None, ge=0, le=1)
    events: float | None = Field(default=None, ge=0, le=1)
    derivatives: float | None = Field(default=None, ge=0, le=1)
    composite: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    classification: Classification


class DecisionSnapshot(BaseModel):
    primary_horizon: Literal["short", "medium", "long"]
    stance: str
    abstain: bool
    key_risks: list[str] = Field(default_factory=list)
    top_positive_drivers: list[str] = Field(default_factory=list)
    top_negative_drivers: list[str] = Field(default_factory=list)


class StockSnapshot(BaseModel):
    schema_version: str
    instrument: InstrumentSnapshot
    snapshot_meta: SnapshotMeta
    features: dict[str, dict[str, Any]] = Field(default_factory=dict)
    signals: list[SignalSnapshot] = Field(default_factory=list)
    scores: dict[str, HorizonScore] = Field(default_factory=dict)
    decision: DecisionSnapshot
