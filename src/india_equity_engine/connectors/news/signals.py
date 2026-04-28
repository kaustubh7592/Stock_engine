"""Rule-based event signal mapper for news_items."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from india_equity_engine.connectors.news.classification import (
    normalize_search_text,
    sectors_for_channel,
)
from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now


@dataclass(frozen=True)
class NewsSignalSourceRow:
    news_id: str
    published_at: object | None
    source_name: str | None
    source_class: str | None
    headline: str | None
    url: str | None
    event_type: str | None
    entities_json: str | None
    source: str | None
    source_url: str | None
    retrieved_at: object | None
    available_at: object | None
    as_of_date: object | None
    document_hash: str | None
    parser_version: str | None
    restated_flag: bool | None
    created_at: object | None
    updated_at: object | None


@dataclass(frozen=True)
class SignalSpec:
    exposure_channel: str
    horizon: str
    sector_name: str
    impact_direction: str
    impact_score: Decimal | None
    confidence: Decimal
    rationale_code: str


def map_event_signals(rows: list[NewsSignalSourceRow]) -> list[NormalizedRecord]:
    """Map classified news_items into sector-level event_signals."""

    output = []
    now = utc_now()
    for row in rows:
        event_date = _event_date(row.published_at, row.as_of_date)
        if event_date is None:
            continue
        specs = _signal_specs(row)
        for spec in specs:
            signal_id = stable_id(
                "event_signal",
                row.news_id,
                spec.exposure_channel,
                spec.sector_name,
                spec.horizon,
                spec.rationale_code,
            )
            output.append(
                NormalizedRecord(
                    table_name="event_signals",
                    row={
                        "event_signal_id": signal_id,
                        "news_id": row.news_id,
                        "instrument_id": None,
                        "sector_name": spec.sector_name,
                        "event_date": event_date,
                        "horizon": spec.horizon,
                        "impact_direction": spec.impact_direction,
                        "impact_score": spec.impact_score,
                        "confidence": _source_adjusted_confidence(
                            spec.confidence,
                            row.source_class,
                        ),
                        "exposure_channel": spec.exposure_channel,
                        "rationale_code": spec.rationale_code,
                        "source": row.source or "news_items",
                        "source_url": row.url or row.source_url,
                        "retrieved_at": row.retrieved_at or now,
                        "available_at": row.available_at or row.published_at or now,
                        "as_of_date": row.as_of_date or event_date,
                        "document_hash": row.document_hash,
                        "parser_version": row.parser_version,
                        "restated_flag": bool(row.restated_flag)
                        if row.restated_flag is not None
                        else False,
                        "created_at": row.created_at or now,
                        "updated_at": row.updated_at or now,
                    },
                )
            )
    return output


def news_signal_source_row_from_dict(row: dict[str, Any]) -> NewsSignalSourceRow:
    return NewsSignalSourceRow(
        news_id=str(row.get("news_id") or ""),
        published_at=row.get("published_at"),
        source_name=_string_or_none(row.get("source_name")),
        source_class=_string_or_none(row.get("source_class")),
        headline=_string_or_none(row.get("headline")),
        url=_string_or_none(row.get("url")),
        event_type=_string_or_none(row.get("event_type")),
        entities_json=_string_or_none(row.get("entities_json")),
        source=_string_or_none(row.get("source")),
        source_url=_string_or_none(row.get("source_url")),
        retrieved_at=row.get("retrieved_at"),
        available_at=row.get("available_at"),
        as_of_date=row.get("as_of_date"),
        document_hash=_string_or_none(row.get("document_hash")),
        parser_version=_string_or_none(row.get("parser_version")),
        restated_flag=row.get("restated_flag"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _signal_specs(row: NewsSignalSourceRow) -> list[SignalSpec]:
    channels = _channels_from_entities(row.entities_json)
    channels.extend(_channels_from_text(row.headline, row.event_type))
    unique_channels = []
    for channel in channels:
        if channel not in unique_channels:
            unique_channels.append(channel)

    specs = []
    for channel in unique_channels:
        specs.extend(_specs_for_channel(channel, row.source_class))
    return specs


def _specs_for_channel(channel: str, source_class: str | None) -> list[SignalSpec]:
    horizon = "medium" if channel in {"government_capex_budget", "rural_agriculture"} else "short"
    if channel == "oil_fuel":
        return [
            SignalSpec(
                channel,
                "short",
                "Aviation",
                "negative",
                Decimal("-0.35"),
                Decimal("0.68"),
                "oil_import_cost",
            ),
            SignalSpec(
                channel,
                "short",
                "Paints",
                "negative",
                Decimal("-0.30"),
                Decimal("0.62"),
                "oil_input_cost",
            ),
            SignalSpec(
                channel,
                "short",
                "Chemicals",
                "negative",
                Decimal("-0.25"),
                Decimal("0.58"),
                "oil_input_cost",
            ),
            SignalSpec(
                channel,
                "short",
                "Logistics",
                "negative",
                Decimal("-0.22"),
                Decimal("0.55"),
                "fuel_cost",
            ),
            SignalSpec(
                channel,
                "short",
                "Oil & Gas",
                "mixed",
                None,
                Decimal("0.55"),
                "oil_sector_mixed",
            ),
        ]
    if channel == "fx":
        return [
            SignalSpec(channel, "short", sector, "mixed", None, Decimal("0.55"), "fx_translation")
            for sector in sectors_for_channel(channel)
        ]
    if channel == "rates_liquidity":
        return [
            SignalSpec(
                channel,
                "medium",
                sector,
                "mixed",
                None,
                Decimal("0.68"),
                "rates_liquidity_policy",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "government_capex_budget":
        return [
            SignalSpec(
                channel,
                horizon,
                sector,
                "positive",
                Decimal("0.30"),
                Decimal("0.58"),
                "policy_capex",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "rural_agriculture":
        return [
            SignalSpec(
                channel,
                horizon,
                sector,
                "positive",
                Decimal("0.24"),
                Decimal("0.52"),
                "rural_demand_policy",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "commodity_input_cost":
        return [
            SignalSpec(
                channel,
                "short",
                sector,
                "mixed",
                None,
                Decimal("0.50"),
                "commodity_input_cost",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "governance_regulation":
        return [
            SignalSpec(
                channel,
                "medium",
                sector,
                "mixed",
                None,
                Decimal("0.60"),
                "sector_regulation",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "election":
        confidence = Decimal("0.56") if source_class == "government" else Decimal("0.48")
        return [
            SignalSpec(
                channel,
                "medium",
                sector,
                "mixed",
                None,
                confidence,
                "election_policy_context",
            )
            for sector in sectors_for_channel(channel)
        ]
    if channel == "geopolitical":
        return [
            SignalSpec(
                channel,
                "short",
                sector,
                "mixed",
                None,
                Decimal("0.45"),
                "geopolitical_context",
            )
            for sector in sectors_for_channel(channel)
        ]
    return []


def _channels_from_entities(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    channels = payload.get("exposure_channels")
    if not isinstance(channels, list):
        return []
    return [str(channel) for channel in channels if channel]


def _channels_from_text(headline: str | None, event_type: str | None) -> list[str]:
    text = normalize_search_text(f"{headline or ''} {event_type or ''}")
    output = []
    mapping = {
        "oil_fuel": ("oil", "crude", "fuel", "petroleum", "gas"),
        "fx": ("rupee", "dollar", "usd", "forex", "fema"),
        "rates_liquidity": ("repo", "rate", "liquidity", "mpc", "monetary"),
        "government_capex_budget": ("budget", "capex", "infrastructure", "defence", "railway"),
        "rural_agriculture": ("rural", "agriculture", "farmer", "fertilizer", "fertiliser"),
        "commodity_input_cost": ("steel", "metal", "coal", "cement", "commodity"),
        "governance_regulation": ("regulation", "regulatory", "circular", "notification", "sebi"),
        "election": ("election", "poll", "eci", "voting"),
        "geopolitical": ("war", "conflict", "sanction", "tariff"),
    }
    for channel, tokens in mapping.items():
        if any(token in text for token in tokens):
            output.append(channel)
    return output


def _source_adjusted_confidence(value: Decimal, source_class: str | None) -> Decimal:
    adjustment = {
        "exchange": Decimal("0.08"),
        "regulator": Decimal("0.08"),
        "government": Decimal("0.03"),
        "global_news": Decimal("-0.08"),
        "media_optional": Decimal("-0.12"),
    }.get(source_class or "", Decimal("0"))
    return max(Decimal("0.10"), min(Decimal("0.95"), value + adjustment))


def _event_date(published_at: object | None, as_of_date: object | None) -> date | None:
    if isinstance(published_at, datetime):
        return published_at.date()
    if isinstance(published_at, date):
        return published_at
    if isinstance(as_of_date, datetime):
        return as_of_date.date()
    if isinstance(as_of_date, date):
        return as_of_date
    return None


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
