"""Event/news carryover feature family built from event_signals."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import (
    date_or_none,
    decimal_or_none,
    feature_record,
    latest_available_at,
)


def compute_event_features(
    event_signal_rows: list[dict[str, object]],
    instrument_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Map direct and sector-level event signals into instrument feature snapshots."""

    sector_map = {
        str(row["instrument_id"]): _text(row.get("sector_name"))
        for row in instrument_rows
        if row.get("instrument_id")
    }
    rows_by_instrument: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in event_signal_rows:
        event_date = date_or_none(row.get("event_date"))
        if event_date is None or not (as_of_date - timedelta(days=30) <= event_date <= as_of_date):
            continue
        direct_instrument = _text(row.get("instrument_id"))
        if direct_instrument:
            rows_by_instrument[direct_instrument].append(row)
            continue
        signal_sector = _text(row.get("sector_name"))
        if signal_sector is None:
            continue
        for instrument_id, sector_name in sector_map.items():
            if sector_name and sector_name.lower() == signal_sector.lower():
                rows_by_instrument[instrument_id].append(row)

    output = []
    for instrument_id, rows in sorted(rows_by_instrument.items()):
        for horizon in sorted({_text(row.get("horizon")) or "short" for row in rows}):
            horizon_rows = [
                row for row in rows if (_text(row.get("horizon")) or "short") == horizon
            ]
            impacts = [_impact_value(row) for row in horizon_rows]
            impacts = [value for value in impacts if value is not None]
            available_at = latest_available_at(horizon_rows)
            output.extend(
                [
                    _record(
                        instrument_id,
                        as_of_date,
                        horizon,
                        "event_signal_count_30d",
                        Decimal(len(horizon_rows)),
                        True,
                        available_at,
                    ),
                    _record(
                        instrument_id,
                        as_of_date,
                        horizon,
                        "event_net_impact_30d",
                        sum(impacts, Decimal("0")) if impacts else None,
                        bool(impacts),
                        available_at,
                    ),
                    _record(
                        instrument_id,
                        as_of_date,
                        horizon,
                        "event_negative_signal_count_30d",
                        Decimal(sum(1 for value in impacts if value < 0)),
                        bool(impacts),
                        available_at,
                    ),
                    _record(
                        instrument_id,
                        as_of_date,
                        horizon,
                        "event_positive_signal_count_30d",
                        Decimal(sum(1 for value in impacts if value > 0)),
                        bool(impacts),
                        available_at,
                    ),
                ]
            )
    return output


def _record(
    instrument_id: str,
    as_of_date: date,
    horizon: str,
    feature_name: str,
    value: Decimal | None,
    coverage_flag: bool,
    available_at: object | None,
) -> NormalizedRecord:
    return feature_record(
        instrument_id=instrument_id,
        as_of_date=as_of_date,
        horizon=horizon if horizon in {"short", "medium", "long"} else "short",
        feature_family="event",
        feature_name=feature_name,
        value_num=value,
        coverage_flag=coverage_flag,
        source="event_signals",
        available_at=available_at,
    )


def _impact_value(row: dict[str, object]) -> Decimal | None:
    confidence = decimal_or_none(row.get("confidence")) or Decimal("0.5")
    impact = decimal_or_none(row.get("impact_score"))
    direction = str(row.get("impact_direction") or "").lower()
    if impact is None:
        if direction == "positive":
            impact = Decimal("0.20")
        elif direction == "negative":
            impact = Decimal("-0.20")
        else:
            return None
    return impact * confidence


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
