"""Derivatives feature family built from derivatives_eod."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import (
    date_or_none,
    decimal_or_none,
    feature_record,
    latest_available_at,
)


def compute_derivatives_features(
    derivatives_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Compute underlying-level options/futures features from contract EOD rows."""

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in derivatives_rows:
        instrument_id = _text(row.get("instrument_id"))
        trade_date = date_or_none(row.get("trade_date"))
        if not instrument_id or trade_date is None or trade_date > as_of_date:
            continue
        grouped[instrument_id].append(row)

    output = []
    for instrument_id, rows in grouped.items():
        latest_date = max(date_or_none(row.get("trade_date")) or date.min for row in rows)
        latest_rows = [
            row for row in rows if date_or_none(row.get("trade_date")) == latest_date
        ]
        available_at = latest_available_at(latest_rows)
        total_oi = _sum_decimal(latest_rows, "open_interest")
        total_oi_change = _sum_decimal(latest_rows, "oi_change")
        total_volume = _sum_decimal(latest_rows, "contract_volume")
        put_oi = _sum_decimal(
            [row for row in latest_rows if _text(row.get("option_type")) == "PE"],
            "open_interest",
        )
        call_oi = _sum_decimal(
            [row for row in latest_rows if _text(row.get("option_type")) == "CE"],
            "open_interest",
        )
        output.extend(
            [
                _record(
                    instrument_id,
                    as_of_date,
                    "derivatives_open_interest_latest",
                    total_oi,
                    total_oi is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "derivatives_oi_change_latest",
                    total_oi_change,
                    total_oi_change is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "derivatives_oi_change_pct_latest",
                    _pct(total_oi_change, total_oi - total_oi_change)
                    if total_oi is not None and total_oi_change is not None
                    else None,
                    total_oi is not None and total_oi_change is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "derivatives_contract_volume_latest",
                    total_volume,
                    total_volume is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "derivatives_put_call_oi_ratio_latest",
                    put_oi / call_oi if put_oi is not None and call_oi not in (None, 0) else None,
                    put_oi is not None and call_oi not in (None, 0),
                    available_at,
                ),
            ]
        )
    return output


def _record(
    instrument_id: str,
    as_of_date: date,
    feature_name: str,
    value: Decimal | None,
    coverage_flag: bool,
    available_at: object | None,
) -> NormalizedRecord:
    return feature_record(
        instrument_id=instrument_id,
        as_of_date=as_of_date,
        horizon="short",
        feature_family="derivatives",
        feature_name=feature_name,
        value_num=value,
        coverage_flag=coverage_flag,
        source="derivatives_eod",
        available_at=available_at,
    )


def _sum_decimal(rows: list[dict[str, object]], column: str) -> Decimal | None:
    values = [decimal_or_none(row.get(column)) for row in rows]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return sum(values, Decimal("0"))


def _pct(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if numerator is None or denominator in (None, Decimal("0")):
        return None
    return numerator / denominator * Decimal("100")


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None
