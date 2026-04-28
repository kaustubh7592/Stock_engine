"""Technical feature family built from price_daily."""

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

TECHNICAL_FEATURES = (
    "close_price",
    "daily_return_pct",
    "return_5d_pct",
    "return_20d_pct",
    "volatility_20d_pct",
    "delivery_pct_latest",
    "delivery_pct_vs_20d_avg",
    "intraday_range_pct",
)


def compute_technical_features(
    price_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Compute first-pass technical features from available EOD history."""

    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in price_rows:
        instrument_id = _text(row.get("instrument_id"))
        trade_date = date_or_none(row.get("trade_date"))
        if not instrument_id or trade_date is None or trade_date > as_of_date:
            continue
        grouped[instrument_id].append(row)

    output = []
    for instrument_id, rows in grouped.items():
        rows.sort(key=lambda item: date_or_none(item.get("trade_date")) or date.min)
        latest = rows[-1]
        available_at = latest_available_at(rows)
        close_prices = [_decimal(row.get("close_price")) for row in rows]
        closes = [value for value in close_prices if value is not None]
        delivery_values = [_decimal(row.get("deliverable_pct")) for row in rows]
        deliveries = [value for value in delivery_values if value is not None]

        output.extend(
            [
                _record(
                    instrument_id,
                    as_of_date,
                    "close_price",
                    _decimal(latest.get("close_price")),
                    _decimal(latest.get("close_price")) is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "daily_return_pct",
                    _return_pct(closes[-2], closes[-1]) if len(closes) >= 2 else None,
                    len(closes) >= 2,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "return_5d_pct",
                    _return_pct(closes[-6], closes[-1]) if len(closes) >= 6 else None,
                    len(closes) >= 6,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "return_20d_pct",
                    _return_pct(closes[-21], closes[-1]) if len(closes) >= 21 else None,
                    len(closes) >= 21,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "volatility_20d_pct",
                    _volatility_pct(closes[-21:]) if len(closes) >= 21 else None,
                    len(closes) >= 21,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "delivery_pct_latest",
                    _decimal(latest.get("deliverable_pct")),
                    _decimal(latest.get("deliverable_pct")) is not None,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "delivery_pct_vs_20d_avg",
                    _delivery_vs_average(deliveries[-20:]) if len(deliveries) >= 20 else None,
                    len(deliveries) >= 20,
                    available_at,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "intraday_range_pct",
                    _intraday_range_pct(latest),
                    _intraday_range_pct(latest) is not None,
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
        feature_family="technical",
        feature_name=feature_name,
        value_num=value,
        coverage_flag=coverage_flag,
        source="price_daily",
        available_at=available_at,
    )


def _return_pct(previous: Decimal | None, current: Decimal | None) -> Decimal | None:
    if previous is None or current is None or previous == 0:
        return None
    return (current / previous - Decimal("1")) * Decimal("100")


def _volatility_pct(closes: list[Decimal]) -> Decimal | None:
    returns = [_return_pct(closes[index - 1], closes[index]) for index in range(1, len(closes))]
    values = [value for value in returns if value is not None]
    if len(values) < 2:
        return None
    avg = sum(values) / Decimal(len(values))
    variance = sum((value - avg) ** 2 for value in values) / Decimal(len(values))
    return Decimal(str(float(variance) ** 0.5))


def _delivery_vs_average(values: list[Decimal]) -> Decimal | None:
    if len(values) < 20:
        return None
    avg = sum(values) / Decimal(len(values))
    if avg == 0:
        return None
    return values[-1] - avg


def _intraday_range_pct(row: dict[str, object]) -> Decimal | None:
    high = _decimal(row.get("high_price"))
    low = _decimal(row.get("low_price"))
    close = _decimal(row.get("close_price"))
    if high is None or low is None or close in (None, Decimal("0")):
        return None
    return (high - low) / close * Decimal("100")


def _decimal(value: object) -> Decimal | None:
    return decimal_or_none(value)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None

