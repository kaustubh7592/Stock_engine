"""Data-quality checks for daily prices."""

from __future__ import annotations

from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord, ValidationResult


def validate_price_daily(records: list[NormalizedRecord]) -> list[ValidationResult]:
    """Run basic OHLCV checks on normalized price_daily rows."""

    issues: list[ValidationResult] = []
    for record in records:
        if record.table_name != "price_daily":
            continue

        row = record.row
        key = f"{row.get('instrument_id')}:{row.get('trade_date')}"
        open_price = _decimal(row.get("open_price"))
        high_price = _decimal(row.get("high_price"))
        low_price = _decimal(row.get("low_price"))
        close_price = _decimal(row.get("close_price"))
        volume = row.get("volume")

        if None in (open_price, high_price, low_price, close_price):
            issues.append(_issue("missing_ohlc", key, "Missing one or more OHLC values."))
            continue

        assert open_price is not None
        assert high_price is not None
        assert low_price is not None
        assert close_price is not None

        if high_price < max(open_price, low_price, close_price):
            issues.append(_issue("high_below_ohlc", key, "High price is below an OHLC value."))
        if low_price > min(open_price, high_price, close_price):
            issues.append(_issue("low_above_ohlc", key, "Low price is above an OHLC value."))
        if volume is not None and int(volume) < 0:
            issues.append(_issue("negative_volume", key, "Volume is negative."))

    return issues


def _issue(rule_name: str, key: str, message: str) -> ValidationResult:
    return ValidationResult(
        ok=False,
        rule_name=rule_name,
        message=f"{key}: {message}",
        severity="high",
    )


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
