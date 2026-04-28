"""Governance and promoter feature family."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import date_or_none, decimal_or_none, feature_record


def compute_governance_features(
    governance_rows: list[dict[str, object]],
    pledge_rows: list[dict[str, object]],
    insider_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Compute governance, pledge, and insider-activity features."""

    instruments = _instrument_ids(governance_rows, pledge_rows, insider_rows)
    output = []
    for instrument_id in sorted(instruments):
        events = [
            row
            for row in governance_rows
            if _text(row.get("instrument_id")) == instrument_id
            and _date_in_window(row.get("event_date"), as_of_date, 365)
        ]
        pledges = [
            row
            for row in pledge_rows
            if _text(row.get("instrument_id")) == instrument_id
            and _date_before_or_equal(row.get("period_end"), as_of_date)
        ]
        trades = [
            row
            for row in insider_rows
            if _text(row.get("instrument_id")) == instrument_id
            and _date_in_window(row.get("transaction_date"), as_of_date, 180)
        ]
        latest_pledge = _latest_by_date(pledges, "period_end")
        high_count = sum(1 for row in events if str(row.get("severity") or "").lower() == "high")
        risk_count = sum(1 for row in events if bool(row.get("risk_flag")))

        output.extend(
            [
                _record(
                    instrument_id,
                    as_of_date,
                    "governance_risk_event_count_365d",
                    Decimal(risk_count),
                    True,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "governance_high_severity_count_365d",
                    Decimal(high_count),
                    True,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "governance_risk_flag_365d",
                    Decimal("1") if risk_count else Decimal("0"),
                    True,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "promoter_pledged_pct_total_equity_latest",
                    _decimal(latest_pledge.get("pledged_pct_total_equity"))
                    if latest_pledge
                    else None,
                    latest_pledge is not None
                    and _decimal(latest_pledge.get("pledged_pct_total_equity")) is not None,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "promoter_pledged_pct_promoter_holding_latest",
                    _decimal(latest_pledge.get("pledged_pct_promoter_holding"))
                    if latest_pledge
                    else None,
                    latest_pledge is not None
                    and _decimal(latest_pledge.get("pledged_pct_promoter_holding")) is not None,
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "insider_net_value_180d",
                    _insider_net_value(trades) if trades else None,
                    bool(trades),
                ),
                _record(
                    instrument_id,
                    as_of_date,
                    "insider_sell_value_180d",
                    _insider_sell_value(trades) if trades else None,
                    bool(trades),
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
) -> NormalizedRecord:
    return feature_record(
        instrument_id=instrument_id,
        as_of_date=as_of_date,
        horizon="medium",
        feature_family="governance",
        feature_name=feature_name,
        value_num=value,
        coverage_flag=coverage_flag,
        source="governance_feature_engine",
    )


def _instrument_ids(*batches: list[dict[str, object]]) -> set[str]:
    ids = set()
    for batch in batches:
        ids.update(_text(row.get("instrument_id")) for row in batch)
    return {value for value in ids if value}


def _date_in_window(value: object, as_of_date: date, days: int) -> bool:
    observed_date = date_or_none(value)
    if observed_date is None:
        return False
    return as_of_date - timedelta(days=days) <= observed_date <= as_of_date


def _date_before_or_equal(value: object, as_of_date: date) -> bool:
    observed_date = date_or_none(value)
    return observed_date is not None and observed_date <= as_of_date


def _latest_by_date(rows: list[dict[str, object]], key: str) -> dict[str, object] | None:
    if not rows:
        return None
    return max(rows, key=lambda row: date_or_none(row.get(key)) or date.min)


def _insider_net_value(rows: list[dict[str, object]]) -> Decimal:
    total = Decimal("0")
    for row in rows:
        value = _decimal(row.get("value_num")) or Decimal("0")
        transaction_type = str(row.get("transaction_type") or "").lower()
        if any(token in transaction_type for token in ("sell", "sale", "disposal")):
            total -= value
        elif any(token in transaction_type for token in ("buy", "purchase", "acquisition")):
            total += value
    return total


def _insider_sell_value(rows: list[dict[str, object]]) -> Decimal:
    total = Decimal("0")
    for row in rows:
        transaction_type = str(row.get("transaction_type") or "").lower()
        if any(token in transaction_type for token in ("sell", "sale", "disposal")):
            total += _decimal(row.get("value_num")) or Decimal("0")
    return total


def _decimal(value: object) -> Decimal | None:
    return decimal_or_none(value)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
