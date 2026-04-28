"""Core fundamental and ownership feature family."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import date_or_none, decimal_or_none, feature_record

SHAREHOLDING_FIELDS = (
    "promoter_pct",
    "public_pct",
    "fii_pct",
    "dii_pct",
    "retail_pct",
    "other_pct",
    "share_count",
)

CORE_FACT_FEATURES = (
    "revenue",
    "net_profit",
    "total_assets",
    "total_borrowings",
)


def compute_fundamental_features(
    shareholding_rows: list[dict[str, object]],
    financial_fact_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Compute first-pass fundamentals from parsed facts and shareholding rows."""

    output = []
    output.extend(_shareholding_features(shareholding_rows, as_of_date))
    output.extend(_financial_fact_features(financial_fact_rows, as_of_date))
    return output


def _shareholding_features(
    rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        instrument_id = _text(row.get("instrument_id"))
        period_end = date_or_none(row.get("period_end"))
        if not instrument_id or period_end is None or period_end > as_of_date:
            continue
        grouped[instrument_id].append(row)

    output = []
    for instrument_id, instrument_rows in grouped.items():
        instrument_rows.sort(key=lambda item: date_or_none(item.get("period_end")) or date.min)
        latest = instrument_rows[-1]
        previous = instrument_rows[-2] if len(instrument_rows) >= 2 else None
        for field_name in SHAREHOLDING_FIELDS:
            value = _decimal(latest.get(field_name))
            output.append(
                _record(
                    instrument_id,
                    as_of_date,
                    f"shareholding_{field_name}_latest",
                    value,
                    value is not None,
                    "shareholding_pattern",
                )
            )
        for field_name in ("promoter_pct", "fii_pct", "dii_pct", "retail_pct"):
            latest_value = _decimal(latest.get(field_name))
            previous_value = _decimal(previous.get(field_name)) if previous else None
            output.append(
                _record(
                    instrument_id,
                    as_of_date,
                    f"shareholding_{field_name}_change_1p",
                    latest_value - previous_value
                    if latest_value is not None and previous_value is not None
                    else None,
                    latest_value is not None and previous_value is not None,
                    "shareholding_pattern",
                )
            )
    return output


def _financial_fact_features(
    rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        instrument_id = _text(row.get("instrument_id"))
        period_end = date_or_none(row.get("period_end"))
        if not instrument_id or period_end is None or period_end > as_of_date:
            continue
        if _core_feature_for_fact(row) is None:
            continue
        grouped[instrument_id].append(row)

    output = []
    for instrument_id, instrument_rows in grouped.items():
        feature_periods = _latest_feature_values(instrument_rows)
        latest_values = {
            feature_name: values[-1] for feature_name, values in feature_periods.items() if values
        }
        previous_values = {
            feature_name: values[-2]
            for feature_name, values in feature_periods.items()
            if len(values) >= 2
        }
        for feature_name in CORE_FACT_FEATURES:
            value = latest_values.get(feature_name, {}).get("value_num")
            output.append(
                _record(
                    instrument_id,
                    as_of_date,
                    f"fundamental_{feature_name}_latest",
                    value,
                    value is not None,
                    "financial_facts",
                )
            )
        output.append(
            _record(
                instrument_id,
                as_of_date,
                "fundamental_debt_to_assets",
                _ratio(
                    latest_values.get("total_borrowings", {}).get("value_num"),
                    latest_values.get("total_assets", {}).get("value_num"),
                ),
                _ratio(
                    latest_values.get("total_borrowings", {}).get("value_num"),
                    latest_values.get("total_assets", {}).get("value_num"),
                )
                is not None,
                "financial_facts",
            )
        )
        for feature_name in ("revenue", "net_profit"):
            output.append(
                _record(
                    instrument_id,
                    as_of_date,
                    f"fundamental_{feature_name}_growth_1p_pct",
                    _growth_pct(
                        previous_values.get(feature_name, {}).get("value_num"),
                        latest_values.get(feature_name, {}).get("value_num"),
                    ),
                    _growth_pct(
                        previous_values.get(feature_name, {}).get("value_num"),
                        latest_values.get(feature_name, {}).get("value_num"),
                    )
                    is not None,
                    "financial_facts",
                )
            )
    return output


def _latest_feature_values(rows: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, dict[date, dict[str, object]]] = defaultdict(dict)
    for row in rows:
        feature_name = _core_feature_for_fact(row)
        period_end = date_or_none(row.get("period_end"))
        value = _decimal(row.get("value_num"))
        if feature_name is None or period_end is None or value is None:
            continue
        current = grouped[feature_name].get(period_end)
        if current is None or _fact_priority(row) >= _fact_priority(current):
            grouped[feature_name][period_end] = {**row, "value_num": value}
    return {
        feature_name: [
            period_rows[period]
            for period in sorted(period_rows)
        ]
        for feature_name, period_rows in grouped.items()
    }


def _core_feature_for_fact(row: dict[str, object]) -> str | None:
    text = _normalize_text(f"{row.get('concept_name') or ''} {row.get('taxonomy_concept') or ''}")
    if _has_any(text, ("revenuefromoperations", "revenuefromsale", "salesrevenue", "revenue")):
        return "revenue"
    if _has_any(text, ("profitlossforperiod", "profitaftertax", "netprofit", "profitloss")):
        return "net_profit"
    if _has_any(text, ("totalborrowings", "borrowings", "debt")):
        return "total_borrowings"
    if _has_any(text, ("totalassets", "assets")):
        return "total_assets"
    return None


def _fact_priority(row: dict[str, object]) -> int:
    priority = 0
    if row.get("consolidated_flag"):
        priority += 10
    if row.get("document_hash"):
        priority += 1
    return priority


def _record(
    instrument_id: str,
    as_of_date: date,
    feature_name: str,
    value: object,
    coverage_flag: bool,
    source: str,
) -> NormalizedRecord:
    return feature_record(
        instrument_id=instrument_id,
        as_of_date=as_of_date,
        horizon="long",
        feature_family="fundamental",
        feature_name=feature_name,
        value_num=value,
        coverage_flag=coverage_flag,
        source=source,
    )


def _ratio(numerator: object, denominator: object) -> Decimal | None:
    num = _decimal(numerator)
    den = _decimal(denominator)
    if num is None or den in (None, Decimal("0")):
        return None
    return num / den


def _growth_pct(previous: object, current: object) -> Decimal | None:
    prev = _decimal(previous)
    curr = _decimal(current)
    if prev is None or curr is None or prev == 0:
        return None
    return (curr / prev - Decimal("1")) * Decimal("100")


def _decimal(value: object) -> Decimal | None:
    return decimal_or_none(value)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_text(value: str) -> str:
    return "".join(char.lower() for char in value if char.isalnum())


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)

