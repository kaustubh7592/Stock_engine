"""Macro feature family built from macro_series."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import (
    date_or_none,
    decimal_or_none,
    feature_record,
    latest_available_at,
)

MACRO_SERIES_FEATURES = {
    "repo": "macro_policy_repo_rate_latest",
    "reverse repo": "macro_reverse_repo_rate_latest",
    "cpi": "macro_cpi_latest",
    "iip": "macro_iip_latest",
    "gdp": "macro_gdp_latest",
}


def compute_macro_features(
    macro_rows: list[dict[str, object]],
    market_flow_rows: list[dict[str, object]],
    instrument_ids: list[str],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Replicate broad macro and flow-regime features to each instrument in the universe."""

    latest_by_feature: dict[str, dict[str, object]] = {}
    for row in macro_rows:
        observation_date = date_or_none(row.get("observation_date"))
        if observation_date is None or observation_date > as_of_date:
            continue
        feature_name = _feature_name_for(row)
        if feature_name is None or row.get("value_num") is None:
            continue
        current = latest_by_feature.get(feature_name)
        current_date = date_or_none(current.get("observation_date")) if current else None
        if current is None or current_date is None or observation_date >= current_date:
            latest_by_feature[feature_name] = row

    output = []
    for instrument_id in sorted(set(instrument_ids)):
        for feature_name, row in sorted(latest_by_feature.items()):
            output.append(
                feature_record(
                    instrument_id=instrument_id,
                    as_of_date=as_of_date,
                    horizon="medium",
                    feature_family="macro",
                    feature_name=feature_name,
                    value_num=row.get("value_num"),
                    coverage_flag=True,
                    source="macro_series",
                    source_url=_text(row.get("source_url")),
                    available_at=latest_available_at([row]),
                    document_hash=_text(row.get("document_hash")),
                    parser_version=_text(row.get("parser_version")),
                )
            )
        for feature_name, value, available_at in _market_flow_features(
            market_flow_rows,
            as_of_date,
        ):
            output.append(
                feature_record(
                    instrument_id=instrument_id,
                    as_of_date=as_of_date,
                    horizon="medium",
                    feature_family="macro",
                    feature_name=feature_name,
                    value_num=value,
                    coverage_flag=value is not None,
                    source="market_flows",
                    available_at=available_at,
                )
            )
    return output


def _feature_name_for(row: dict[str, object]) -> str | None:
    text = f"{row.get('series_name') or ''} {row.get('series_code') or ''}".lower()
    for token, feature_name in MACRO_SERIES_FEATURES.items():
        if token in text:
            return feature_name
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _market_flow_features(
    rows: list[dict[str, object]],
    as_of_date: date,
) -> list[tuple[str, Decimal | None, object | None]]:
    valid_rows = []
    for row in rows:
        trade_date = date_or_none(row.get("trade_date"))
        if trade_date is None or trade_date > as_of_date:
            continue
        if str(row.get("investor_class") or "").upper() != "FPI":
            continue
        valid_rows.append(row)
    if not valid_rows:
        return []

    latest_date = max(date_or_none(row.get("trade_date")) or date.min for row in valid_rows)
    latest_rows = [row for row in valid_rows if date_or_none(row.get("trade_date")) == latest_date]
    trade_dates = {
        date_or_none(row.get("trade_date"))
        for row in valid_rows
        if date_or_none(row.get("trade_date"))
    }
    last_five_dates = sorted(trade_dates, reverse=True)[:5]
    last_five_rows = [
        row for row in valid_rows if date_or_none(row.get("trade_date")) in set(last_five_dates)
    ]
    return [
        (
            "macro_fpi_total_net_flow_latest",
            _sum_net_flow(latest_rows),
            latest_available_at(latest_rows),
        ),
        (
            "macro_fpi_equity_net_flow_latest",
            _sum_net_flow([row for row in latest_rows if _is_equity_segment(row)]),
            latest_available_at(latest_rows),
        ),
        (
            "macro_fpi_total_net_flow_5d",
            _sum_net_flow(last_five_rows),
            latest_available_at(last_five_rows),
        ),
    ]


def _sum_net_flow(rows: list[dict[str, object]]) -> Decimal | None:
    values = [decimal_or_none(row.get("net_flow")) for row in rows]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return sum(values, Decimal("0"))


def _is_equity_segment(row: dict[str, object]) -> bool:
    return "equity" in str(row.get("segment") or "").lower()
