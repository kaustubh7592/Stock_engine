"""Shared feature-snapshot helpers."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from statistics import mean, pstdev
from typing import Any

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now


def feature_record(
    *,
    instrument_id: str,
    as_of_date: date,
    horizon: str,
    feature_family: str,
    feature_name: str,
    value_num: object,
    coverage_flag: bool,
    source: str,
    source_url: str | None = None,
    available_at: object | None = None,
    document_hash: str | None = None,
    parser_version: str | None = None,
) -> NormalizedRecord:
    """Build a canonical feature_snapshots row."""

    now = utc_now()
    value = decimal_or_none(value_num)
    return NormalizedRecord(
        table_name="feature_snapshots",
        row={
            "feature_snapshot_id": stable_id(
                "feature",
                instrument_id,
                as_of_date,
                horizon,
                feature_family,
                feature_name,
            ),
            "instrument_id": instrument_id,
            "as_of_date": as_of_date,
            "horizon": horizon,
            "feature_family": feature_family,
            "feature_name": feature_name,
            "value_num": value,
            "zscore": None,
            "rank_pct": None,
            "coverage_flag": coverage_flag,
            "source": source,
            "source_url": source_url,
            "retrieved_at": now,
            "available_at": available_at or now,
            "document_hash": document_hash,
            "parser_version": parser_version,
            "restated_flag": False,
            "created_at": now,
            "updated_at": now,
        },
    )


def add_cross_section_stats(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    """Add peer z-score and percentile rank where at least two covered values exist."""

    grouped: dict[tuple[object, ...], list[NormalizedRecord]] = defaultdict(list)
    for record in records:
        row = record.row
        if row.get("coverage_flag") is not True or row.get("value_num") is None:
            continue
        key = (
            row.get("as_of_date"),
            row.get("horizon"),
            row.get("feature_family"),
            row.get("feature_name"),
        )
        grouped[key].append(record)

    for rows in grouped.values():
        values = [float(row.row["value_num"]) for row in rows]
        if len(values) < 2:
            continue
        avg = mean(values)
        std = pstdev(values)
        sorted_rows = sorted(rows, key=lambda item: float(item.row["value_num"]))
        for index, record in enumerate(sorted_rows):
            value = float(record.row["value_num"])
            record.row["rank_pct"] = _decimal(index / (len(sorted_rows) - 1))
            if std:
                record.row["zscore"] = _decimal((value - avg) / std)
    return records


def decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def date_or_none(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def latest_available_at(rows: list[dict[str, Any]]) -> object | None:
    values = [row.get("available_at") for row in rows if row.get("available_at") is not None]
    return max(values) if values else None


def _decimal(value: float) -> Decimal:
    return Decimal(str(round(value, 6)))

