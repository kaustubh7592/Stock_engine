"""Benchmark and peer-relative feature family."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.features.common import date_or_none, decimal_or_none, feature_record

MIN_PEERS = 3


def compute_peer_features(
    price_rows: list[dict[str, object]],
    instrument_rows: list[dict[str, object]],
    as_of_date: date,
) -> list[NormalizedRecord]:
    """Compute sector/industry-relative ranks where enough valid peers exist."""

    peer_groups = {
        str(row["instrument_id"]): _peer_group(row)
        for row in instrument_rows
        if row.get("instrument_id")
    }
    grouped_prices: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in price_rows:
        instrument_id = _text(row.get("instrument_id"))
        trade_date = date_or_none(row.get("trade_date"))
        if not instrument_id or trade_date is None or trade_date > as_of_date:
            continue
        grouped_prices[instrument_id].append(row)

    metrics_by_group: dict[str, list[dict[str, object]]] = defaultdict(list)
    for instrument_id, rows in grouped_prices.items():
        rows.sort(key=lambda row: date_or_none(row.get("trade_date")) or date.min)
        closes = [decimal_or_none(row.get("close_price")) for row in rows]
        closes = [value for value in closes if value is not None]
        latest = rows[-1]
        peer_group = peer_groups.get(instrument_id, "unknown")
        metrics_by_group[peer_group].append(
            {
                "instrument_id": instrument_id,
                "return_5d": _return_pct(closes[-6], closes[-1]) if len(closes) >= 6 else None,
                "return_20d": _return_pct(closes[-21], closes[-1]) if len(closes) >= 21 else None,
                "liquidity": decimal_or_none(latest.get("traded_value"))
                or decimal_or_none(latest.get("volume")),
                "delivery_pct": decimal_or_none(latest.get("deliverable_pct")),
                "available_at": latest.get("available_at"),
            }
        )

    output = []
    for peer_group, rows in metrics_by_group.items():
        output.extend(
            _rank_feature_rows(
                peer_group, rows, "return_5d", "peer_return_5d_rank_pct", as_of_date
            )
        )
        output.extend(
            _rank_feature_rows(
                peer_group, rows, "return_20d", "peer_return_20d_rank_pct", as_of_date
            )
        )
        output.extend(
            _rank_feature_rows(
                peer_group, rows, "liquidity", "peer_liquidity_rank_pct", as_of_date
            )
        )
        output.extend(
            _rank_feature_rows(
                peer_group,
                rows,
                "delivery_pct",
                "peer_delivery_pct_rank_pct",
                as_of_date,
            )
        )
    return output


def _peer_group(row: dict[str, object]) -> str:
    return _text(row.get("sector_name")) or _text(row.get("industry_name")) or "unknown"


def _rank_feature_rows(
    peer_group: str,
    rows: list[dict[str, object]],
    metric_name: str,
    feature_name: str,
    as_of_date: date,
) -> list[NormalizedRecord]:
    covered_rows = [row for row in rows if row.get(metric_name) is not None]
    enough_peers = peer_group != "unknown" and len(covered_rows) >= MIN_PEERS
    rank_by_instrument = _rank_pct(covered_rows, metric_name) if enough_peers else {}
    output = []
    for row in rows:
        instrument_id = str(row["instrument_id"])
        output.append(
            feature_record(
                instrument_id=instrument_id,
                as_of_date=as_of_date,
                horizon="medium",
                feature_family="peer",
                feature_name=feature_name,
                value_num=rank_by_instrument.get(instrument_id),
                coverage_flag=instrument_id in rank_by_instrument,
                source="price_daily+instruments",
                available_at=row.get("available_at"),
            )
        )
    return output


def _rank_pct(rows: list[dict[str, object]], metric_name: str) -> dict[str, Decimal]:
    sorted_rows = sorted(rows, key=lambda row: row[metric_name])
    if len(sorted_rows) < 2:
        return {}
    denominator = Decimal(len(sorted_rows) - 1)
    return {
        str(row["instrument_id"]): Decimal(index) / denominator
        for index, row in enumerate(sorted_rows)
    }


def _return_pct(previous: Decimal | None, current: Decimal | None) -> Decimal | None:
    if previous is None or current is None or previous == 0:
        return None
    return (current / previous - Decimal("1")) * Decimal("100")


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
