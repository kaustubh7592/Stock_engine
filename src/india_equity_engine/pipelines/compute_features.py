"""Compute deterministic Stage A feature snapshots."""

from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.features.common import add_cross_section_stats, date_or_none
from india_equity_engine.features.derivatives import compute_derivatives_features
from india_equity_engine.features.events import compute_event_features
from india_equity_engine.features.fundamentals import compute_fundamental_features
from india_equity_engine.features.governance import compute_governance_features
from india_equity_engine.features.macro import compute_macro_features
from india_equity_engine.features.peer import compute_peer_features
from india_equity_engine.features.technical import compute_technical_features
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore

DEFAULT_FEATURE_FAMILIES = (
    "technical",
    "governance",
    "fundamental",
    "macro",
    "derivatives",
    "event",
    "peer",
)


def compute_features(
    settings: Settings,
    *,
    as_of_date: date | None = None,
    families: tuple[str, ...] = DEFAULT_FEATURE_FAMILIES,
) -> JobRunResult:
    """Compute feature_snapshots for the requested feature families."""

    settings.ensure_runtime_dirs()
    requested_families = _normalize_families(families)
    if not requested_families:
        return JobRunResult(
            job_name="compute_features",
            status="failed",
            warnings=["No supported feature families were requested."],
        )

    resolved_as_of_date = as_of_date or _infer_as_of_date(settings, requested_families)
    if resolved_as_of_date is None:
        return JobRunResult(
            job_name="compute_features",
            status="failed",
            warnings=["No source rows found for feature computation."],
        )

    warnings = []
    records: list[NormalizedRecord] = []
    records_in = 0
    if "technical" in requested_families:
        price_rows = _query_rows(settings, _price_daily_query(settings), [resolved_as_of_date])
        records_in += len(price_rows)
        if price_rows:
            records.extend(compute_technical_features(price_rows, resolved_as_of_date))
        else:
            warnings.append("No price_daily rows found for technical features.")

    if "governance" in requested_families:
        governance_rows = _query_rows(
            settings, _governance_events_query(settings), [resolved_as_of_date]
        )
        pledge_rows = _query_rows(
            settings, _pledge_disclosures_query(settings), [resolved_as_of_date]
        )
        insider_rows = _query_rows(
            settings, _insider_trades_query(settings), [resolved_as_of_date]
        )
        records_in += len(governance_rows) + len(pledge_rows) + len(insider_rows)
        if governance_rows or pledge_rows or insider_rows:
            records.extend(
                compute_governance_features(
                    governance_rows,
                    pledge_rows,
                    insider_rows,
                    resolved_as_of_date,
                )
            )
        else:
            warnings.append("No governance, pledge, or insider rows found for governance features.")

    if "fundamental" in requested_families:
        shareholding_rows = _query_rows(
            settings, _shareholding_query(settings), [resolved_as_of_date]
        )
        financial_fact_rows = _query_rows(
            settings, _financial_facts_query(settings), [resolved_as_of_date]
        )
        records_in += len(shareholding_rows) + len(financial_fact_rows)
        if shareholding_rows or financial_fact_rows:
            records.extend(
                compute_fundamental_features(
                    shareholding_rows,
                    financial_fact_rows,
                    resolved_as_of_date,
                )
            )
        else:
            warnings.append("No shareholding_pattern or financial_facts rows found.")

    if "macro" in requested_families:
        macro_rows = _query_rows(settings, _macro_series_query(settings), [resolved_as_of_date])
        market_flow_rows = _query_rows(
            settings, _market_flows_query(settings), [resolved_as_of_date]
        )
        instrument_rows = _query_rows(
            settings, _instrument_ids_query(settings), [resolved_as_of_date]
        )
        instrument_ids = [
            str(row["instrument_id"]) for row in instrument_rows if row.get("instrument_id")
        ]
        records_in += len(macro_rows) + len(market_flow_rows)
        if (macro_rows or market_flow_rows) and instrument_ids:
            records.extend(
                compute_macro_features(
                    macro_rows,
                    market_flow_rows,
                    instrument_ids,
                    resolved_as_of_date,
                )
            )
        elif not macro_rows and not market_flow_rows:
            warnings.append("No macro_series or market_flows rows found for macro features.")
        else:
            warnings.append("No instrument universe found for macro feature replication.")

    if "derivatives" in requested_families:
        derivatives_rows = _query_rows(
            settings, _derivatives_eod_query(settings), [resolved_as_of_date]
        )
        records_in += len(derivatives_rows)
        if derivatives_rows:
            records.extend(compute_derivatives_features(derivatives_rows, resolved_as_of_date))
        else:
            warnings.append("No derivatives_eod rows found for derivatives features.")

    if "event" in requested_families:
        event_signal_rows = _query_rows(
            settings, _event_signals_query(settings), [resolved_as_of_date]
        )
        instrument_rows = _query_rows(settings, _instrument_sector_query(settings), [])
        records_in += len(event_signal_rows)
        if event_signal_rows and instrument_rows:
            records.extend(
                compute_event_features(event_signal_rows, instrument_rows, resolved_as_of_date)
            )
        elif not event_signal_rows:
            warnings.append("No event_signals rows found for event features.")
        else:
            warnings.append("No instruments with sector metadata found for event features.")

    if "peer" in requested_families:
        peer_price_rows = _query_rows(settings, _price_daily_query(settings), [resolved_as_of_date])
        instrument_rows = _query_rows(settings, _instrument_sector_query(settings), [])
        records_in += len(peer_price_rows)
        if peer_price_rows and instrument_rows:
            records.extend(
                compute_peer_features(peer_price_rows, instrument_rows, resolved_as_of_date)
            )
        elif not peer_price_rows:
            warnings.append("No price_daily rows found for peer features.")
        else:
            warnings.append("No instruments with sector metadata found for peer features.")

    records = _dedupe_records(add_cross_section_stats(records))
    if not records:
        return JobRunResult(
            job_name="compute_features",
            status="failed",
            records_in=records_in,
            warnings=warnings or ["No feature_snapshots records were produced."],
        )

    write_results = ParquetStore(settings.gold_root).write_current_records(records)
    duckdb_warnings = _refresh_duckdb_views(settings, write_results)
    warnings.extend(duckdb_warnings)

    counts = Counter(record.table_name for record in records)
    family_counts = Counter(record.row.get("feature_family") for record in records)
    feature_counts = Counter(record.row.get("feature_name") for record in records)
    return JobRunResult(
        job_name="compute_features",
        status="success",
        records_in=records_in,
        records_out=len(records),
        warnings=warnings,
        outputs={
            "as_of_date": resolved_as_of_date.isoformat(),
            "tables": dict(counts),
            "feature_families": dict(family_counts),
            "features": dict(feature_counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _normalize_families(families: tuple[str, ...]) -> tuple[str, ...]:
    supported = set(DEFAULT_FEATURE_FAMILIES)
    output = []
    for family in families:
        normalized = family.strip().lower()
        if normalized in supported and normalized not in output:
            output.append(normalized)
    return tuple(output)


def _infer_as_of_date(settings: Settings, families: tuple[str, ...]) -> date | None:
    candidates = []
    if "technical" in families:
        candidates.extend(_max_dates(settings, "price_daily", "trade_date"))
    if "governance" in families:
        candidates.extend(_max_dates(settings, "governance_events", "event_date"))
        candidates.extend(_max_dates(settings, "pledge_disclosures", "period_end"))
        candidates.extend(_max_dates(settings, "insider_trades", "transaction_date"))
    if "fundamental" in families:
        candidates.extend(_max_dates(settings, "shareholding_pattern", "period_end"))
        candidates.extend(_max_dates(settings, "financial_facts", "period_end"))
    if "macro" in families:
        candidates.extend(_max_dates(settings, "macro_series", "observation_date"))
    if "derivatives" in families:
        candidates.extend(_max_dates(settings, "derivatives_eod", "trade_date"))
    if "event" in families:
        candidates.extend(_max_dates(settings, "event_signals", "event_date"))
    if "peer" in families:
        candidates.extend(_max_dates(settings, "price_daily", "trade_date"))
    dates = [value for value in candidates if value is not None]
    return max(dates) if dates else None


def _max_dates(settings: Settings, table_name: str, column_name: str) -> list[date | None]:
    query = f"select max({column_name}) as max_date from {_source(settings, table_name)}"
    rows = _query_rows(settings, query, [])
    return [date_or_none(row.get("max_date")) for row in rows]


def _query_rows(
    settings: Settings,
    query: str,
    params: list[object],
) -> list[dict[str, object]]:
    try:
        if settings.duckdb_path.exists():
            con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        else:
            con = duckdb.connect()
        with con:
            result = con.execute(query, params)
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _source(settings: Settings, table_name: str, layer: str = "silver") -> str:
    root = settings.gold_root if layer == "gold" else settings.silver_root
    current = root / table_name / "current.parquet"
    if current.exists():
        return _read_parquet_expr(current)
    table_dir = current.parent
    if table_dir.exists() and any(table_dir.glob("*.parquet")):
        return _read_parquet_expr(table_dir / "*.parquet")
    return table_name


def _read_parquet_expr(path: Path) -> str:
    escaped = str(path).replace("'", "''")
    return f"read_parquet('{escaped}', union_by_name=true)"


def _refresh_duckdb_views(settings: Settings, write_results: list[object]) -> list[str]:
    warnings = []
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.gold_root / result.table_name / "current.parquet"
        try:
            duckdb_store.refresh_parquet_view(result.table_name, table_path)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")
    return warnings


def _price_daily_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            traded_value,
            deliverable_qty,
            deliverable_pct,
            available_at
        from {_source(settings, "price_daily")}
        where trade_date <= ?
        order by instrument_id, trade_date
    """


def _governance_events_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            event_date,
            event_type,
            severity,
            risk_flag,
            available_at
        from {_source(settings, "governance_events")}
        where event_date <= ?
    """


def _pledge_disclosures_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            period_end,
            pledged_pct_promoter_holding,
            pledged_pct_total_equity,
            available_at
        from {_source(settings, "pledge_disclosures")}
        where period_end <= ?
    """


def _insider_trades_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            transaction_date,
            transaction_type,
            value_num,
            available_at
        from {_source(settings, "insider_trades")}
        where transaction_date <= ?
    """


def _shareholding_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            period_end,
            promoter_pct,
            public_pct,
            fii_pct,
            dii_pct,
            retail_pct,
            other_pct,
            share_count,
            available_at
        from {_source(settings, "shareholding_pattern")}
        where period_end <= ?
    """


def _financial_facts_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            concept_name,
            taxonomy_concept,
            period_end,
            consolidated_flag,
            value_num,
            document_hash,
            available_at
        from {_source(settings, "financial_facts")}
        where period_end <= ?
          and value_num is not null
    """


def _macro_series_query(settings: Settings) -> str:
    return f"""
        select
            series_code,
            series_name,
            source_family,
            observation_date,
            value_num,
            unit,
            frequency,
            vintage_date,
            seasonal_adjustment,
            source_url,
            document_hash,
            parser_version,
            available_at
        from {_source(settings, "macro_series")}
        where observation_date <= ?
          and value_num is not null
    """


def _market_flows_query(settings: Settings) -> str:
    return f"""
        select
            trade_date,
            flow_type,
            segment,
            investor_class,
            gross_buy,
            gross_sell,
            net_flow,
            notes,
            available_at
        from {_source(settings, "market_flows")}
        where trade_date <= ?
    """


def _instrument_ids_query(settings: Settings) -> str:
    return f"""
        select distinct instrument_id
        from {_source(settings, "price_daily")}
        where trade_date <= ?
    """


def _instrument_sector_query(settings: Settings) -> str:
    return f"""
        select
            instrument_id,
            sector_name,
            industry_name
        from {_source(settings, "instruments")}
        where instrument_id is not null
    """


def _derivatives_eod_query(settings: Settings) -> str:
    return f"""
        select
            contract_id,
            instrument_id,
            trade_date,
            segment,
            expiry_date,
            strike_price,
            option_type,
            settlement_price,
            open_interest,
            oi_change,
            contract_volume,
            available_at
        from {_source(settings, "derivatives_eod")}
        where trade_date <= ?
    """


def _event_signals_query(settings: Settings) -> str:
    return f"""
        select
            event_signal_id,
            news_id,
            instrument_id,
            sector_name,
            event_date,
            horizon,
            impact_direction,
            impact_score,
            confidence,
            exposure_channel,
            rationale_code,
            available_at
        from {_source(settings, "event_signals")}
        where event_date <= ?
    """


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (
            record.row.get("instrument_id"),
            record.row.get("as_of_date"),
            record.row.get("horizon"),
            record.row.get("feature_family"),
            record.row.get("feature_name"),
        )
        deduped[key] = record
    return list(deduped.values())
