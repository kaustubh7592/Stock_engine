"""Build exposure-aware event_signals from canonical news_items."""

from __future__ import annotations

from collections import Counter

import duckdb

from india_equity_engine.connectors.news.signals import (
    map_event_signals,
    news_signal_source_row_from_dict,
)
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def build_event_signals(settings: Settings, limit: int = 10000) -> JobRunResult:
    """Build sector-level event signals from local news_items."""

    settings.ensure_runtime_dirs()
    rows = _load_candidate_rows(settings, limit)
    if not rows:
        return JobRunResult(
            job_name="build_event_signals",
            status="failed",
            warnings=["No news_items rows found. Run ingest-news-items first."],
        )

    source_rows = [news_signal_source_row_from_dict(row) for row in rows]
    records = _dedupe_records(map_event_signals(source_rows))
    if not records:
        return JobRunResult(
            job_name="build_event_signals",
            status="failed",
            records_in=len(rows),
            warnings=["No rows matched the event signal rule set."],
        )

    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    warnings = []
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        try:
            duckdb_store.refresh_parquet_view(result.table_name, table_path)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")

    counts = Counter(record.table_name for record in records)
    channel_counts = Counter(record.row.get("exposure_channel") for record in records)
    return JobRunResult(
        job_name="build_event_signals",
        status="success",
        records_in=len(rows),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "tables": dict(counts),
            "exposure_channels": dict(channel_counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_candidate_rows(settings: Settings, limit: int) -> list[dict[str, object]]:
    if not settings.duckdb_path.exists() or limit < 1:
        return []
    query = """
        select
            news_id,
            published_at,
            source_name,
            source_class,
            headline,
            url,
            event_type,
            cast(entities_json as varchar) as entities_json,
            source,
            source_url,
            retrieved_at,
            available_at,
            as_of_date,
            document_hash,
            parser_version,
            restated_flag,
            created_at,
            updated_at
        from news_items
        where headline is not null
        order by available_at desc nulls last
        limit ?
    """
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            result = con.execute(query, [limit])
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (
            record.row.get("news_id"),
            record.row.get("sector_name"),
            record.row.get("horizon"),
            record.row.get("exposure_channel"),
            record.row.get("rationale_code"),
        )
        deduped[key] = record
    return list(deduped.values())
