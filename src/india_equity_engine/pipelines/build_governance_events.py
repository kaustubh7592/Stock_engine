"""Build normalized governance events from canonical disclosure tables."""

from __future__ import annotations

from collections import Counter

import duckdb

from india_equity_engine.connectors.events.governance import (
    governance_source_row_from_dict,
    map_governance_events,
)
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def build_governance_events(settings: Settings, limit: int = 10000) -> JobRunResult:
    """Build canonical governance events from local disclosures and filing rows."""

    settings.ensure_runtime_dirs()
    rows = _load_candidate_rows(settings, limit)
    if not rows:
        return JobRunResult(
            job_name="build_governance_events",
            status="failed",
            warnings=[
                "No disclosure rows found. Run ingest-disclosures, ingest-filings, "
                "parse-pledge-disclosures, or ingest-insider-trades first."
            ],
        )

    source_rows = [governance_source_row_from_dict(row) for row in rows]
    records = map_governance_events(source_rows)
    if not records:
        return JobRunResult(
            job_name="build_governance_events",
            status="failed",
            records_in=len(rows),
            warnings=["No rows matched the governance event rule set."],
        )

    records = _dedupe_records(records)
    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in records)
    event_counts = Counter(record.row.get("event_type") for record in records)
    return JobRunResult(
        job_name="build_governance_events",
        status="success",
        records_in=len(rows),
        records_out=len(records),
        outputs={
            "tables": dict(counts),
            "event_types": dict(event_counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_candidate_rows(settings: Settings, limit: int) -> list[dict[str, object]]:
    if not settings.duckdb_path.exists() or limit < 1:
        return []

    rows = []
    remaining = limit
    for query in (
        _announcement_query(),
        _filing_query(),
        _pledge_query(),
        _insider_trade_query(),
    ):
        if remaining < 1:
            break
        batch = _query_rows(settings, query, remaining)
        rows.extend(batch)
        remaining = limit - len(rows)
    return rows


def _query_rows(settings: Settings, query: str, limit: int) -> list[dict[str, object]]:
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            result = con.execute(query, [limit])
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _announcement_query() -> str:
    return """
        select
            'corporate_announcements' as source_table,
            announcement_id as source_id,
            instrument_id,
            cast(announced_at as date) as event_date,
            headline,
            coalesce(
                cast(summary_text as varchar),
                cast(sub_category as varchar),
                cast(headline as varchar)
            ) as event_text,
            null as related_filing_id,
            source,
            coalesce(cast(attachment_url as varchar), cast(source_url as varchar)) as source_url,
            retrieved_at,
            available_at,
            as_of_date,
            document_hash,
            parser_version,
            restated_flag,
            created_at,
            updated_at
        from corporate_announcements
        where headline is not null
           or summary_text is not null
           or sub_category is not null
        order by available_at desc nulls last
        limit ?
    """


def _filing_query() -> str:
    return """
        select
            'filings' as source_table,
            filing_id as source_id,
            instrument_id,
            filing_date as event_date,
            filing_subtype as headline,
            concat_ws(
                '; ',
                filing_family,
                filing_subtype,
                document_type,
                document_url
            ) as event_text,
            filing_id as related_filing_id,
            source,
            coalesce(document_url, source_url) as source_url,
            retrieved_at,
            available_at,
            as_of_date,
            document_hash,
            parser_version,
            restated_flag,
            created_at,
            updated_at
        from filings
        where filing_subtype is not null
           or filing_family in ('pledge', 'insider_or_trading')
        order by available_at desc nulls last
        limit ?
    """


def _pledge_query() -> str:
    return """
        select
            'pledge_disclosures' as source_table,
            pledge_id as source_id,
            instrument_id,
            period_end as event_date,
            'Promoter pledge disclosure' as headline,
            concat_ws(
                '; ',
                concat('promoter_shares=', promoter_shares),
                concat('pledged_shares=', pledged_shares),
                concat('pledged_pct_promoter_holding=', pledged_pct_promoter_holding),
                concat('pledged_pct_total_equity=', pledged_pct_total_equity),
                concat('flag=', release_or_creation_flag)
            ) as event_text,
            filing_id as related_filing_id,
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
        from pledge_disclosures
        order by available_at desc nulls last
        limit ?
    """


def _insider_trade_query() -> str:
    return """
        select
            'insider_trades' as source_table,
            insider_trade_id as source_id,
            instrument_id,
            transaction_date as event_date,
            concat_ws(' ', insider_name, transaction_type) as headline,
            concat_ws(
                '; ',
                concat('category=', insider_category),
                concat('type=', transaction_type),
                concat('quantity=', quantity),
                concat('value=', value_num),
                concat('post_holding=', post_holding)
            ) as event_text,
            filing_id as related_filing_id,
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
        from insider_trades
        order by available_at desc nulls last
        limit ?
    """


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (
            record.row.get("instrument_id") or record.row["governance_event_id"],
            record.row.get("event_date"),
            record.row.get("event_type"),
            _normalize_headline(record.row.get("headline")),
        )
        deduped[key] = record
    return list(deduped.values())


def _normalize_headline(value: object) -> str:
    text = "" if value is None else str(value)
    return "".join(char.lower() for char in text if char.isalnum())
