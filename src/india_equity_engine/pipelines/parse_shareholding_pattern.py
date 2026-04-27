"""Build canonical shareholding pattern rows from parsed XBRL facts."""

from __future__ import annotations

from collections import Counter

import duckdb

from india_equity_engine.connectors.xbrl.shareholding import (
    map_shareholding_pattern,
    shareholding_fact_from_row,
)
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def parse_shareholding_pattern(settings: Settings, limit: int = 5000) -> JobRunResult:
    """Parse canonical shareholding rows from financial_facts."""

    settings.ensure_runtime_dirs()
    rows = _load_candidate_fact_rows(settings, limit)
    if not rows:
        return JobRunResult(
            job_name="parse_shareholding_pattern",
            status="failed",
            warnings=["No financial facts found. Run iee parse-financial-facts first."],
        )

    facts = [fact for row in rows if (fact := shareholding_fact_from_row(row)) is not None]
    records = map_shareholding_pattern(facts)
    if not records:
        return JobRunResult(
            job_name="parse_shareholding_pattern",
            status="failed",
            records_in=len(rows),
            warnings=[
                "No shareholding facts matched the current concept mapping. "
                "Download shareholding XBRL filings and rerun financial fact parsing."
            ],
        )

    records = _dedupe_records(records)
    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in records)
    return JobRunResult(
        job_name="parse_shareholding_pattern",
        status="success",
        records_in=len(rows),
        records_out=len(records),
        outputs={
            "tables": dict(counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_candidate_fact_rows(settings: Settings, limit: int) -> list[dict[str, object]]:
    if not settings.duckdb_path.exists() or limit < 1:
        return []

    query = """
        select
            instrument_id,
            filing_id,
            concept_name,
            taxonomy_concept,
            period_end,
            consolidated_flag,
            unit,
            value_num,
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
        from financial_facts
        where concept_name is not null
          and (
            contains(lower(concept_name), 'share')
            or contains(lower(concept_name), 'promoter')
            or contains(lower(concept_name), 'public')
            or contains(lower(concept_name), 'fii')
            or contains(lower(concept_name), 'dii')
            or contains(lower(concept_name), 'institution')
            or contains(lower(concept_name), 'retail')
            or contains(lower(taxonomy_concept), 'share')
            or contains(lower(taxonomy_concept), 'promoter')
            or contains(lower(taxonomy_concept), 'public')
            or contains(lower(taxonomy_concept), 'institution')
          )
        order by available_at desc nulls last, filing_id, concept_name
        limit ?
    """
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            columns = [column[0] for column in con.execute(query, [limit]).description]
            rows = con.fetchall()
    except duckdb.Error:
        return []

    return [dict(zip(columns, row, strict=True)) for row in rows]


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (
            record.row.get("instrument_id"),
            record.row.get("period_end"),
            record.row.get("consolidated_flag"),
        )
        deduped[key] = record
    return list(deduped.values())
