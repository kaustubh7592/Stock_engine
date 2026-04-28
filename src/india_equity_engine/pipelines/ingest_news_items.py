"""Ingest official and open news/event items into news_items."""

from __future__ import annotations

from collections import Counter

from india_equity_engine.connectors.news.gdelt import DEFAULT_GDELT_QUERY, GDELTDocConnector
from india_equity_engine.connectors.news.official_pages import OfficialPageNewsConnector
from india_equity_engine.connectors.news.rss import (
    PIB_RSS_FEEDS,
    RBI_RSS_FEEDS,
    OfficialRSSNewsConnector,
)
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_news_items(
    settings: Settings,
    *,
    include_gdelt: bool = True,
    include_official_pages: bool = True,
    gdelt_query: str = DEFAULT_GDELT_QUERY,
    gdelt_max_records: int = 50,
) -> JobRunResult:
    """Ingest Step 7 event/news sources into canonical news_items."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    verify = build_http_verify(settings)
    connectors = [
        OfficialRSSNewsConnector(
            source=registry.get("S15"),
            feeds=RBI_RSS_FEEDS,
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
            trust_env=settings.http_trust_env,
        ),
        OfficialRSSNewsConnector(
            source=registry.get("S18"),
            feeds=PIB_RSS_FEEDS,
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
            trust_env=settings.http_trust_env,
        ),
    ]
    if include_official_pages:
        connectors.extend(
            [
                OfficialPageNewsConnector(
                    source=registry.get("S19"),
                    source_name="India Budget",
                    source_class="government",
                    user_agent=settings.user_agent,
                    timeout_seconds=settings.http_timeout_seconds,
                    verify=verify,
                    trust_env=settings.http_trust_env,
                ),
                OfficialPageNewsConnector(
                    source=registry.get("S20"),
                    source_name="Election Commission of India",
                    source_class="government",
                    user_agent=settings.user_agent,
                    timeout_seconds=settings.http_timeout_seconds,
                    verify=verify,
                    trust_env=settings.http_trust_env,
                ),
            ]
        )
    if include_gdelt:
        connectors.append(
            GDELTDocConnector(
                source=registry.get("S21"),
                user_agent=settings.user_agent,
                query=gdelt_query,
                max_records=gdelt_max_records,
                timeout_seconds=settings.http_timeout_seconds,
                verify=verify,
                trust_env=settings.http_trust_env,
            )
        )

    all_records: list[NormalizedRecord] = []
    raw_records = []
    warnings = []
    raw_store = RawArtifactStore(settings.raw_root, settings.parser_version)
    for connector in connectors:
        for source_object in connector.discover():
            try:
                artifact = connector.download(source_object)
                validation = connector.validate(artifact)
            except EngineError as exc:
                warnings.append(str(exc))
                continue
            if not validation.ok:
                warnings.append(validation.message)
                continue
            raw_record = raw_store.store(artifact)
            raw_records.append(raw_record)
            parsed = connector.parse(artifact)
            records = connector.normalize(parsed, artifact)
            all_records.extend(_with_lineage(records, raw_record.sha256, settings.parser_version))

    records = _dedupe_records(all_records)
    if not records:
        return JobRunResult(
            job_name="ingest_news_items",
            status="failed",
            warnings=warnings or ["No news_items records were produced."],
        )

    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in records)
    type_counts = Counter(record.row.get("event_type") for record in records)
    source_counts = Counter(record.row.get("source") for record in records)
    return JobRunResult(
        job_name="ingest_news_items",
        status="success",
        records_in=len(all_records),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "raw_artifacts": [str(raw_record.path) for raw_record in raw_records],
            "document_hashes": {
                raw_record.logical_name: raw_record.sha256 for raw_record in raw_records
            },
            "tables": dict(counts),
            "event_types": dict(type_counts),
            "sources": dict(source_counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _with_lineage(
    records: list[NormalizedRecord],
    document_hash: str,
    parser_version: str,
) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = dict(record.row)
        row["document_hash"] = document_hash
        row["parser_version"] = parser_version
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (
            record.row.get("news_id"),
            _normalize(record.row.get("headline")),
            record.row.get("url"),
        )
        deduped[key] = record
    return list(deduped.values())


def _normalize(value: object) -> str:
    text = "" if value is None else str(value)
    return "".join(char.lower() for char in text if char.isalnum())
