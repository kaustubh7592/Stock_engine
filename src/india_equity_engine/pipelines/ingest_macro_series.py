"""Ingest official macro/rate series into macro_series."""

from __future__ import annotations

from collections import Counter

import duckdb

from india_equity_engine.connectors.mospi.latest_releases import MoSPILatestReleasesConnector
from india_equity_engine.connectors.rbi.current_rates import RBICurrentRatesConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_macro_series(settings: Settings) -> JobRunResult:
    """Ingest official RBI and MoSPI series into the canonical macro_series table."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    verify = build_http_verify(settings)
    raw_store = RawArtifactStore(settings.raw_root, settings.parser_version)
    connectors = [
        RBICurrentRatesConnector(
            source=registry.get("S14"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
            trust_env=settings.http_trust_env,
        ),
        MoSPILatestReleasesConnector(
            source=registry.get("S16"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
            trust_env=settings.http_trust_env,
        ),
    ]

    warnings = []
    raw_records = []
    records: list[NormalizedRecord] = []
    rows_in_by_source = {}
    for connector in connectors:
        connector_name = connector.source.family
        source_object = connector.discover()[0]
        try:
            artifact = connector.download(source_object)
        except EngineError as exc:
            warnings.append(f"{connector_name}: {exc}")
            continue

        validation = connector.validate(artifact)
        if not validation.ok:
            warnings.append(f"{connector_name}: {validation.message}")
            continue

        raw_record = raw_store.store(artifact)
        raw_records.append(raw_record)
        parsed = connector.parse(artifact)
        rows_in_by_source[connector_name] = len(parsed)
        if not parsed:
            warnings.append(f"{connector_name}: no numeric macro observations parsed.")
            continue
        source_records = connector.normalize(parsed, artifact)
        records.extend(_with_lineage(source_records, raw_record.sha256, settings.parser_version))

    records = _dedupe_records(records)
    if not records:
        return JobRunResult(
            job_name="ingest_macro_series",
            status="failed",
            records_in=sum(rows_in_by_source.values()),
            warnings=warnings or ["No macro_series records were produced."],
        )

    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        try:
            duckdb_store.refresh_parquet_view(result.table_name, table_path)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")

    counts = Counter(record.table_name for record in records)
    return JobRunResult(
        job_name="ingest_macro_series",
        status="partial_success" if warnings else "success",
        records_in=sum(rows_in_by_source.values()),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "raw_artifacts": [str(record.path) for record in raw_records],
            "document_hashes": {
                record.logical_name: record.sha256 for record in raw_records
            },
            "tables": dict(counts),
            "rows_in_by_source": rows_in_by_source,
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
            record.row["series_code"],
            record.row["observation_date"],
            record.row["vintage_date"],
        )
        deduped[key] = record
    return list(deduped.values())
