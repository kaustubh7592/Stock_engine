"""Ingest official market-flow series into market_flows."""

from __future__ import annotations

from collections import Counter

from india_equity_engine.connectors.nsdl.fpi_flows import NSDLFPIFlowsConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_market_flows(settings: Settings) -> JobRunResult:
    """Ingest NSDL daily FPI flows into the canonical market_flows table."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    connector = NSDLFPIFlowsConnector(
        source=registry.get("S17"),
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=build_http_verify(settings),
        trust_env=settings.http_trust_env,
    )
    source_object = connector.discover()[0]
    try:
        artifact = connector.download(source_object)
    except EngineError as exc:
        return JobRunResult(job_name="ingest_market_flows", status="failed", warnings=[str(exc)])

    validation = connector.validate(artifact)
    if not validation.ok:
        return JobRunResult(
            job_name="ingest_market_flows",
            status="failed",
            warnings=[validation.message],
        )

    raw_record = RawArtifactStore(settings.raw_root, settings.parser_version).store(artifact)
    parsed = connector.parse(artifact)
    records = connector.normalize(parsed, artifact)
    records = _with_lineage(records, raw_record.sha256, settings.parser_version)
    records = _dedupe_records(records)
    if not records:
        return JobRunResult(
            job_name="ingest_market_flows",
            status="failed",
            records_in=len(parsed),
            warnings=["No market_flows records were produced."],
        )

    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in records)
    return JobRunResult(
        job_name="ingest_market_flows",
        status="success",
        records_in=len(parsed),
        records_out=len(records),
        outputs={
            "raw_artifacts": [str(raw_record.path)],
            "document_hashes": {raw_record.logical_name: raw_record.sha256},
            "tables": dict(counts),
            "source_url": artifact.source_url,
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
            record.row["trade_date"],
            record.row["flow_type"],
            record.row["segment"],
            record.row["investor_class"],
        )
        deduped[key] = record
    return list(deduped.values())
