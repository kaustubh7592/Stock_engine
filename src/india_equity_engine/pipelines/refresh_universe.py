"""Security-master refresh pipeline."""

from __future__ import annotations

from collections import Counter
from typing import Protocol

from india_equity_engine.connectors.bse.security_master import BSEScripMasterConnector
from india_equity_engine.connectors.nse.nifty500 import NSENifty500Connector
from india_equity_engine.connectors.nse.security_master import NSEEquityListConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import (
    JobRunResult,
    NormalizedRecord,
    RawArtifactRecord,
)
from india_equity_engine.core.settings import Settings
from india_equity_engine.models.canonical import CANONICAL_SCHEMAS
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


class SecurityMasterConnector(Protocol):
    def discover(self): ...

    def download(self, source_object): ...

    def validate(self, raw_artifact): ...

    def parse(self, raw_artifact): ...

    def normalize(self, records, raw_artifact): ...


def refresh_universe(settings: Settings) -> JobRunResult:
    """Refresh the Stage A security master, BSE aliases, and Nifty 500 universe."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    verify = build_http_verify(settings)
    raw_store = RawArtifactStore(settings.raw_root, settings.parser_version)

    connectors: list[SecurityMasterConnector] = [
        NSEEquityListConnector(
            source=registry.get("S01"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
        ),
        NSENifty500Connector(
            source=registry.get("S03"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
        ),
        BSEScripMasterConnector(
            source=registry.get("S02"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
        ),
    ]

    normalized: list[NormalizedRecord] = []
    raw_records: list[RawArtifactRecord] = []
    records_in = 0
    warnings: list[str] = []

    for connector in connectors:
        artifact = None
        source_warnings = []
        try:
            for source_object in connector.discover():
                try:
                    artifact = connector.download(source_object)
                    break
                except EngineError as exc:
                    source_warnings.append(str(exc))
            if artifact is None:
                warnings.extend(source_warnings)
                continue
            validation = connector.validate(artifact)
            if not validation.ok:
                warnings.append(validation.message)
                continue
            raw_records.append(raw_store.store(artifact))
            parsed = connector.parse(artifact)
            records_in += len(parsed)
            normalized.extend(connector.normalize(parsed, artifact))
        except EngineError as exc:
            warnings.append(str(exc))

    if not normalized:
        return JobRunResult(
            job_name="refresh_universe",
            status="failed",
            records_in=records_in,
            warnings=warnings or ["No security-master records were produced."],
        )

    normalized = _dedupe_records(normalized)

    write_results = ParquetStore(settings.silver_root).write_current_records(normalized)

    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in normalized)
    status = "partial_success" if warnings else "success"
    return JobRunResult(
        job_name="refresh_universe",
        status=status,
        records_in=records_in,
        records_out=len(normalized),
        warnings=warnings,
        outputs={
            "raw_artifacts": [str(record.path) for record in raw_records],
            "document_hashes": {
                record.logical_name: record.sha256 for record in raw_records
            },
            "tables": dict(counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    merged: dict[tuple[str, tuple[object, ...]], NormalizedRecord] = {}
    passthrough_counter = 0

    for record in records:
        schema = CANONICAL_SCHEMAS.get(record.table_name)
        if schema is None:
            passthrough_counter += 1
            merged[(record.table_name, ("__row__", passthrough_counter))] = record
            continue

        key = tuple(record.row.get(column) for column in schema.primary_key)
        if any(value in (None, "") for value in key):
            passthrough_counter += 1
            merged[(record.table_name, ("__row__", passthrough_counter))] = record
            continue

        merge_key = (record.table_name, key)
        existing = merged.get(merge_key)
        if existing is None:
            merged[merge_key] = record
            continue

        merged_row = dict(existing.row)
        for column, value in record.row.items():
            if merged_row.get(column) in (None, "") and value not in (None, ""):
                merged_row[column] = value
        merged[merge_key] = NormalizedRecord(table_name=record.table_name, row=merged_row)

    return list(merged.values())
