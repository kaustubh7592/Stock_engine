"""NSE derivatives EOD ingestion pipeline."""

from __future__ import annotations

from collections import Counter
from datetime import date

import duckdb

from india_equity_engine.connectors.nse.derivatives_eod import NSEDerivativesEODConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import (
    JobRunResult,
    NormalizedRecord,
    RawArtifact,
)
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.ingest_market_eod import previous_weekday
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_derivatives_eod(settings: Settings, trade_date: date | None = None) -> JobRunResult:
    """Ingest NSE derivatives EOD contract rows for a trade date."""

    settings.ensure_runtime_dirs()
    target_date = trade_date or previous_weekday(date.today())
    registry = SourceRegistry(settings.config_dir)
    connector = NSEDerivativesEODConnector(
        source=registry.get("S12"),
        trade_date=target_date,
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=build_http_verify(settings),
        trust_env=settings.http_trust_env,
    )
    artifact, download_warnings = _download_first_available(connector)
    if artifact is None:
        return JobRunResult(
            job_name="ingest_derivatives_eod",
            status="failed",
            warnings=download_warnings,
            outputs={"trade_date": target_date.isoformat()},
        )

    validation = connector.validate(artifact)
    if not validation.ok:
        return JobRunResult(
            job_name="ingest_derivatives_eod",
            status="failed",
            warnings=[validation.message],
            outputs={"trade_date": target_date.isoformat()},
        )

    raw_record = RawArtifactStore(settings.raw_root, settings.parser_version).store(artifact)
    parsed = connector.parse(artifact)
    records = connector.normalize(parsed, artifact)
    records = _resolve_nse_symbols(records, _load_nse_symbol_map(settings))
    records = _strip_private_fields(_dedupe_records(records))
    records = _with_lineage(records, raw_record.sha256, settings.parser_version)
    if not records:
        return JobRunResult(
            job_name="ingest_derivatives_eod",
            status="failed",
            records_in=len(parsed),
            warnings=["No derivatives_eod records were produced."],
            outputs={"trade_date": target_date.isoformat()},
        )

    dataset_label = f"trade_date_{target_date:%Y%m%d}"
    write_results = ParquetStore(settings.silver_root).write_records(records, dataset_label)
    warnings = list(download_warnings)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_glob = settings.silver_root / result.table_name / "trade_date_*.parquet"
        try:
            duckdb_store.refresh_parquet_view(result.table_name, table_glob)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")

    counts = Counter(record.table_name for record in records)
    return JobRunResult(
        job_name="ingest_derivatives_eod",
        status="partial_success" if warnings else "success",
        records_in=len(parsed),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "trade_date": target_date.isoformat(),
            "raw_artifacts": [str(raw_record.path)],
            "document_hashes": {raw_record.logical_name: raw_record.sha256},
            "tables": dict(counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _download_first_available(
    connector: NSEDerivativesEODConnector,
) -> tuple[RawArtifact | None, list[str]]:
    warnings = []
    for source_object in connector.discover():
        try:
            return connector.download(source_object), warnings
        except EngineError as exc:
            warnings.append(str(exc))
    return None, warnings


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


def _load_nse_symbol_map(settings: Settings) -> dict[str, str]:
    if not settings.duckdb_path.exists():
        return {}
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            rows = con.execute(
                """
                select upper(symbol) as symbol, instrument_id
                from listings
                where exchange_code = 'NSE' and symbol is not null
                """
            ).fetchall()
    except duckdb.Error:
        return {}
    return {str(symbol): str(instrument_id) for symbol, instrument_id in rows}


def _resolve_nse_symbols(
    records: list[NormalizedRecord],
    symbol_map: dict[str, str],
) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = dict(record.row)
        symbol = row.get("__nse_symbol")
        if symbol is not None:
            resolved = symbol_map.get(str(symbol).upper())
            if resolved:
                row["instrument_id"] = resolved
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        key = (record.row.get("contract_id"), record.row.get("trade_date"))
        deduped[key] = record
    return list(deduped.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output
