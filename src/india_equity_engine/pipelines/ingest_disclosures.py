"""Corporate disclosure ingestion pipeline."""

from __future__ import annotations

import duckdb

from india_equity_engine.connectors.nse.announcements import NSEAnnouncementsRSSConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_disclosures(settings: Settings) -> JobRunResult:
    """Ingest NSE announcement RSS into corporate_announcements."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    connector = NSEAnnouncementsRSSConnector(
        source=registry.get("S06"),
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=build_http_verify(settings),
    )
    source_object = connector.discover()[0]

    try:
        artifact = connector.download(source_object)
    except EngineError as exc:
        return JobRunResult(job_name="ingest_disclosures", status="failed", warnings=[str(exc)])

    validation = connector.validate(artifact)
    if not validation.ok:
        return JobRunResult(
            job_name="ingest_disclosures",
            status="failed",
            warnings=[validation.message],
        )

    raw_record = RawArtifactStore(settings.raw_root, settings.parser_version).store(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)
    normalized = _resolve_nse_symbols(normalized, _load_nse_symbol_map(settings))
    normalized = _with_lineage(normalized, raw_record.sha256, settings.parser_version)
    normalized = _dedupe_announcements(normalized)
    normalized = _strip_private_fields(normalized)

    write_results = ParquetStore(settings.silver_root).write_current_records(normalized)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    resolved_count = sum(1 for record in normalized if record.row.get("instrument_id"))
    return JobRunResult(
        job_name="ingest_disclosures",
        status="success",
        records_in=len(parsed),
        records_out=len(normalized),
        outputs={
            "raw_artifact": str(raw_record.path),
            "document_hash": raw_record.sha256,
            "tables": {"corporate_announcements": len(normalized)},
            "resolved_instrument_count": resolved_count,
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


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


def _dedupe_announcements(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        deduped[record.row["announcement_id"]] = record
    return list(deduped.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output
