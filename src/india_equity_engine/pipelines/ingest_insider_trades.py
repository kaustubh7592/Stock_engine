"""Ingest NSE PIT insider-trading rows into the canonical table."""

from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

import duckdb

from india_equity_engine.connectors.nse.insider_trades import NSEInsiderTradesConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_insider_trades(
    settings: Settings,
    from_date: date | None = None,
    to_date: date | None = None,
    lookback_days: int = 30,
    index: str = "equities",
    symbol: str | None = None,
) -> JobRunResult:
    """Ingest NSE PIT insider-trading disclosures."""

    settings.ensure_runtime_dirs()
    to_date = to_date or date.today()
    from_date = from_date or (to_date - timedelta(days=lookback_days))
    if from_date > to_date:
        return JobRunResult(
            job_name="ingest_insider_trades",
            status="failed",
            warnings=["from_date must be on or before to_date."],
        )

    registry = SourceRegistry(settings.config_dir)
    connector = NSEInsiderTradesConnector(
        source=registry.get("S09"),
        user_agent=settings.user_agent,
        from_date=from_date,
        to_date=to_date,
        index=index,
        symbol=symbol,
        timeout_seconds=settings.http_timeout_seconds,
        verify=build_http_verify(settings),
        trust_env=settings.http_trust_env,
    )

    source_object = connector.discover()[0]
    try:
        artifact = connector.download(source_object)
    except EngineError as exc:
        return JobRunResult(
            job_name="ingest_insider_trades",
            status="failed",
            warnings=[str(exc)],
        )

    validation = connector.validate(artifact)
    if not validation.ok:
        return JobRunResult(
            job_name="ingest_insider_trades",
            status="failed",
            warnings=[validation.message],
        )

    raw_record = RawArtifactStore(settings.raw_root, settings.parser_version).store(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)
    normalized = _resolve_nse_symbols(normalized, _load_nse_resolution_maps(settings))
    normalized = _with_lineage(normalized, raw_record.sha256, settings.parser_version)
    normalized = _dedupe_records(normalized)
    normalized = _strip_private_fields(normalized)

    if not normalized:
        return JobRunResult(
            job_name="ingest_insider_trades",
            status="failed",
            records_in=len(parsed),
            warnings=["No insider-trade records were produced."],
        )

    write_results = ParquetStore(settings.silver_root).write_current_records(normalized)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    counts = Counter(record.table_name for record in normalized)
    return JobRunResult(
        job_name="ingest_insider_trades",
        status="success",
        records_in=len(parsed),
        records_out=len(normalized),
        outputs={
            "raw_artifacts": [str(raw_record.path)],
            "document_hashes": {raw_record.logical_name: raw_record.sha256},
            "tables": dict(counts),
            "resolved_instrument_count": sum(
                1 for record in normalized if record.row.get("instrument_id")
            ),
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "index": index,
            "symbol": symbol,
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_nse_resolution_maps(settings: Settings) -> dict[str, str]:
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


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        deduped[record.row["insider_trade_id"]] = record
    return list(deduped.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output
