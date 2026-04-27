"""EOD market data ingestion pipeline."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Protocol

import duckdb

from india_equity_engine.connectors.nse.market_eod import (
    NSECashMarketEODConnector,
    NSESecurityWiseDeliveryConnector,
)
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import (
    JobRunResult,
    NormalizedRecord,
    RawArtifact,
    RawArtifactRecord,
)
from india_equity_engine.core.settings import Settings
from india_equity_engine.quality.price_checks import validate_price_daily
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


class EODConnector(Protocol):
    def discover(self): ...

    def download(self, source_object): ...

    def validate(self, raw_artifact): ...

    def parse(self, raw_artifact): ...

    def normalize(self, records, raw_artifact): ...


def ingest_market_eod(settings: Settings, trade_date: date | None = None) -> JobRunResult:
    """Ingest NSE cash-market EOD prices for a trade date."""

    settings.ensure_runtime_dirs()
    target_date = trade_date or previous_weekday(date.today())
    registry = SourceRegistry(settings.config_dir)
    verify = build_http_verify(settings)
    raw_store = RawArtifactStore(settings.raw_root, settings.parser_version)
    price_connector = NSECashMarketEODConnector(
        source=registry.get("S04"),
        trade_date=target_date,
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=verify,
        trust_env=settings.http_trust_env,
    )
    delivery_connector = NSESecurityWiseDeliveryConnector(
        source=registry.get("S04"),
        trade_date=target_date,
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=verify,
        trust_env=settings.http_trust_env,
    )

    price_artifact, price_download_warnings = _download_first_available(price_connector)
    if price_artifact is None:
        return JobRunResult(
            job_name="ingest_market_eod",
            status="failed",
            warnings=price_download_warnings,
            outputs={"trade_date": target_date.isoformat()},
        )

    validation = price_connector.validate(price_artifact)
    if not validation.ok:
        return JobRunResult(
            job_name="ingest_market_eod",
            status="failed",
            warnings=[validation.message],
            outputs={"trade_date": target_date.isoformat()},
        )

    raw_records: list[RawArtifactRecord] = []
    price_raw_record = raw_store.store(price_artifact)
    raw_records.append(price_raw_record)
    parsed = price_connector.parse(price_artifact)
    price_records = price_connector.normalize(parsed, price_artifact)
    price_records = _with_lineage(
        price_records,
        price_raw_record.sha256,
        settings.parser_version,
    )

    delivery_records: list[NormalizedRecord] = []
    delivery_rows_in = 0
    warnings: list[str] = []
    delivery_artifact, delivery_download_warnings = _download_first_available(delivery_connector)
    if delivery_artifact is None:
        warnings.extend(delivery_download_warnings)
    else:
        delivery_validation = delivery_connector.validate(delivery_artifact)
        if not delivery_validation.ok:
            warnings.append(delivery_validation.message)
        else:
            delivery_raw_record = raw_store.store(delivery_artifact)
            raw_records.append(delivery_raw_record)
            delivery_parsed = delivery_connector.parse(delivery_artifact)
            delivery_rows_in = len(delivery_parsed)
            delivery_records = delivery_connector.normalize(delivery_parsed, delivery_artifact)
            symbol_map = _load_nse_symbol_map(settings)
            delivery_records = _resolve_nse_symbols(delivery_records, symbol_map)
            delivery_records = _with_lineage(
                delivery_records,
                delivery_raw_record.sha256,
                settings.parser_version,
            )

    normalized = _merge_price_daily(price_records, delivery_records)
    normalized = _strip_private_fields(normalized)

    dq_issues = validate_price_daily(normalized)
    if dq_issues:
        return JobRunResult(
            job_name="ingest_market_eod",
            status="failed",
            records_in=len(parsed) + delivery_rows_in,
            records_out=0,
            warnings=[issue.message for issue in dq_issues[:20]],
            outputs={
                "trade_date": target_date.isoformat(),
                "raw_artifacts": [str(record.path) for record in raw_records],
                "dq_issue_count": len(dq_issues),
            },
        )

    dataset_label = f"trade_date_{target_date:%Y%m%d}"
    write_results = ParquetStore(settings.silver_root).write_records(normalized, dataset_label)

    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_glob = settings.silver_root / result.table_name / "trade_date_*.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_glob)

    return JobRunResult(
        job_name="ingest_market_eod",
        status="partial_success" if warnings else "success",
        records_in=len(parsed) + delivery_rows_in,
        records_out=len(normalized),
        warnings=warnings,
        outputs={
            "trade_date": target_date.isoformat(),
            "raw_artifacts": [str(record.path) for record in raw_records],
            "document_hashes": {
                record.logical_name: record.sha256 for record in raw_records
            },
            "tables": {"price_daily": len(normalized)},
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _download_first_available(connector: EODConnector) -> tuple[RawArtifact | None, list[str]]:
    warnings = []
    for source_object in connector.discover():
        try:
            return connector.download(source_object), warnings
        except EngineError as exc:
            warnings.append(str(exc))
    return None, warnings


def previous_weekday(value: date) -> date:
    """Return the previous Monday-Friday date."""

    candidate = value - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def _with_lineage(
    records: list[NormalizedRecord],
    document_hash: str,
    parser_version: str,
) -> list[NormalizedRecord]:
    output: list[NormalizedRecord] = []
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


def _merge_price_daily(
    primary_records: list[NormalizedRecord],
    enrichment_records: list[NormalizedRecord],
) -> list[NormalizedRecord]:
    merged: dict[tuple[object, object], NormalizedRecord] = {}

    for record in primary_records:
        key = (record.row.get("instrument_id"), record.row.get("trade_date"))
        merged[key] = record

    for record in enrichment_records:
        key = (record.row.get("instrument_id"), record.row.get("trade_date"))
        existing = merged.get(key)
        if existing is None:
            continue

        row = dict(existing.row)
        for column, value in record.row.items():
            if column in {"deliverable_qty", "deliverable_pct"} and value not in (None, ""):
                row[column] = value
            elif row.get(column) in (None, "") and value not in (None, ""):
                row[column] = value
        merged[key] = NormalizedRecord(table_name=existing.table_name, row=row)

    return list(merged.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output
