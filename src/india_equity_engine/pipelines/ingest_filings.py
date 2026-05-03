"""Filing discovery ingestion pipeline."""

from __future__ import annotations

import duckdb

from india_equity_engine.connectors.nse.filings import NSEFilingDiscoveryConnector
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_filings(settings: Settings, symbol: str | None = None) -> JobRunResult:
    """Discover filing/document metadata from official NSE surfaces."""

    settings.ensure_runtime_dirs()
    normalized_symbol = symbol.strip().upper() if symbol else None
    registry = SourceRegistry(settings.config_dir)
    connector = NSEFilingDiscoveryConnector(
        source=registry.get("S09"),
        user_agent=settings.user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        verify=build_http_verify(settings),
        trust_env=settings.http_trust_env,
    )

    raw_records = []
    normalized = []
    records_in = 0
    warnings = []
    for source_object in connector.discover():
        try:
            artifact = connector.download(source_object)
        except EngineError as exc:
            warnings.append(str(exc))
            continue

        validation = connector.validate(artifact)
        if not validation.ok:
            warnings.append(validation.message)
            continue

        raw_record = RawArtifactStore(settings.raw_root, settings.parser_version).store(artifact)
        raw_records.append(raw_record)
        parsed = connector.parse(artifact)
        records_in += len(parsed)
        source_records = connector.normalize(parsed, artifact)
        source_records = _resolve_filings(source_records, _load_nse_resolution_maps(settings))
        if normalized_symbol:
            source_records = _filter_symbol(source_records, normalized_symbol)
        source_records = _with_lineage(
            source_records,
            raw_record.sha256,
            settings.parser_version,
        )
        normalized.extend(source_records)

    if not normalized:
        return JobRunResult(
            job_name="ingest_filings",
            status="failed",
            warnings=warnings or ["No filing records were produced."],
        )

    normalized = _dedupe_filings(normalized)
    normalized = _strip_private_fields(normalized)

    write_results = ParquetStore(settings.silver_root).write_current_records(normalized)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    resolved_count = sum(1 for record in normalized if record.row.get("instrument_id"))
    xbrl_count = sum(1 for record in normalized if record.row.get("xbrl_flag"))
    return JobRunResult(
        job_name="ingest_filings",
        status="partial_success" if warnings else "success",
        records_in=records_in,
        records_out=len(normalized),
        warnings=warnings,
        outputs={
            "symbol": normalized_symbol,
            "raw_artifacts": [str(record.path) for record in raw_records],
            "document_hashes": {record.logical_name: record.sha256 for record in raw_records},
            "tables": {"filings": len(normalized)},
            "resolved_instrument_count": resolved_count,
            "xbrl_count": xbrl_count,
            "shareholding_count": sum(
                1 for record in normalized if record.row.get("filing_family") == "shareholding"
            ),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _filter_symbol(records: list[NormalizedRecord], symbol: str) -> list[NormalizedRecord]:
    return [
        record
        for record in records
        if str(record.row.get("__nse_symbol") or "").upper() == symbol
        or str(record.row.get("instrument_id") or "").upper() == symbol
    ]


def _load_nse_resolution_maps(settings: Settings) -> dict[str, dict[str, str]]:
    if not settings.duckdb_path.exists():
        return {"symbols": {}, "names": {}}
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            rows = con.execute(
                """
                select upper(symbol) as symbol, instrument_id, i.legal_name, i.issuer_name
                from listings
                left join instruments i using (instrument_id)
                where exchange_code = 'NSE' and symbol is not null
                """
            ).fetchall()
    except duckdb.Error:
        return {"symbols": {}, "names": {}}

    symbols = {}
    names = {}
    for symbol, instrument_id, legal_name, issuer_name in rows:
        symbols[str(symbol)] = str(instrument_id)
        for name in (legal_name, issuer_name):
            normalized_name = _normalize_name_key(name)
            if normalized_name:
                names[normalized_name] = str(instrument_id)
    return {"symbols": symbols, "names": names}


def _resolve_filings(
    records: list[NormalizedRecord],
    symbol_maps: dict[str, dict[str, str]],
) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = dict(record.row)
        symbol = row.get("__nse_symbol")
        company_name = row.get("__company_name")
        resolved = None
        if symbol is not None:
            resolved = symbol_maps["symbols"].get(str(symbol).upper())
        if not resolved and company_name is not None:
            resolved = symbol_maps["names"].get(_normalize_name_key(company_name))
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


def _dedupe_filings(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        deduped[record.row["filing_id"]] = record
    return list(deduped.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output


def _normalize_name_key(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).upper().replace("&", "AND")
    for suffix in (
        " LIMITED",
        " LTD",
        " PRIVATE",
        " PVT",
        " COMPANY",
        " CO",
        " CORPORATION",
        " CORP",
    ):
        text = text.replace(suffix, "")
    normalized = "".join(char for char in text if char.isalnum())
    return normalized or None
