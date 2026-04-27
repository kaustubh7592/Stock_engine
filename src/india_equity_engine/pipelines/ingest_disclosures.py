"""Corporate disclosure ingestion pipeline."""

from __future__ import annotations

import duckdb

from india_equity_engine.connectors.nse.announcements import (
    NSEAnnouncementsRSSConnector,
    NSECorporateActionsRSSConnector,
)
from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore
from india_equity_engine.storage.registry import SourceRegistry


def ingest_disclosures(settings: Settings) -> JobRunResult:
    """Ingest NSE announcement and corporate-action RSS feeds."""

    settings.ensure_runtime_dirs()
    registry = SourceRegistry(settings.config_dir)
    verify = build_http_verify(settings)
    raw_store = RawArtifactStore(settings.raw_root, settings.parser_version)
    connectors = [
        NSEAnnouncementsRSSConnector(
            source=registry.get("S06"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
        ),
        NSECorporateActionsRSSConnector(
            source=registry.get("S07"),
            user_agent=settings.user_agent,
            timeout_seconds=settings.http_timeout_seconds,
            verify=verify,
        ),
    ]

    all_records: list[NormalizedRecord] = []
    raw_paths = []
    document_hashes = {}
    records_in = 0
    warnings = []

    for connector in connectors:
        source_object = connector.discover()[0]
        try:
            artifact = connector.download(source_object)
        except EngineError as exc:
            warnings.append(str(exc))
            continue

        validation = connector.validate(artifact)
        if not validation.ok:
            warnings.append(validation.message)
            continue

        raw_record = raw_store.store(artifact)
        raw_paths.append(str(raw_record.path))
        document_hashes[raw_record.logical_name] = raw_record.sha256
        parsed = connector.parse(artifact)
        records_in += len(parsed)
        normalized = connector.normalize(parsed, artifact)
        normalized = _with_lineage(normalized, raw_record.sha256, settings.parser_version)
        all_records.extend(normalized)

    if not all_records:
        return JobRunResult(
            job_name="ingest_disclosures",
            status="failed",
            warnings=warnings or ["No disclosure records were produced."],
        )

    symbol_maps = _load_nse_resolution_maps(settings)
    all_records = _resolve_nse_symbols(all_records, symbol_maps)
    all_records = _dedupe_records(all_records)
    all_records = _strip_private_fields(all_records)

    write_results = ParquetStore(settings.silver_root).write_current_records(all_records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    table_counts = _table_counts(all_records)
    resolved_count = sum(1 for record in all_records if record.row.get("instrument_id"))
    return JobRunResult(
        job_name="ingest_disclosures",
        status="partial_success" if warnings else "success",
        records_in=records_in,
        records_out=len(all_records),
        warnings=warnings,
        outputs={
            "raw_artifacts": raw_paths,
            "document_hashes": document_hashes,
            "tables": table_counts,
            "resolved_instrument_count": resolved_count,
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


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


def _resolve_nse_symbols(
    records: list[NormalizedRecord],
    symbol_maps: dict[str, dict[str, str]],
) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = dict(record.row)
        symbol = row.get("__nse_symbol")
        if symbol is not None:
            resolved = symbol_maps["symbols"].get(str(symbol).upper())
            if not resolved:
                resolved = symbol_maps["names"].get(_normalize_name_key(symbol))
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
        if record.table_name == "corporate_announcements":
            key = (record.table_name, record.row["announcement_id"])
        elif record.table_name == "corporate_actions":
            key = (record.table_name, record.row["corporate_action_id"])
        else:
            key = (record.table_name, id(record))
        deduped[key] = record
    return list(deduped.values())


def _strip_private_fields(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    output = []
    for record in records:
        row = {key: value for key, value in record.row.items() if not key.startswith("__")}
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output


def _table_counts(records: list[NormalizedRecord]) -> dict[str, int]:
    counts = {}
    for record in records:
        counts[record.table_name] = counts.get(record.table_name, 0) + 1
    return counts


def _normalize_name_key(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).upper()
    text = text.replace("&", "AND")
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
