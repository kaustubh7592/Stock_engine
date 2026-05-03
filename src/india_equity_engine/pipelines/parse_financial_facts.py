"""Parse downloaded XBRL/XML filing artifacts into financial facts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import duckdb

from india_equity_engine.connectors.xbrl.facts import (
    XBRLArtifactMetadata,
    parse_financial_facts_from_xml,
)
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.core.time_utils import utc_now
from india_equity_engine.pipelines.stock_lookup import resolve_instrument
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


@dataclass(frozen=True)
class FinancialFactArtifact:
    artifact_id: str
    filing_id: str
    instrument_id: str | None
    document_type: str
    local_path: Path
    source_url: str | None
    document_hash: str | None
    available_at: object | None
    as_of_date: object | None
    source: str | None


def parse_financial_facts(
    settings: Settings,
    limit: int = 25,
    symbol_or_id: str | None = None,
) -> JobRunResult:
    """Parse successful filing artifacts into the canonical financial_facts table."""

    settings.ensure_runtime_dirs()
    target_instrument_id = _resolve_target_instrument_id(settings, symbol_or_id)
    artifacts = _load_artifacts(settings, limit, target_instrument_id)
    if not artifacts:
        target_text = f" for {symbol_or_id}" if symbol_or_id else ""
        return JobRunResult(
            job_name="parse_financial_facts",
            status="failed",
            warnings=[
                "No successful XBRL/XML filing artifacts found"
                f"{target_text}. Run iee download-filings first."
            ],
        )

    records: list[NormalizedRecord] = []
    warnings: list[str] = []
    for artifact in artifacts:
        try:
            records.extend(_parse_artifact(artifact, settings.parser_version))
        except (OSError, BadZipFile, ValueError) as exc:
            warnings.append(f"Failed to parse artifact {artifact.artifact_id}: {exc}")

    if not records:
        return JobRunResult(
            job_name="parse_financial_facts",
            status="failed",
            records_in=len(artifacts),
            warnings=warnings or ["No numeric facts were parsed from the selected artifacts."],
        )

    records = _dedupe_records(records)
    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    instrument_count = len({record.row.get("instrument_id") for record in records})
    concept_count = len({record.row.get("taxonomy_concept") for record in records})
    return JobRunResult(
        job_name="parse_financial_facts",
        status="partial_success" if warnings else "success",
        records_in=len(artifacts),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "tables": {"financial_facts": len(records)},
            "symbol_or_id": symbol_or_id,
            "target_instrument_id": target_instrument_id,
            "instrument_count": instrument_count,
            "concept_count": concept_count,
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_artifacts(
    settings: Settings,
    limit: int,
    instrument_id: str | None = None,
) -> list[FinancialFactArtifact]:
    if not settings.duckdb_path.exists() or limit < 1:
        return []

    instrument_clause = "and instrument_id = ?" if instrument_id else ""
    params: list[object] = []
    if instrument_id:
        params.append(instrument_id)
    query = f"""
        select
            artifact_id,
            filing_id,
            instrument_id,
            document_type,
            local_path,
            source_url,
            document_hash,
            available_at,
            as_of_date,
            source
        from filing_artifacts
        where download_status = 'success'
          and local_path is not null
          and upper(document_type) in ('XBRL', 'XML', 'ZIP')
          {instrument_clause}
        order by available_at desc nulls last, filing_id, artifact_id
        limit ?
    """
    params.append(limit)
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            rows = con.execute(query, params).fetchall()
    except duckdb.Error:
        return []

    return [
        FinancialFactArtifact(
            artifact_id=str(row[0]),
            filing_id=str(row[1]),
            instrument_id=str(row[2]) if row[2] is not None else None,
            document_type=str(row[3] or "").upper(),
            local_path=Path(str(row[4])),
            source_url=str(row[5]) if row[5] is not None else None,
            document_hash=str(row[6]) if row[6] is not None else None,
            available_at=row[7] or utc_now(),
            as_of_date=row[8] or utc_now().date(),
            source=str(row[9]) if row[9] is not None else None,
        )
        for row in rows
    ]


def _resolve_target_instrument_id(settings: Settings, symbol_or_id: str | None) -> str | None:
    if not symbol_or_id:
        return None
    resolved = resolve_instrument(settings, symbol_or_id)
    if resolved:
        return str(resolved["instrument_id"])
    return symbol_or_id.strip()


def _parse_artifact(
    artifact: FinancialFactArtifact,
    parser_version: str,
) -> list[NormalizedRecord]:
    if not artifact.local_path.exists():
        raise OSError(f"missing local file {artifact.local_path}")

    metadata = XBRLArtifactMetadata(
        filing_id=artifact.filing_id,
        instrument_id=artifact.instrument_id,
        source_url=artifact.source_url,
        document_hash=artifact.document_hash,
        available_at=artifact.available_at,
        as_of_date=artifact.as_of_date,
        source=artifact.source,
    )
    records: list[NormalizedRecord] = []
    for content in _xml_payloads(artifact.local_path):
        records.extend(parse_financial_facts_from_xml(content, metadata, parser_version))
    return records


def _xml_payloads(path: Path) -> list[bytes]:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        payloads = []
        with ZipFile(path) as archive:
            for name in archive.namelist():
                if name.lower().endswith((".xml", ".xbrl")):
                    payloads.append(archive.read(name))
        return payloads
    return [path.read_bytes()]


def _dedupe_records(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        deduped[record.row["fact_id"]] = record
    return list(deduped.values())
