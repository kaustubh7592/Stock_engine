"""Download selected filing artifacts for later structured parsing."""

from __future__ import annotations

from dataclasses import dataclass
from mimetypes import guess_extension
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import httpx

from india_equity_engine.core.exceptions import EngineError
from india_equity_engine.core.http import build_http_verify
from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import (
    JobRunResult,
    NormalizedRecord,
    RawArtifact,
)
from india_equity_engine.core.settings import Settings
from india_equity_engine.core.time_utils import utc_now
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.local_fs import RawArtifactStore
from india_equity_engine.storage.parquet_store import ParquetStore


@dataclass(frozen=True)
class FilingDownloadCandidate:
    filing_id: str
    instrument_id: str | None
    exchange_code: str
    document_type: str
    document_url: str


def download_filings(
    settings: Settings,
    document_types: tuple[str, ...] = ("XBRL", "XML", "ZIP"),
    limit: int = 25,
) -> JobRunResult:
    """Download selected filing artifacts, prioritizing structured documents."""

    settings.ensure_runtime_dirs()
    normalized_types = _normalize_document_types(document_types)
    candidates = _load_candidates(settings, normalized_types, limit)
    if not candidates:
        return JobRunResult(
            job_name="download_filings",
            status="failed",
            warnings=["No filing candidates found. Run iee ingest-filings first."],
        )

    store = RawArtifactStore(settings.raw_root, settings.parser_version)
    records: list[NormalizedRecord] = []
    warnings = []

    with httpx.Client(
        timeout=settings.http_timeout_seconds,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent, "Accept": "*/*"},
        verify=build_http_verify(settings),
    ) as client:
        for candidate in candidates:
            try:
                artifact = _download_candidate(client, candidate)
                raw_record = store.store(artifact)
                row = _success_row(
                    candidate=candidate,
                    local_path=raw_record.path,
                    metadata_path=raw_record.metadata_path,
                    content_type=raw_record.content_type,
                    size_bytes=raw_record.size_bytes,
                    sha256=raw_record.sha256,
                )
            except EngineError as exc:
                warnings.append(str(exc))
                row = _failed_row(candidate, str(exc))
            records.append(NormalizedRecord(table_name="filing_artifacts", row=row))

    records = _with_lineage(records, settings.parser_version)
    write_results = ParquetStore(settings.silver_root).write_current_records(records)
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.silver_root / result.table_name / "current.parquet"
        duckdb_store.refresh_parquet_view(result.table_name, table_path)

    success_count = sum(1 for record in records if record.row["download_status"] == "success")
    return JobRunResult(
        job_name="download_filings",
        status="partial_success" if warnings else "success",
        records_in=len(candidates),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "document_types": list(normalized_types),
            "success_count": success_count,
            "failure_count": len(records) - success_count,
            "tables": {"filing_artifacts": len(records)},
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _load_candidates(
    settings: Settings,
    document_types: tuple[str, ...],
    limit: int,
) -> list[FilingDownloadCandidate]:
    if not settings.duckdb_path.exists():
        return []
    if limit < 1:
        return []

    filter_clause = ""
    order_clause = "xbrl_flag desc, filing_date desc nulls last, filing_id"
    params: list[object] = []
    if "ALL" not in document_types:
        placeholders = ", ".join(["?"] * len(document_types))
        filter_clause = f"and upper(document_type) in ({placeholders})"
        params.extend(document_types)
        priority_case = " ".join(
            f"when upper(document_type) = ? then {index}"
            for index, _ in enumerate(document_types)
        )
        order_clause = (
            f"xbrl_flag desc, case {priority_case} else 999 end, "
            "filing_date desc nulls last, filing_id"
        )
        params.extend(document_types)

    query = f"""
        select filing_id, instrument_id, exchange_code, document_type, document_url
        from filings
        where document_url is not null
          {filter_clause}
        order by {order_clause}
        limit ?
    """
    params.append(limit)

    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            rows = con.execute(query, params).fetchall()
    except duckdb.Error:
        return []

    return [
        FilingDownloadCandidate(
            filing_id=str(row[0]),
            instrument_id=str(row[1]) if row[1] is not None else None,
            exchange_code=str(row[2] or "NSE").upper(),
            document_type=str(row[3] or "").upper(),
            document_url=str(row[4]),
        )
        for row in rows
    ]


def _download_candidate(
    client: httpx.Client,
    candidate: FilingDownloadCandidate,
) -> RawArtifact:
    try:
        response = client.get(candidate.document_url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise EngineError(f"Failed to download filing {candidate.filing_id}: {exc}") from exc

    if not response.content.strip():
        raise EngineError(
            f"Failed to download filing {candidate.filing_id}: response body was empty"
        )
    if _looks_like_html_error(response.content, response.headers.get("content-type")):
        raise EngineError(
            f"Failed to download filing {candidate.filing_id}: expected a filing artifact, got HTML"
        )

    extension = _extension_for(
        candidate.document_url,
        candidate.document_type,
        response.headers.get("content-type"),
    )
    return RawArtifact(
        source_code="S09",
        source_family=candidate.exchange_code.lower(),
        logical_name=f"filing_{candidate.filing_id}",
        source_url=candidate.document_url,
        content=response.content,
        extension=extension,
        retrieved_at=utc_now(),
        content_type=response.headers.get("content-type"),
        metadata={
            "filing_id": candidate.filing_id,
            "instrument_id": candidate.instrument_id,
            "document_type": candidate.document_type,
        },
    )


def _success_row(
    candidate: FilingDownloadCandidate,
    local_path: Path,
    metadata_path: Path,
    content_type: str | None,
    size_bytes: int,
    sha256: str,
) -> dict[str, object]:
    return {
        "artifact_id": stable_id("artifact", candidate.filing_id, sha256),
        "filing_id": candidate.filing_id,
        "instrument_id": candidate.instrument_id,
        "exchange_code": candidate.exchange_code,
        "document_type": candidate.document_type,
        "document_url": candidate.document_url,
        "local_path": str(local_path),
        "metadata_path": str(metadata_path),
        "content_type": content_type,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "download_status": "success",
        "error_text": None,
    }


def _failed_row(candidate: FilingDownloadCandidate, error_text: str) -> dict[str, object]:
    return {
        "artifact_id": stable_id("artifact", candidate.filing_id, "failed"),
        "filing_id": candidate.filing_id,
        "instrument_id": candidate.instrument_id,
        "exchange_code": candidate.exchange_code,
        "document_type": candidate.document_type,
        "document_url": candidate.document_url,
        "local_path": None,
        "metadata_path": None,
        "content_type": None,
        "size_bytes": None,
        "sha256": None,
        "download_status": "failed",
        "error_text": error_text,
    }


def _with_lineage(
    records: list[NormalizedRecord],
    parser_version: str,
) -> list[NormalizedRecord]:
    now = utc_now()
    output = []
    for record in records:
        row = dict(record.row)
        row["source"] = row.get("exchange_code", "").lower()
        row["source_url"] = row.get("document_url")
        row["retrieved_at"] = now
        row["available_at"] = now
        row["as_of_date"] = now.date()
        row["document_hash"] = row.get("sha256")
        row["parser_version"] = parser_version
        row["restated_flag"] = False
        row["created_at"] = now
        row["updated_at"] = now
        output.append(NormalizedRecord(table_name=record.table_name, row=row))
    return output


def _normalize_document_types(document_types: tuple[str, ...]) -> tuple[str, ...]:
    normalized = []
    for document_type in document_types:
        value = document_type.strip().upper()
        if value and value not in normalized:
            normalized.append(value)
    return tuple(normalized or ["XBRL", "XML", "ZIP"])


def _extension_for(
    document_url: str,
    document_type: str,
    content_type: str | None = None,
) -> str:
    suffix = Path(urlparse(document_url).path).suffix.lower().lstrip(".")
    if suffix:
        return suffix

    if content_type:
        mime_type = content_type.split(";", 1)[0].strip().lower()
        guessed = guess_extension(mime_type)
        if guessed:
            return guessed.lstrip(".")

    document_type_map = {
        "XBRL": "xml",
        "XML": "xml",
        "PDF": "pdf",
        "ZIP": "zip",
        "HTML": "html",
    }
    return document_type_map.get(document_type.upper(), document_type.lower())


def _looks_like_html_error(content: bytes, content_type: str | None = None) -> bool:
    if content_type and "html" in content_type.lower():
        return True
    sample = content[:256].lstrip().lower()
    return sample.startswith(b"<!doctype html") or sample.startswith(b"<html")
