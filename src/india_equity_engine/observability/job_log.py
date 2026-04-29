"""Job-run and data-quality observability writes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def utc_now() -> datetime:
    """Return timezone-aware UTC now for operational timestamps."""

    return datetime.now(timezone.utc)


def record_job_run(
    settings: Settings,
    result: JobRunResult,
    *,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> JobRunResult:
    """Persist a job run and emit warning rows as data-quality issues."""

    started = started_at or utc_now()
    finished = finished_at or utc_now()
    settings.ensure_runtime_dirs()
    job_run_id = stable_id("job_run", result.job_name, started.isoformat(), finished.isoformat())
    log_path = _write_job_log(settings, job_run_id, result, started, finished)
    job_row = {
        "job_run_id": job_run_id,
        "job_name": result.job_name,
        "started_at": started,
        "finished_at": finished,
        "status": result.status,
        "records_in": int(result.records_in),
        "records_out": int(result.records_out),
        "error_count": 1 if result.status == "failed" else 0,
        "warning_count": len(result.warnings),
        "log_path": str(log_path),
    }
    issue_rows = [
        data_quality_issue_row(
            table_name="job_runs",
            entity_key=job_run_id,
            rule_name="job_warning",
            severity="high" if result.status == "failed" else "medium",
            issue_text=f"{result.job_name}: {warning}",
            detected_at=finished,
        )
        for warning in result.warnings
    ]
    warnings = write_observability_records(
        settings,
        [NormalizedRecord(table_name="job_runs", row=job_row)]
        + [NormalizedRecord(table_name="data_quality_issues", row=row) for row in issue_rows],
    )
    if warnings:
        updated = result.model_copy(deep=True)
        updated.warnings.extend(f"observability: {warning}" for warning in warnings)
        return updated
    return result


def data_quality_issue_row(
    *,
    table_name: str,
    entity_key: str,
    rule_name: str,
    severity: str,
    issue_text: str,
    detected_at: datetime | None = None,
    resolved_flag: bool = False,
    resolution_note: str | None = None,
) -> dict[str, Any]:
    """Create a canonical data_quality_issues row."""

    detected = detected_at or utc_now()
    return {
        "dq_issue_id": stable_id(
            "dq_issue",
            table_name,
            entity_key,
            rule_name,
            severity,
            issue_text,
            detected.date().isoformat(),
        ),
        "table_name": table_name,
        "entity_key": entity_key,
        "detected_at": detected,
        "rule_name": rule_name,
        "severity": severity,
        "issue_text": issue_text,
        "resolved_flag": resolved_flag,
        "resolution_note": resolution_note,
    }


def write_observability_records(
    settings: Settings,
    records: list[NormalizedRecord],
) -> list[str]:
    """Merge observability records into current gold Parquet tables."""

    if not records:
        return []

    merged = _merge_existing(settings.gold_root, records)
    write_results = ParquetStore(settings.gold_root).write_current_records(merged)
    warnings = []
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        if result.path is None:
            continue
        try:
            duckdb_store.refresh_parquet_view(result.table_name, result.path)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")
    return warnings


def _write_job_log(
    settings: Settings,
    job_run_id: str,
    result: JobRunResult,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    directory = settings.logs_root / "job_runs"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{job_run_id}.json"
    payload = {
        "job_run_id": job_run_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "result": result.model_dump(mode="json"),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _merge_existing(root: Path, records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    grouped: dict[str, list[NormalizedRecord]] = {}
    for record in records:
        grouped.setdefault(record.table_name, []).append(record)

    output = []
    for table_name, new_records in grouped.items():
        pk = _primary_key(table_name)
        merged: dict[tuple[object, ...], NormalizedRecord] = {}
        for row in _read_existing_rows(root / table_name / "current.parquet"):
            record = NormalizedRecord(table_name=table_name, row=row)
            merged[_key(row, pk)] = record
        for record in new_records:
            merged[_key(record.row, pk)] = record
        output.extend(merged.values())
    return output


def _read_existing_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with duckdb.connect() as con:
            escaped_path = str(path).replace("'", "''")
            result = con.execute(f"select * from read_parquet('{escaped_path}')")
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _primary_key(table_name: str) -> tuple[str, ...]:
    if table_name == "job_runs":
        return ("job_run_id",)
    if table_name == "data_quality_issues":
        return ("dq_issue_id",)
    return ("table_name",)


def _key(row: dict[str, Any], fields: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(row.get(field) for field in fields)
