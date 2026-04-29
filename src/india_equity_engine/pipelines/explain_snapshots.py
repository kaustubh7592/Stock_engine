"""Generate validated explanation outputs from stock_snapshot JSON."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.schemas.snapshot_schema import StockSnapshot
from india_equity_engine.core.settings import Settings
from india_equity_engine.features.common import date_or_none
from india_equity_engine.llm.explainer import MODEL_NAME, explain_snapshot, snapshot_input_hash
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def explain_snapshots(
    settings: Settings,
    as_of_date: date | None = None,
    limit: int = 100,
) -> JobRunResult:
    """Generate schema-valid explanations from stock_snapshot JSON artifacts."""

    settings.ensure_runtime_dirs()
    resolved_as_of_date = as_of_date or _infer_snapshot_as_of_date(settings)
    if resolved_as_of_date is None:
        return JobRunResult(
            job_name="explain_snapshots",
            status="failed",
            warnings=["No stock_snapshots rows found. Run build-stock-snapshots first."],
        )

    snapshot_rows = _load_snapshot_rows(settings, resolved_as_of_date, limit)
    if not snapshot_rows:
        return JobRunResult(
            job_name="explain_snapshots",
            status="failed",
            warnings=[f"No stock_snapshots rows found for {resolved_as_of_date}."],
        )

    records = []
    warnings = []
    markdown_paths = []
    for row in snapshot_rows:
        path = Path(str(row.get("snapshot_json_path") or ""))
        if not path.exists():
            warnings.append(f"Missing stock_snapshot JSON: {path}")
            continue
        snapshot = StockSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
        output, markdown, safety = explain_snapshot(snapshot)
        markdown_path = _write_markdown(settings, snapshot, markdown)
        markdown_paths.append(markdown_path)
        records.append(_explanation_record(snapshot, output, markdown, safety))

    if not records:
        return JobRunResult(
            job_name="explain_snapshots",
            status="failed",
            records_in=len(snapshot_rows),
            warnings=warnings or ["No explanations were produced."],
        )

    write_results = ParquetStore(settings.gold_root).write_current_records(records)
    warnings.extend(_refresh_duckdb_views(settings, write_results))
    return JobRunResult(
        job_name="explain_snapshots",
        status="success",
        records_in=len(snapshot_rows),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "as_of_date": resolved_as_of_date.isoformat(),
            "tables": dict(Counter(record.table_name for record in records)),
            "markdown_outputs": [str(path) for path in markdown_paths[:10]],
            "markdown_output_count": len(markdown_paths),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _explanation_record(
    snapshot: StockSnapshot,
    output,
    markdown: str,
    safety: dict[str, Any],
) -> NormalizedRecord:
    input_hash = snapshot_input_hash(snapshot)
    prompt_version = stable_id("prompt", "stock_explainer_system", "v1")
    return NormalizedRecord(
        table_name="llm_explanations",
        row={
            "explanation_id": stable_id(
                "explanation",
                snapshot.instrument.instrument_id,
                snapshot.snapshot_meta.as_of_date,
                MODEL_NAME,
                input_hash,
            ),
            "instrument_id": snapshot.instrument.instrument_id,
            "as_of_date": snapshot.snapshot_meta.as_of_date,
            "model_name": MODEL_NAME,
            "prompt_version": prompt_version,
            "input_hash": input_hash,
            "output_json": output.model_dump_json(),
            "rendered_markdown": markdown,
            "safety_flags_json": json.dumps(safety, sort_keys=True),
            "source": "stock_snapshot_json",
            "source_url": None,
            "retrieved_at": snapshot.snapshot_meta.generated_at,
            "available_at": snapshot.snapshot_meta.generated_at,
            "document_hash": input_hash,
            "parser_version": "stock-explainer-v1",
            "restated_flag": False,
            "created_at": snapshot.snapshot_meta.generated_at,
            "updated_at": snapshot.snapshot_meta.generated_at,
        },
    )


def _write_markdown(settings: Settings, snapshot: StockSnapshot, markdown: str) -> Path:
    as_of_date = snapshot.snapshot_meta.as_of_date.isoformat()
    directory = settings.gold_root / "llm_outputs" / as_of_date
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{snapshot.instrument.instrument_id}.md"
    path.write_text(markdown, encoding="utf-8")
    return path


def _infer_snapshot_as_of_date(settings: Settings) -> date | None:
    rows = _query_rows(
        settings,
        f"select max(as_of_date) as max_date from {_snapshot_source(settings)}",
        [],
    )
    return date_or_none(rows[0].get("max_date")) if rows else None


def _load_snapshot_rows(
    settings: Settings,
    as_of_date: date,
    limit: int,
) -> list[dict[str, Any]]:
    return _query_rows(
        settings,
        f"""
        select *
        from {_snapshot_source(settings)}
        where as_of_date = ?
        order by instrument_id
        limit ?
        """,
        [as_of_date, max(1, limit)],
    )


def _snapshot_source(settings: Settings) -> str:
    parquet_path = settings.gold_root / "stock_snapshots" / "current.parquet"
    if parquet_path.exists():
        return _read_parquet_expr(parquet_path)
    return "stock_snapshots"


def _read_parquet_expr(path: Path) -> str:
    escaped_path = str(path).replace("'", "''")
    return f"read_parquet('{escaped_path}')"


def _query_rows(settings: Settings, query: str, params: list[object]) -> list[dict[str, Any]]:
    if not settings.duckdb_path.exists():
        return []
    try:
        with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
            result = con.execute(query, params)
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _refresh_duckdb_views(settings: Settings, write_results: list[object]) -> list[str]:
    warnings = []
    duckdb_store = DuckDBStore(settings.duckdb_path)
    for result in write_results:
        table_path = settings.gold_root / result.table_name / "current.parquet"
        try:
            duckdb_store.refresh_parquet_view(result.table_name, table_path)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {result.table_name}: {exc}")
    return warnings

