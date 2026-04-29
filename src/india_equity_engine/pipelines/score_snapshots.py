"""Build deterministic score snapshots from feature snapshots."""

from __future__ import annotations

from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import yaml

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.features.common import date_or_none
from india_equity_engine.scoring.engine import (
    build_score_records,
    confidence_rules_from_config,
    score_weights_from_config,
)
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def score_snapshots(settings: Settings, as_of_date: date | None = None) -> JobRunResult:
    """Build canonical score_snapshots from local feature snapshots."""

    settings.ensure_runtime_dirs()
    resolved_as_of_date = as_of_date or _infer_feature_as_of_date(settings)
    if resolved_as_of_date is None:
        return JobRunResult(
            job_name="score_snapshots",
            status="failed",
            warnings=["No feature_snapshots rows found. Run compute-features first."],
        )

    feature_rows = _load_feature_rows(settings, resolved_as_of_date)
    if not feature_rows:
        return JobRunResult(
            job_name="score_snapshots",
            status="failed",
            warnings=[f"No feature_snapshots rows found for {resolved_as_of_date}."],
        )
    event_signal_rows = _load_event_signal_rows(settings, resolved_as_of_date)
    weights = score_weights_from_config(
        _load_yaml(settings.config_dir / "weights" / "score_weights.yaml")
    )
    confidence_rules = confidence_rules_from_config(
        _load_yaml(settings.config_dir / "weights" / "confidence_rules.yaml")
    )

    records = build_score_records(
        feature_rows,
        event_signal_rows,
        as_of_date=resolved_as_of_date,
        score_weights=weights,
        confidence_rules=confidence_rules,
    )
    if not records:
        return JobRunResult(
            job_name="score_snapshots",
            status="failed",
            records_in=len(feature_rows) + len(event_signal_rows),
            warnings=["No score_snapshots records were produced."],
        )

    write_results = ParquetStore(settings.gold_root).write_current_records(records)
    warnings = _refresh_duckdb_views(settings, write_results)
    classification_counts = Counter(record.row.get("classification") for record in records)
    horizon_counts = Counter(record.row.get("horizon") for record in records)
    return JobRunResult(
        job_name="score_snapshots",
        status="success",
        records_in=len(feature_rows) + len(event_signal_rows),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "as_of_date": resolved_as_of_date.isoformat(),
            "tables": dict(Counter(record.table_name for record in records)),
            "horizons": dict(horizon_counts),
            "classifications": dict(classification_counts),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _infer_feature_as_of_date(settings: Settings) -> date | None:
    rows = _query_rows(
        settings,
        f"select max(as_of_date) as max_date from {_feature_source(settings)}",
        [],
    )
    return date_or_none(rows[0].get("max_date")) if rows else None


def _load_feature_rows(settings: Settings, as_of_date: date) -> list[dict[str, Any]]:
    return _query_rows(
        settings,
        f"""
        select *
        from {_feature_source(settings)}
        where as_of_date = ?
        """,
        [as_of_date],
    )


def _load_event_signal_rows(settings: Settings, as_of_date: date) -> list[dict[str, Any]]:
    source = _event_signal_source(settings)
    if source is None:
        return []
    return _query_rows(
        settings,
        f"""
        select *
        from {source}
        where event_date <= ?
        """,
        [as_of_date],
    )


def _feature_source(settings: Settings) -> str:
    parquet_path = settings.gold_root / "feature_snapshots" / "current.parquet"
    if parquet_path.exists():
        return _read_parquet_expr(parquet_path)
    return "feature_snapshots"


def _event_signal_source(settings: Settings) -> str | None:
    parquet_path = settings.silver_root / "event_signals" / "current.parquet"
    if parquet_path.exists():
        return _read_parquet_expr(parquet_path)
    return "event_signals" if settings.duckdb_path.exists() else None


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


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
