"""Build validated stock_snapshot JSON artifacts."""

from __future__ import annotations

import json
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.features.common import date_or_none
from india_equity_engine.snapshots.builder import (
    SNAPSHOT_SCHEMA_VERSION,
    build_stock_snapshot_payload,
    snapshot_driver_json,
)
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def build_stock_snapshots(
    settings: Settings,
    as_of_date: date | None = None,
    limit: int = 5000,
) -> JobRunResult:
    """Build validated stock_snapshot JSON artifacts and metadata rows."""

    settings.ensure_runtime_dirs()
    resolved_as_of_date = as_of_date or _infer_score_as_of_date(settings)
    if resolved_as_of_date is None:
        return JobRunResult(
            job_name="build_stock_snapshots",
            status="failed",
            warnings=["No score_snapshots rows found. Run score-snapshots first."],
        )

    score_rows = _load_score_rows(settings, resolved_as_of_date, limit)
    if not score_rows:
        return JobRunResult(
            job_name="build_stock_snapshots",
            status="failed",
            warnings=[f"No score_snapshots rows found for {resolved_as_of_date}."],
        )
    feature_rows = _load_feature_rows(settings, resolved_as_of_date)
    instrument_rows = _load_instruments(settings)
    listing_rows = _load_listings(settings)

    by_instrument_scores = _group(score_rows, "instrument_id")
    by_instrument_features = _group(feature_rows, "instrument_id")
    by_instrument_listings = _group(listing_rows, "instrument_id")
    records = []
    json_paths = []
    for instrument_id in sorted(by_instrument_scores):
        snapshot = build_stock_snapshot_payload(
            instrument_id=instrument_id,
            as_of_date=resolved_as_of_date,
            instrument_row=instrument_rows.get(instrument_id),
            listing_rows=by_instrument_listings.get(instrument_id, []),
            feature_rows=by_instrument_features.get(instrument_id, []),
            score_rows=by_instrument_scores[instrument_id],
        )
        snapshot_path = _write_snapshot_json(settings, snapshot.model_dump(mode="json"))
        json_paths.append(snapshot_path)
        records.append(_metadata_record(snapshot, snapshot_path))

    write_results = ParquetStore(settings.gold_root).write_current_records(records)
    warnings = _refresh_duckdb_views(settings, write_results)
    classifications = Counter()
    for record in records:
        classifications.update(
            {
                "short:" + str(record.row.get("short_horizon_classification")): 1,
                "medium:" + str(record.row.get("medium_horizon_classification")): 1,
                "long:" + str(record.row.get("long_horizon_classification")): 1,
            }
        )
    return JobRunResult(
        job_name="build_stock_snapshots",
        status="success",
        records_in=len(score_rows) + len(feature_rows),
        records_out=len(records),
        warnings=warnings,
        outputs={
            "as_of_date": resolved_as_of_date.isoformat(),
            "tables": dict(Counter(record.table_name for record in records)),
            "classifications": dict(classifications),
            "json_outputs": [str(path) for path in json_paths[:10]],
            "json_output_count": len(json_paths),
            "parquet_outputs": [str(result.path) for result in write_results if result.path],
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _metadata_record(snapshot, snapshot_path: Path) -> NormalizedRecord:
    positives, negatives, risks = snapshot_driver_json(snapshot)
    scores = snapshot.scores
    return NormalizedRecord(
        table_name="stock_snapshots",
        row={
            "instrument_id": snapshot.instrument.instrument_id,
            "as_of_date": snapshot.snapshot_meta.as_of_date,
            "snapshot_version": SNAPSHOT_SCHEMA_VERSION,
            "short_horizon_classification": _classification(scores, "short"),
            "medium_horizon_classification": _classification(scores, "medium"),
            "long_horizon_classification": _classification(scores, "long"),
            "top_positive_drivers_json": json.dumps(positives, sort_keys=True),
            "top_negative_drivers_json": json.dumps(negatives, sort_keys=True),
            "risk_flags_json": json.dumps(risks, sort_keys=True),
            "snapshot_json_path": str(snapshot_path),
            "source": "stock_snapshot_builder",
            "source_url": None,
            "retrieved_at": snapshot.snapshot_meta.generated_at,
            "available_at": snapshot.snapshot_meta.generated_at,
            "document_hash": stable_id(
                "stock_snapshot",
                snapshot.instrument.instrument_id,
                snapshot.snapshot_meta.as_of_date,
                snapshot.model_dump_json(),
            ),
            "parser_version": "stock-snapshot-v1",
            "restated_flag": False,
            "created_at": snapshot.snapshot_meta.generated_at,
            "updated_at": snapshot.snapshot_meta.generated_at,
        },
    )


def _classification(scores: dict[str, Any], horizon: str) -> str | None:
    score = scores.get(horizon)
    return score.classification if score else None


def _write_snapshot_json(settings: Settings, payload: dict[str, Any]) -> Path:
    as_of_date = str(payload["snapshot_meta"]["as_of_date"])
    instrument_id = payload["instrument"]["instrument_id"]
    directory = settings.gold_root / "stock_snapshots" / "json" / as_of_date
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{instrument_id}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _infer_score_as_of_date(settings: Settings) -> date | None:
    rows = _query_rows(
        settings,
        f"select max(as_of_date) as max_date from {_score_source(settings)}",
        [],
    )
    return date_or_none(rows[0].get("max_date")) if rows else None


def _load_score_rows(settings: Settings, as_of_date: date, limit: int) -> list[dict[str, Any]]:
    return _query_rows(
        settings,
        f"""
        select *
        from {_score_source(settings)}
        where as_of_date = ?
        order by instrument_id, horizon
        limit ?
        """,
        [as_of_date, max(1, limit) * 3],
    )


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


def _load_instruments(settings: Settings) -> dict[str, dict[str, Any]]:
    rows = _query_rows(settings, "select * from instruments", [])
    return {str(row["instrument_id"]): row for row in rows if row.get("instrument_id")}


def _load_listings(settings: Settings) -> list[dict[str, Any]]:
    return _query_rows(settings, "select * from listings", [])


def _score_source(settings: Settings) -> str:
    parquet_path = settings.gold_root / "score_snapshots" / "current.parquet"
    if parquet_path.exists():
        return _read_parquet_expr(parquet_path)
    return "score_snapshots"


def _feature_source(settings: Settings) -> str:
    parquet_path = settings.gold_root / "feature_snapshots" / "current.parquet"
    if parquet_path.exists():
        return _read_parquet_expr(parquet_path)
    return "feature_snapshots"


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


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        value = row.get(key)
        if value:
            output.setdefault(str(value), []).append(row)
    return output


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
