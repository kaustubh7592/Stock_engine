"""Local engine status summary helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.settings import Settings

STATUS_TABLES = (
    ("instruments", "silver", None),
    ("price_daily", "silver", "trade_date"),
    ("derivatives_eod", "silver", "trade_date"),
    ("macro_series", "silver", "observation_date"),
    ("market_flows", "silver", "trade_date"),
    ("event_signals", "silver", "event_date"),
    ("feature_snapshots", "gold", "as_of_date"),
    ("score_snapshots", "gold", "as_of_date"),
    ("stock_snapshots", "gold", "as_of_date"),
    ("llm_explanations", "gold", "as_of_date"),
    ("data_quality_issues", "gold", "detected_at"),
)


def engine_status(settings: Settings) -> dict[str, Any]:
    """Return a compact local status summary without triggering network work."""

    settings.ensure_runtime_dirs()
    tables = {}
    for table_name, layer, date_column in STATUS_TABLES:
        path = _current_path(settings, layer, table_name)
        tables[table_name] = _table_status(path, date_column)
    return {
        "duckdb_path": str(settings.duckdb_path),
        "data_root": str(settings.data_root),
        "tables": tables,
        "score_classifications": _score_classifications(settings),
        "latest_snapshot_paths": _latest_snapshot_paths(settings, limit=5),
    }


def _table_status(path: Path, date_column: str | None) -> dict[str, Any]:
    source = _parquet_source(path)
    if source is None:
        return {"exists": False, "rows": 0, "latest": None, "path": str(path)}
    try:
        with duckdb.connect() as con:
            if date_column is None:
                row = con.execute(
                    "select count(*) as rows, null as latest from read_parquet(?)",
                    [source],
                ).fetchone()
            else:
                row = con.execute(
                    f"select count(*) as rows, max({date_column}) as latest from read_parquet(?)",
                    [source],
                ).fetchone()
    except duckdb.Error as exc:
        return {
            "exists": True,
            "rows": None,
            "latest": None,
            "path": str(path),
            "error": str(exc),
        }
    return {
        "exists": True,
        "rows": int(row[0]) if row else 0,
        "latest": _json_value(row[1]) if row and row[1] is not None else None,
        "path": source,
    }


def _score_classifications(settings: Settings) -> dict[str, int]:
    path = _current_path(settings, "gold", "score_snapshots")
    if not path.exists():
        return {}
    try:
        with duckdb.connect() as con:
            rows = con.execute(
                """
                select horizon || ':' || classification as label, count(*) as rows
                from read_parquet(?)
                group by label
                order by label
                """,
                [str(path)],
            ).fetchall()
    except duckdb.Error:
        return {}
    return {str(label): int(count) for label, count in rows}


def _latest_snapshot_paths(settings: Settings, limit: int) -> list[str]:
    path = _current_path(settings, "gold", "stock_snapshots")
    if not path.exists():
        return []
    try:
        with duckdb.connect() as con:
            rows = con.execute(
                """
                select snapshot_json_path
                from read_parquet(?)
                where snapshot_json_path is not null
                order by as_of_date desc, instrument_id
                limit ?
                """,
                [str(path), limit],
            ).fetchall()
    except duckdb.Error:
        return []
    return [str(row[0]) for row in rows]


def _current_path(settings: Settings, layer: str, table_name: str) -> Path:
    root = settings.gold_root if layer == "gold" else settings.silver_root
    return root / table_name / "current.parquet"


def _parquet_source(current_path: Path) -> str | None:
    if current_path.exists():
        return str(current_path)
    table_dir = current_path.parent
    if not table_dir.exists():
        return None
    if any(table_dir.glob("*.parquet")):
        return str(table_dir / "*.parquet")
    return None


def _json_value(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
