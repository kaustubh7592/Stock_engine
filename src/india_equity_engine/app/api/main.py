"""Local FastAPI app for querying snapshots and triggering convenience jobs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query

from india_equity_engine import __version__
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_event_signals import build_event_signals
from india_equity_engine.pipelines.build_stock_snapshots import build_stock_snapshots
from india_equity_engine.pipelines.compute_features import compute_features
from india_equity_engine.pipelines.explain_snapshots import explain_snapshots
from india_equity_engine.pipelines.ingest_news_items import ingest_news_items
from india_equity_engine.pipelines.score_snapshots import score_snapshots


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create the local API app with optional test settings override."""

    app = FastAPI(title="India Equity Research Engine", version=__version__)

    def active_settings() -> Settings:
        return settings or Settings.load("configs")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/snapshots")
    def list_snapshots(
        as_of_date: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=1000),
    ) -> dict[str, Any]:
        resolved = active_settings()
        _validate_date_string(as_of_date)
        rows = _query_rows(
            resolved,
            f"""
            select
                instrument_id,
                as_of_date,
                snapshot_version,
                short_horizon_classification,
                medium_horizon_classification,
                long_horizon_classification,
                snapshot_json_path
            from {_table_source(resolved, "stock_snapshots")}
            where (? is null or cast(as_of_date as varchar) = ?)
            order by as_of_date desc, instrument_id
            limit ?
            """,
            [as_of_date, as_of_date, limit],
        )
        return {"count": len(rows), "rows": _json_ready(rows)}

    @app.get("/snapshots/{instrument_id}")
    def get_snapshot(
        instrument_id: str,
        as_of_date: str | None = Query(default=None),
    ) -> dict[str, Any]:
        resolved = active_settings()
        _validate_date_string(as_of_date)
        rows = _query_rows(
            resolved,
            f"""
            select snapshot_json_path
            from {_table_source(resolved, "stock_snapshots")}
            where instrument_id = ?
              and (? is null or cast(as_of_date as varchar) = ?)
            order by as_of_date desc
            limit 1
            """,
            [instrument_id, as_of_date, as_of_date],
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Snapshot not found.")
        path = Path(str(rows[0].get("snapshot_json_path") or ""))
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"Snapshot JSON missing: {path}")
        return _read_json(path)

    @app.get("/scores/{instrument_id}")
    def get_scores(
        instrument_id: str,
        as_of_date: str | None = Query(default=None),
    ) -> dict[str, Any]:
        resolved = active_settings()
        _validate_date_string(as_of_date)
        rows = _query_rows(
            resolved,
            f"""
            select *
            from {_table_source(resolved, "score_snapshots")}
            where instrument_id = ?
              and (? is null or cast(as_of_date as varchar) = ?)
            order by as_of_date desc, horizon
            """,
            [instrument_id, as_of_date, as_of_date],
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Scores not found.")
        return {"count": len(rows), "rows": _json_ready(rows)}

    @app.post("/jobs/{job_name}/run")
    def run_job(
        job_name: str,
        as_of_date: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=10000),
        include_gdelt: bool = Query(default=True),
        include_official_pages: bool = Query(default=True),
        gdelt_max_records: int = Query(default=50, ge=1, le=250),
    ) -> dict[str, Any]:
        resolved = active_settings()
        parsed_date = _parse_date(as_of_date)
        if job_name == "ingest-news-items":
            result = ingest_news_items(
                resolved,
                include_gdelt=include_gdelt,
                include_official_pages=include_official_pages,
                gdelt_max_records=gdelt_max_records,
            )
        elif job_name == "build-event-signals":
            result = build_event_signals(resolved, limit=limit)
        elif job_name == "compute-features":
            result = compute_features(resolved, as_of_date=parsed_date)
        elif job_name == "score-snapshots":
            result = score_snapshots(resolved, as_of_date=parsed_date)
        elif job_name == "build-stock-snapshots":
            result = build_stock_snapshots(resolved, as_of_date=parsed_date, limit=limit)
        elif job_name == "explain-snapshots":
            result = explain_snapshots(resolved, as_of_date=parsed_date, limit=limit)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported job: {job_name}")
        return result.model_dump(mode="json")

    return app


app = create_app()


def _table_source(settings: Settings, table_name: str) -> str:
    gold_path = settings.gold_root / table_name / "current.parquet"
    silver_path = settings.silver_root / table_name / "current.parquet"
    if gold_path.exists():
        return _read_parquet_expr(gold_path)
    if silver_path.exists():
        return _read_parquet_expr(silver_path)
    return table_name


def _read_parquet_expr(path: Path) -> str:
    escaped_path = str(path).replace("'", "''")
    return f"read_parquet('{escaped_path}')"


def _query_rows(settings: Settings, query: str, params: list[object]) -> list[dict[str, Any]]:
    try:
        if settings.duckdb_path.exists():
            con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        else:
            con = duckdb.connect()
        with con:
            result = con.execute(query, params)
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _json_ready(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        output.append({key: _json_value(value) for key, value in row.items()})
    return output


def _json_value(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _read_json(path: Path) -> dict[str, Any]:
    import json

    return json.loads(path.read_text(encoding="utf-8"))


def _parse_date(value: str | None):
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Use YYYY-MM-DD format.") from exc


def _validate_date_string(value: str | None) -> None:
    _parse_date(value)
