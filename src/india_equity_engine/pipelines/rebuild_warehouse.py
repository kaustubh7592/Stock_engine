"""Rebuild local DuckDB views from Parquet datasets."""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.storage.duckdb_store import DuckDBStore


def rebuild_duckdb_views(settings: Settings) -> JobRunResult:
    """Create DuckDB views for every available silver/gold Parquet table."""

    settings.ensure_runtime_dirs()
    candidates = _discover_parquet_sources(settings)
    if not candidates:
        return JobRunResult(
            job_name="rebuild_duckdb_views",
            status="failed",
            warnings=["No Parquet datasets found under silver or gold roots."],
        )

    refreshed, warnings = _refresh_views(settings.duckdb_path, candidates)
    output_path = settings.duckdb_path
    fallback_kind = None
    if _needs_fallback(refreshed, warnings, candidates):
        for kind, fallback_path in _fallback_duckdb_paths(settings.duckdb_path):
            fallback_refreshed, fallback_warnings = _refresh_views(fallback_path, candidates)
            warnings.append(
                f"Primary DuckDB appears locked; attempted {kind} warehouse at {fallback_path}."
            )
            warnings.extend(fallback_warnings)
            if len(fallback_refreshed) > len(refreshed):
                refreshed = fallback_refreshed
                output_path = fallback_path
                fallback_kind = kind
            if len(refreshed) == len(candidates):
                break

    return JobRunResult(
        job_name="rebuild_duckdb_views",
        status="success" if not warnings else "partial_success",
        records_in=len(candidates),
        records_out=len(refreshed),
        warnings=warnings,
        outputs={
            "duckdb_path": str(settings.duckdb_path),
            "output_duckdb_path": str(output_path),
            "fallback_duckdb": fallback_kind,
            "views_refreshed": refreshed,
            "view_count": len(refreshed),
            "source_count": len(candidates),
        },
    )


def _refresh_views(
    duckdb_path: Path,
    candidates: dict[str, Path],
) -> tuple[list[str], list[str]]:
    duckdb_store = DuckDBStore(duckdb_path)
    warnings = []
    refreshed = []
    for table_name, source_path in candidates.items():
        try:
            duckdb_store.refresh_parquet_view(table_name, source_path)
            refreshed.append(table_name)
        except duckdb.Error as exc:
            warnings.append(f"DuckDB view refresh failed for {table_name}: {exc}")
    return refreshed, warnings


def _looks_like_lock_failure(warnings: list[str]) -> bool:
    combined = "\n".join(warnings).lower()
    return ".wal" in combined and "access is denied" in combined


def _needs_fallback(
    refreshed: list[str],
    warnings: list[str],
    candidates: dict[str, Path],
) -> bool:
    return len(refreshed) < len(candidates) and _looks_like_lock_failure(warnings)


def _fallback_duckdb_paths(path: Path) -> list[tuple[str, Path]]:
    temp_dir = Path(tempfile.gettempdir()) / "india_equity_engine"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return [
        ("shadow", path.with_name(f"{path.stem}.rebuilt{path.suffix}")),
        ("temp", temp_dir / f"{path.stem}.rebuilt{path.suffix}"),
    ]


def _discover_parquet_sources(settings: Settings) -> dict[str, Path]:
    """Find table-level Parquet sources, preferring gold over silver."""

    candidates: dict[str, Path] = {}
    for root in (settings.silver_root, settings.gold_root):
        if not root.exists():
            continue
        for table_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            source = _table_source(table_dir)
            if source is not None:
                candidates[table_dir.name] = source
    return candidates


def _table_source(table_dir: Path) -> Path | None:
    current = table_dir / "current.parquet"
    if current.exists():
        return current
    parquet_files = sorted(table_dir.glob("*.parquet"))
    if parquet_files:
        return table_dir / "*.parquet"
    return None
