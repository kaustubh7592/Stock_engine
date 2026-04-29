"""Create local backup archives for configs, warehouse, and data layers."""

from __future__ import annotations

import json
import zipfile
from pathlib import Path

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.observability.job_log import utc_now


def backup_local_data(
    settings: Settings,
    *,
    backup_dir: Path | None = None,
    include_raw: bool = True,
    include_silver: bool = True,
    include_gold: bool = True,
    include_logs: bool = True,
    dry_run: bool = False,
) -> JobRunResult:
    """Create a local zip backup with a reproducibility manifest."""

    settings.ensure_runtime_dirs()
    created_at = utc_now()
    target_dir = (backup_dir or settings.data_root / "backups").resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    archive_path = target_dir / f"india_equity_engine_backup_{created_at:%Y%m%dT%H%M%SZ}.zip"
    files = _collect_files(
        settings,
        target_dir=target_dir,
        include_raw=include_raw,
        include_silver=include_silver,
        include_gold=include_gold,
        include_logs=include_logs,
    )
    manifest = {
        "created_at": created_at.isoformat(),
        "data_root": str(settings.data_root),
        "config_dir": str(settings.config_dir),
        "duckdb_path": str(settings.duckdb_path),
        "include_raw": include_raw,
        "include_silver": include_silver,
        "include_gold": include_gold,
        "include_logs": include_logs,
        "dry_run": dry_run,
        "archive_path": str(archive_path),
        "file_count": len(files),
        "total_bytes": sum(path.stat().st_size for path, _ in files if path.exists()),
        "files": [label for _, label in files],
    }
    if dry_run:
        return JobRunResult(
            job_name="backup_local_data",
            status="success",
            records_in=len(files),
            records_out=0,
            outputs=manifest,
        )

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest, indent=2, sort_keys=True))
        for path, label in files:
            archive.write(path, label)

    return JobRunResult(
        job_name="backup_local_data",
        status="success",
        records_in=len(files),
        records_out=len(files) + 1,
        outputs={
            **manifest,
            "archive_path": str(archive_path),
            "archive_bytes": archive_path.stat().st_size,
        },
    )


def _collect_files(
    settings: Settings,
    *,
    target_dir: Path,
    include_raw: bool,
    include_silver: bool,
    include_gold: bool,
    include_logs: bool,
) -> list[tuple[Path, str]]:
    roots: list[tuple[Path, str]] = [(settings.config_dir, "configs")]
    if include_raw:
        roots.append((settings.raw_root, "data/raw"))
    if include_silver:
        roots.append((settings.silver_root, "data/silver"))
    if include_gold:
        roots.append((settings.gold_root, "data/gold"))
    if include_logs:
        roots.append((settings.logs_root, "data/logs"))

    files: list[tuple[Path, str]] = []
    for root, label_root in roots:
        if not root.exists():
            continue
        for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
            if _should_skip(path, target_dir):
                continue
            files.append((path, (Path(label_root) / path.relative_to(root)).as_posix()))
    warehouse_files = sorted(settings.duckdb_path.parent.glob("*.duckdb"))
    for path in warehouse_files:
        if not _should_skip(path, target_dir):
            files.append((path, (Path("data/warehouse") / path.name).as_posix()))
    return files


def _should_skip(path: Path, target_dir: Path) -> bool:
    suffixes = "".join(path.suffixes).lower()
    if suffixes.endswith(".wal") or suffixes.endswith(".tmp"):
        return True
    try:
        path.resolve().relative_to(target_dir)
        return True
    except ValueError:
        return False
