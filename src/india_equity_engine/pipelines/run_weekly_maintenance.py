"""Run local weekly maintenance jobs."""

from __future__ import annotations

from pathlib import Path

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.backup_local_data import backup_local_data
from india_equity_engine.pipelines.rebuild_warehouse import rebuild_duckdb_views
from india_equity_engine.pipelines.run_data_quality_review import run_data_quality_review


def run_weekly_maintenance(
    settings: Settings,
    *,
    backup_dir: Path | None = None,
    include_backup: bool = True,
    dry_run_backup: bool = False,
) -> JobRunResult:
    """Run the weekend maintenance flow from the design document."""

    steps = [
        rebuild_duckdb_views(settings),
        run_data_quality_review(settings),
    ]
    if include_backup:
        steps.append(backup_local_data(settings, backup_dir=backup_dir, dry_run=dry_run_backup))
    return _combined_result("run_weekly_maintenance", steps)


def _combined_result(job_name: str, results: list[JobRunResult]) -> JobRunResult:
    status_counts = {result.status for result in results}
    if status_counts == {"success"}:
        status = "success"
    elif "success" in status_counts:
        status = "partial_success"
    else:
        status = "failed"
    warnings = [
        f"{result.job_name}: {warning}"
        for result in results
        for warning in result.warnings
    ]
    return JobRunResult(
        job_name=job_name,
        status=status,
        records_in=sum(result.records_in for result in results),
        records_out=sum(result.records_out for result in results),
        warnings=warnings,
        outputs={"steps": [result.model_dump(mode="json") for result in results]},
    )
