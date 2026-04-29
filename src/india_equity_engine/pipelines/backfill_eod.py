"""Date-range backfill helpers for EOD source pipelines."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from datetime import date, timedelta

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.ingest_derivatives_eod import ingest_derivatives_eod
from india_equity_engine.pipelines.ingest_market_eod import ingest_market_eod

BackfillFunc = Callable[[Settings, date | None], JobRunResult]


def backfill_market_eod(
    settings: Settings,
    *,
    from_date: date,
    to_date: date,
    continue_on_failure: bool = True,
) -> JobRunResult:
    """Backfill NSE cash-market EOD prices over a weekday date range."""

    return _backfill_dates(
        settings,
        job_name="backfill_market_eod",
        child_job_name="ingest_market_eod",
        from_date=from_date,
        to_date=to_date,
        func=ingest_market_eod,
        continue_on_failure=continue_on_failure,
    )


def backfill_derivatives_eod(
    settings: Settings,
    *,
    from_date: date,
    to_date: date,
    continue_on_failure: bool = True,
) -> JobRunResult:
    """Backfill NSE derivatives EOD contract rows over a weekday date range."""

    return _backfill_dates(
        settings,
        job_name="backfill_derivatives_eod",
        child_job_name="ingest_derivatives_eod",
        from_date=from_date,
        to_date=to_date,
        func=ingest_derivatives_eod,
        continue_on_failure=continue_on_failure,
    )


def _backfill_dates(
    settings: Settings,
    *,
    job_name: str,
    child_job_name: str,
    from_date: date,
    to_date: date,
    func: BackfillFunc,
    continue_on_failure: bool,
) -> JobRunResult:
    if from_date > to_date:
        return JobRunResult(
            job_name=job_name,
            status="failed",
            warnings=["from_date must be on or before to_date."],
        )

    settings.ensure_runtime_dirs()
    results = []
    warnings = []
    for trade_date in _weekdays(from_date, to_date):
        try:
            result = func(settings, trade_date)
        except Exception as exc:  # noqa: BLE001 - backfills should report and continue by default.
            result = JobRunResult(
                job_name=child_job_name,
                status="failed",
                warnings=[str(exc)],
                outputs={"trade_date": trade_date.isoformat()},
            )
        results.append(result)
        warnings.extend(
            f"{trade_date.isoformat()}: {warning}" for warning in result.warnings
        )
        if result.status == "failed" and not continue_on_failure:
            break

    status_counts = Counter(result.status for result in results)
    if not results:
        status = "failed"
        warnings.append("No weekday dates were available in the requested range.")
    elif status_counts.get("failed", 0) == 0 and status_counts.get("partial_success", 0) == 0:
        status = "success"
    elif status_counts.get("success", 0) or status_counts.get("partial_success", 0):
        status = "partial_success"
    else:
        status = "failed"

    return JobRunResult(
        job_name=job_name,
        status=status,
        records_in=sum(result.records_in for result in results),
        records_out=sum(result.records_out for result in results),
        warnings=warnings,
        outputs={
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "attempted_dates": [
                result.outputs.get("trade_date")
                for result in results
                if result.outputs.get("trade_date")
            ],
            "status_counts": dict(status_counts),
            "steps": [result.model_dump(mode="json") for result in results],
        },
    )


def _weekdays(from_date: date, to_date: date) -> list[date]:
    dates = []
    current = from_date
    while current <= to_date:
        if current.weekday() < 5:
            dates.append(current)
        current += timedelta(days=1)
    return dates
