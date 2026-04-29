from datetime import datetime, timezone
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.observability.job_log import record_job_run
from india_equity_engine.pipelines.run_data_quality_review import run_data_quality_review
from india_equity_engine.storage.parquet_store import ParquetStore


def test_record_job_run_writes_job_and_warning_issue(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    result = JobRunResult(
        job_name="example_job",
        status="partial_success",
        records_in=10,
        records_out=9,
        warnings=["one source was stale"],
    )

    logged = record_job_run(
        settings,
        result,
        started_at=datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 29, 10, 1, tzinfo=timezone.utc),
    )

    assert logged.status == "partial_success"
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        job = con.execute("select job_name, warning_count from job_runs").fetchone()
        issue = con.execute(
            "select table_name, rule_name, severity from data_quality_issues"
        ).fetchone()
    assert job == ("example_job", 1)
    assert issue == ("job_runs", "job_warning", "medium")


def test_data_quality_review_detects_duplicate_price_key(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    ParquetStore(settings.silver_root).write_current_records(
        [
            NormalizedRecord(
                table_name="price_daily",
                row={
                    "instrument_id": "INS_1",
                    "trade_date": "2026-04-28",
                    "open_price": 10.0,
                    "high_price": 12.0,
                    "low_price": 9.0,
                    "close_price": 11.0,
                },
            ),
            NormalizedRecord(
                table_name="price_daily",
                row={
                    "instrument_id": "INS_1",
                    "trade_date": "2026-04-28",
                    "open_price": 10.0,
                    "high_price": 12.0,
                    "low_price": 9.0,
                    "close_price": 11.0,
                },
            ),
        ]
    )

    result = run_data_quality_review(settings, max_age_days=30)

    assert result.status == "partial_success"
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        rules = {
            row[0]
            for row in con.execute(
                "select rule_name from data_quality_issues where table_name = 'price_daily'"
            ).fetchall()
        }
    assert "duplicate_primary_key" in rules


def test_data_quality_review_resolves_old_managed_issue(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    ParquetStore(settings.gold_root).write_current_records(
        [
            NormalizedRecord(
                table_name="data_quality_issues",
                row={
                    "dq_issue_id": "DQ_OLD",
                    "table_name": "job_runs",
                    "entity_key": "table",
                    "detected_at": datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
                    "rule_name": "missing_current_table",
                    "severity": "medium",
                    "issue_text": "No current Parquet table found for job_runs.",
                    "resolved_flag": False,
                    "resolution_note": None,
                },
            ),
            NormalizedRecord(
                table_name="job_runs",
                row={
                    "job_run_id": "JOB_1",
                    "job_name": "example",
                    "started_at": datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
                    "finished_at": datetime(2026, 4, 29, 10, 1, tzinfo=timezone.utc),
                    "status": "success",
                    "records_in": 0,
                    "records_out": 0,
                    "error_count": 0,
                    "warning_count": 0,
                    "log_path": "none",
                },
            ),
        ]
    )

    run_data_quality_review(settings, max_age_days=30)

    path = settings.gold_root / "data_quality_issues" / "current.parquet"
    escaped_path = str(path).replace("'", "''")
    with duckdb.connect() as con:
        resolved = con.execute(
            f"""
            select resolved_flag
            from read_parquet('{escaped_path}')
            where dq_issue_id = 'DQ_OLD'
            """
        ).fetchone()[0]
    assert resolved is True


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        config_dir=Path("configs").resolve(),
        data_root=tmp_path / "data",
        raw_root=tmp_path / "data" / "raw",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
        cache_root=tmp_path / "data" / "cache",
        logs_root=tmp_path / "data" / "logs",
        duckdb_path=tmp_path / "warehouse" / "test.duckdb",
        http_timeout_seconds=30,
        user_agent="test",
        use_system_cert_store=True,
        http_trust_env=False,
        ca_bundle=None,
        parser_version="test",
    )
