import zipfile
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.backup_local_data import backup_local_data
from india_equity_engine.pipelines.rebuild_warehouse import rebuild_duckdb_views
from india_equity_engine.pipelines.run_weekly_maintenance import run_weekly_maintenance
from india_equity_engine.storage.parquet_store import ParquetStore


def test_rebuild_duckdb_views_uses_dated_parquet_files(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    ParquetStore(settings.silver_root).write_records(
        [
            NormalizedRecord(
                table_name="price_daily",
                row={
                    "instrument_id": "INS_1",
                    "trade_date": "2026-04-28",
                    "close_price": 100.0,
                },
            )
        ],
        "trade_date_20260428",
    )

    result = rebuild_duckdb_views(settings)

    assert result.status == "success"
    with duckdb.connect(str(settings.duckdb_path), read_only=True) as con:
        row = con.execute("select instrument_id, close_price from price_daily").fetchone()
    assert row == ("INS_1", 100.0)


def test_backup_local_data_writes_manifest_and_archive(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    settings.raw_root.mkdir(parents=True)
    (settings.raw_root / "sample.txt").write_text("raw", encoding="utf-8")
    backup_dir = tmp_path / "backups"

    result = backup_local_data(
        settings,
        backup_dir=backup_dir,
        include_raw=True,
        include_silver=False,
        include_gold=False,
        include_logs=False,
    )

    archive_path = Path(result.outputs["archive_path"])
    assert archive_path.exists()
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
    assert "manifest.json" in names
    assert "data/raw/sample.txt" in names


def test_weekly_maintenance_can_run_without_writing_backup(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    ParquetStore(settings.gold_root).write_current_records(
        [
            NormalizedRecord(
                table_name="job_runs",
                row={
                    "job_run_id": "JOB_1",
                    "job_name": "example",
                    "started_at": "2026-04-29T10:00:00Z",
                    "finished_at": "2026-04-29T10:01:00Z",
                    "status": "success",
                    "records_in": 0,
                    "records_out": 0,
                    "error_count": 0,
                    "warning_count": 0,
                    "log_path": "none",
                },
            )
        ]
    )

    result = run_weekly_maintenance(settings, include_backup=True, dry_run_backup=True)

    assert result.status in {"success", "partial_success"}
    assert [step["job_name"] for step in result.outputs["steps"]] == [
        "rebuild_duckdb_views",
        "run_data_quality_review",
        "backup_local_data",
    ]
    assert result.outputs["steps"][-1]["outputs"]["dry_run"] is True


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
