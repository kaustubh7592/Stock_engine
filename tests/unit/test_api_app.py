import json
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from india_equity_engine.app.api.main import create_app
from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.observability.job_log import record_job_run
from india_equity_engine.storage.parquet_store import ParquetStore


def test_api_reads_snapshot_and_scores_from_parquet(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    snapshot_path = settings.gold_root / "stock_snapshots" / "json" / "2026-04-28" / "INS_1.json"
    snapshot_path.parent.mkdir(parents=True)
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshot_meta": {"as_of_date": "2026-04-28"},
                "instrument": {"instrument_id": "INS_1", "name": "Example Industries Ltd"},
                "scores": {"long": {"classification": "bullish"}},
            }
        ),
        encoding="utf-8",
    )
    ParquetStore(settings.gold_root).write_current_records(
        [
            NormalizedRecord(
                table_name="stock_snapshots",
                row={
                    "instrument_id": "INS_1",
                    "as_of_date": date(2026, 4, 28),
                    "snapshot_version": "stock-snapshot-v1",
                    "short_horizon_classification": "abstain",
                    "medium_horizon_classification": "abstain",
                    "long_horizon_classification": "bullish",
                    "snapshot_json_path": str(snapshot_path),
                    "created_at": datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
                },
            ),
            NormalizedRecord(
                table_name="score_snapshots",
                row={
                    "instrument_id": "INS_1",
                    "as_of_date": date(2026, 4, 28),
                    "horizon": "long",
                    "technical_score": 0.55,
                    "fundamental_score": 0.70,
                    "governance_score": 0.80,
                    "composite_score": 0.72,
                    "confidence_score": 0.77,
                    "classification": "bullish",
                },
            ),
        ]
    )
    ParquetStore(settings.silver_root).write_current_records(
        [
            NormalizedRecord(
                table_name="instruments",
                row={
                    "instrument_id": "INS_1",
                    "isin": "INE000A01011",
                    "legal_name": "Example Industries Ltd",
                    "issuer_name": "Example Industries",
                    "sector_name": "Capital Goods",
                    "industry_name": "Industrial Machinery",
                },
            ),
            NormalizedRecord(
                table_name="listings",
                row={
                    "listing_id": "L1",
                    "instrument_id": "INS_1",
                    "exchange_code": "NSE",
                    "symbol": "EXAMPLE",
                    "bse_scrip_code": None,
                },
            ),
        ]
    )
    client = TestClient(create_app(settings))

    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "ok"

    status_response = client.get("/engine-status")
    assert status_response.status_code == 200
    assert status_response.json()["tables"]["score_snapshots"]["rows"] == 1

    list_response = client.get("/snapshots", params={"as_of_date": "2026-04-28"})
    assert list_response.status_code == 200
    assert list_response.json()["rows"][0]["instrument_id"] == "INS_1"

    snapshot_response = client.get("/snapshots/INS_1")
    assert snapshot_response.status_code == 200
    assert snapshot_response.json()["instrument"]["instrument_id"] == "INS_1"

    score_response = client.get("/scores/INS_1", params={"as_of_date": "2026-04-28"})
    assert score_response.status_code == 200
    assert score_response.json()["rows"][0]["classification"] == "bullish"

    stock_response = client.get("/stocks/EXAMPLE", params={"as_of_date": "2026-04-28"})
    assert stock_response.status_code == 200
    assert stock_response.json()["instrument"]["instrument_id"] == "INS_1"

    coverage_response = client.get("/coverage/EXAMPLE", params={"as_of_date": "2026-04-28"})
    assert coverage_response.status_code == 200
    assert coverage_response.json()["components"]["technical"]["ok"] is False


def test_api_rejects_bad_dates_and_unknown_jobs(tmp_path: Path) -> None:
    client = TestClient(create_app(_settings(tmp_path)))

    assert client.get("/snapshots", params={"as_of_date": "28-04-2026"}).status_code == 422
    assert client.post("/jobs/not-real/run").status_code == 400


def test_api_lists_observability_tables(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    record_job_run(
        settings,
        JobRunResult(
            job_name="example_job",
            status="partial_success",
            warnings=["example warning"],
        ),
        started_at=datetime(2026, 4, 29, 10, 0, tzinfo=timezone.utc),
        finished_at=datetime(2026, 4, 29, 10, 1, tzinfo=timezone.utc),
    )
    client = TestClient(create_app(settings))

    jobs = client.get("/job-runs")
    issues = client.get("/quality-issues")

    assert jobs.status_code == 200
    assert jobs.json()["rows"][0]["job_name"] == "example_job"
    assert issues.status_code == 200
    assert issues.json()["rows"][0]["rule_name"] == "job_warning"


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
