from datetime import date
from pathlib import Path

from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines import backfill_eod


def test_backfill_market_eod_skips_weekends_and_collects_results(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = []

    def fake_ingest(settings: Settings, trade_date: date | None = None) -> JobRunResult:
        calls.append(trade_date)
        return JobRunResult(
            job_name="ingest_market_eod",
            status="success",
            records_in=1,
            records_out=1,
            outputs={"trade_date": trade_date.isoformat()},
        )

    monkeypatch.setattr(backfill_eod, "ingest_market_eod", fake_ingest)
    settings = _settings(tmp_path)

    result = backfill_eod.backfill_market_eod(
        settings,
        from_date=date(2026, 4, 24),
        to_date=date(2026, 4, 28),
    )

    assert result.status == "success"
    assert calls == [date(2026, 4, 24), date(2026, 4, 27), date(2026, 4, 28)]
    assert result.records_out == 3


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
