import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_event_signals import build_event_signals
from india_equity_engine.storage.duckdb_store import DuckDBStore
from india_equity_engine.storage.parquet_store import ParquetStore


def test_build_event_signals_pipeline_writes_duckdb_view(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    settings = _settings(tmp_path, db_path)
    now = datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc)
    news_record = NormalizedRecord(
        table_name="news_items",
        row={
            "news_id": "news-1",
            "published_at": now,
            "source_name": "RBI",
            "source_class": "regulator",
            "headline": "Monetary policy repo rate and liquidity measures announced",
            "url": "https://www.rbi.org.in/example",
            "language_code": "en",
            "geography": "India",
            "event_type": "macro_policy",
            "sentiment_score": None,
            "novelty_score": Decimal("1"),
            "entities_json": json.dumps({"exposure_channels": ["rates_liquidity"]}),
            "source": "rbi",
            "source_url": "https://www.rbi.org.in/Scripts/rss.aspx",
            "retrieved_at": now,
            "available_at": now,
            "as_of_date": now.date(),
            "document_hash": "sha",
            "parser_version": "test",
            "restated_flag": False,
            "created_at": now,
            "updated_at": now,
        },
    )
    result = ParquetStore(settings.silver_root).write_current_records([news_record])[0]
    DuckDBStore(settings.duckdb_path).refresh_parquet_view("news_items", result.path)

    job = build_event_signals(settings, limit=20)

    assert job.status == "success"
    assert job.outputs["tables"] == {"event_signals": 5}
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select sector_name, exposure_channel, confidence
            from event_signals
            order by sector_name
            """
        ).fetchall()
    assert ("Banks", "rates_liquidity", Decimal("0.760")) in rows


def _settings(tmp_path: Path, db_path: Path) -> Settings:
    return Settings(
        config_dir=Path("configs").resolve(),
        data_root=tmp_path / "data",
        raw_root=tmp_path / "data" / "raw",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
        cache_root=tmp_path / "data" / "cache",
        logs_root=tmp_path / "data" / "logs",
        duckdb_path=db_path,
        http_timeout_seconds=30,
        user_agent="test",
        use_system_cert_store=True,
        http_trust_env=False,
        ca_bundle=None,
        parser_version="test",
    )
