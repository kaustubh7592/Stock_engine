from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import RawArtifact, RawArtifactRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines import ingest_insider_trades as pipeline


def test_ingest_insider_trades_pipeline_writes_duckdb_view(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table listings as
            select 'INS_360ONE' as instrument_id, 'NSE' as exchange_code, '360ONE' as symbol
            """
        )

    settings = Settings(
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

    monkeypatch.setattr(
        pipeline.NSEInsiderTradesConnector,
        "download",
        lambda self, source_object: RawArtifact(
            source_code="S09",
            source_family="nse",
            logical_name=source_object.logical_name,
            source_url=str(source_object.url),
            content=Path("tests/fixtures/nse_insider_trades_sample.json").read_bytes(),
            extension="json",
            retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
            content_type="application/json",
            metadata=source_object.metadata,
        ),
    )
    monkeypatch.setattr(
        pipeline.RawArtifactStore,
        "store",
        lambda self, artifact: RawArtifactRecord(
            source_code=artifact.source_code,
            source_family=artifact.source_family,
            logical_name=artifact.logical_name,
            source_url=artifact.source_url,
            path=tmp_path / "raw.json",
            metadata_path=tmp_path / "raw.json.json",
            sha256="abc",
            size_bytes=len(artifact.content),
            retrieved_at=artifact.retrieved_at,
            content_type=artifact.content_type,
            parser_version="test",
        ),
    )

    result = pipeline.ingest_insider_trades(
        settings,
        from_date=date(2026, 4, 24),
        to_date=date(2026, 4, 28),
    )

    assert result.status == "success"
    assert result.outputs["tables"] == {"insider_trades": 2}
    assert result.outputs["resolved_instrument_count"] == 2
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute(
            """
            select instrument_id, transaction_type, quantity, price, value_num, post_holding
            from insider_trades
            order by transaction_type desc
            """
        ).fetchone()
    assert row == (
        "INS_360ONE",
        "Sell",
        10000,
        Decimal("1041.4500"),
        Decimal("10414500.0000"),
        145152,
    )
