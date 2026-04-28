from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import RawArtifact, RawArtifactRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines import ingest_market_flows as pipeline


def test_ingest_market_flows_pipeline_writes_duckdb_view(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    settings = _settings(tmp_path, db_path)

    monkeypatch.setattr(
        pipeline.NSDLFPIFlowsConnector,
        "download",
        lambda self, source_object: RawArtifact(
            source_code="S17",
            source_family="nsdl",
            logical_name=source_object.logical_name,
            source_url="https://pilot.fpi.nsdl.co.in/Reports/Latest.aspx",
            content=Path("tests/fixtures/nsdl_fpi_latest_sample.html").read_bytes(),
            extension="html",
            retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
            content_type="text/html",
            metadata={"host": "pilot.fpi.nsdl.co.in"},
        ),
    )
    monkeypatch.setattr(pipeline.RawArtifactStore, "store", _raw_record(tmp_path))

    result = pipeline.ingest_market_flows(settings)

    assert result.status == "success"
    assert result.outputs["tables"] == {"market_flows": 6}
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute(
            """
            select trade_date, segment, flow_type, gross_buy, gross_sell, net_flow
            from market_flows
            where segment = 'equity' and flow_type = 'fpi_net_investment_subtotal'
            """
        ).fetchone()
    assert row == (
        datetime(2025, 10, 30).date(),
        "equity",
        "fpi_net_investment_subtotal",
        Decimal("12594.6000"),
        Decimal("13374.9400"),
        Decimal("-780.3400"),
    )


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


def _raw_record(tmp_path: Path):
    return lambda self, artifact: RawArtifactRecord(
        source_code=artifact.source_code,
        source_family=artifact.source_family,
        logical_name=artifact.logical_name,
        source_url=artifact.source_url,
        path=tmp_path / "raw.html",
        metadata_path=tmp_path / "raw.html.json",
        sha256="abc",
        size_bytes=len(artifact.content),
        retrieved_at=artifact.retrieved_at,
        content_type=artifact.content_type,
        parser_version="test",
    )
