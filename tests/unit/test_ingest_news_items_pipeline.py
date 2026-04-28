from datetime import datetime, timezone
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.contracts import RawArtifact, RawArtifactRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines import ingest_news_items as pipeline


def test_ingest_news_items_pipeline_writes_duckdb_view(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    settings = _settings(tmp_path, db_path)

    def rss_download(self, source_object):
        fixture = (
            "pib_news_rss_sample.xml"
            if source_object.source.family == "pib"
            else "rbi_news_rss_sample.xml"
        )
        return RawArtifact(
            source_code=source_object.source.code,
            source_family=source_object.source.family,
            logical_name=source_object.logical_name,
            source_url=str(source_object.url),
            content=Path(f"tests/fixtures/{fixture}").read_bytes(),
            extension="xml",
            retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
            content_type="text/xml",
            metadata=source_object.metadata,
        )

    monkeypatch.setattr(pipeline.OfficialRSSNewsConnector, "download", rss_download)
    monkeypatch.setattr(pipeline.RawArtifactStore, "store", _raw_record(tmp_path))

    result = pipeline.ingest_news_items(
        settings,
        include_gdelt=False,
        include_official_pages=False,
    )

    assert result.status == "success"
    assert result.outputs["tables"] == {"news_items": 8}
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            "select event_type, source_class from news_items order by event_type, source_class"
        ).fetchall()
    assert ("budget_policy", "government") in rows
    assert ("macro_policy", "regulator") in rows


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
    def factory(self, artifact):
        return RawArtifactRecord(
            source_code=artifact.source_code,
            source_family=artifact.source_family,
            logical_name=artifact.logical_name,
            source_url=artifact.source_url,
            path=tmp_path / f"{artifact.logical_name}.raw",
            metadata_path=tmp_path / f"{artifact.logical_name}.raw.json",
            sha256=f"sha-{artifact.logical_name}",
            size_bytes=len(artifact.content),
            retrieved_at=artifact.retrieved_at,
            content_type=artifact.content_type,
            parser_version="test",
        )

    return factory
