from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.parse_financial_facts import parse_financial_facts


def test_parse_financial_facts_pipeline_writes_duckdb_view(tmp_path: Path) -> None:
    artifact_path = tmp_path / "sample.xml"
    artifact_path.write_bytes(Path("tests/fixtures/sample_financial_facts_xbrl.xml").read_bytes())
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table filing_artifacts as
            select
                'artifact_1' as artifact_id,
                'filing_1' as filing_id,
                'instrument_1' as instrument_id,
                'XBRL' as document_type,
                ? as local_path,
                'https://nsearchives.nseindia.com/corporate/xbrl/sample.xml' as source_url,
                'abc' as document_hash,
                timestamp '2026-04-27 12:30:00' as available_at,
                date '2026-04-27' as as_of_date,
                'nse' as source,
                'success' as download_status
            """,
            [str(artifact_path)],
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

    result = parse_financial_facts(settings, limit=5)

    assert result.status == "success"
    assert result.outputs["tables"] == {"financial_facts": 3}
    with duckdb.connect(str(db_path), read_only=True) as con:
        assert con.execute("select count(*) from financial_facts").fetchone()[0] == 3
