from decimal import Decimal
from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.parse_shareholding_pattern import parse_shareholding_pattern


def test_parse_shareholding_pattern_pipeline_writes_duckdb_view(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table financial_facts as
            select * from (
                values
                (
                    'instrument_1', 'filing_1',
                    'promoter_and_promoter_group_shareholding_percentage',
                    'taxonomy#PromoterAndPromoterGroupShareholdingPercentage',
                    date '2026-03-31', false, 'pure', 51.2::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                ),
                (
                    'instrument_1', 'filing_1', 'public_shareholding_percentage',
                    'taxonomy#PublicShareholdingPercentage',
                    date '2026-03-31', false, 'pure', 48.8::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                )
            ) as t(
                instrument_id, filing_id, concept_name, taxonomy_concept, period_end,
                consolidated_flag, unit, value_num, source, source_url, retrieved_at,
                available_at, as_of_date, document_hash, parser_version, restated_flag,
                created_at, updated_at
            )
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

    result = parse_shareholding_pattern(settings, limit=10)

    assert result.status == "success"
    assert result.outputs["tables"] == {"shareholding_pattern": 1}
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute(
            "select promoter_pct, public_pct from shareholding_pattern"
        ).fetchone()
    assert row == (Decimal("51.2000"), Decimal("48.8000"))
