from decimal import Decimal
from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.parse_pledge_disclosures import parse_pledge_disclosures


def test_parse_pledge_disclosures_pipeline_writes_duckdb_view(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table financial_facts as
            select * from (
                values
                (
                    'instrument_1', 'filing_1', 'number_of_shares',
                    'taxonomy#NumberOfShares',
                    'ctx_promoter', 'ShareholdingOfPromoterAndPromoterGroupMember',
                    date '2026-03-31', false, 'shares', 18469116::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                ),
                (
                    'instrument_1', 'filing_1', 'number_of_shares_encumbered_under_pledged',
                    'taxonomy#NumberOfSharesEncumberedUnderPledged',
                    'ctx_promoter', 'ShareholdingOfPromoterAndPromoterGroupMember',
                    date '2026-03-31', false, 'shares', 2900000::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                ),
                (
                    'instrument_1', 'filing_1',
                    'encumbered_share_under_pledged_as_percentage_of_total_number_of_shares',
                    'taxonomy#EncumberedShareUnderPledgedAsPercentageOfTotalNumberOfShares',
                    'ctx_promoter', 'ShareholdingOfPromoterAndPromoterGroupMember',
                    date '2026-03-31', false, 'pure', 0.1570::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                ),
                (
                    'instrument_1', 'filing_1',
                    'encumbered_share_under_pledged_as_percentage_of_total_number_of_shares',
                    'taxonomy#EncumberedShareUnderPledgedAsPercentageOfTotalNumberOfShares',
                    'ctx_total', 'ShareholdingPatternMember',
                    date '2026-03-31', false, 'pure', 0.0876::decimal(18,4),
                    'nse', 'https://example.test/xbrl.xml',
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00',
                    date '2026-04-27', 'abc', 'test', false,
                    timestamp '2026-04-27 12:30:00',
                    timestamp '2026-04-27 12:30:00'
                )
            ) as t(
                instrument_id, filing_id, concept_name, taxonomy_concept, context_id,
                context_text, period_end, consolidated_flag, unit, value_num, source,
                source_url, retrieved_at, available_at, as_of_date, document_hash,
                parser_version, restated_flag, created_at, updated_at
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

    result = parse_pledge_disclosures(settings, limit=10)

    assert result.status == "success"
    assert result.outputs["tables"] == {"pledge_disclosures": 1}
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute(
            """
            select promoter_shares, pledged_shares, pledged_pct_promoter_holding,
                   pledged_pct_total_equity
            from pledge_disclosures
            """
        ).fetchone()
    assert row == (18469116, 2900000, Decimal("15.7000"), Decimal("8.7600"))
