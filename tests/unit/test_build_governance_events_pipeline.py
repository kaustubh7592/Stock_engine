from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_governance_events import build_governance_events


def test_build_governance_events_pipeline_writes_duckdb_view(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table corporate_announcements as
            select * from (
                values (
                    'ann_1', 'INS_1', timestamp '2026-04-28 10:30:00',
                    'Resignation of Statutory Auditor',
                    'Auditor has resigned due to pre-occupation',
                    null, 'https://example.test/auditor.pdf', 'nse', 'https://example.test/ann.xml',
                    timestamp '2026-04-28 10:31:00',
                    timestamp '2026-04-28 10:30:00',
                    date '2026-04-28', 'hash_ann', 'test', false,
                    timestamp '2026-04-28 10:31:00',
                    timestamp '2026-04-28 10:31:00'
                )
            ) as t(
                announcement_id, instrument_id, announced_at, headline, summary_text,
                sub_category, attachment_url, source, source_url, retrieved_at, available_at,
                as_of_date, document_hash, parser_version, restated_flag, created_at, updated_at
            )
            """
        )
        con.execute(
            """
            create table pledge_disclosures as
            select * from (
                values (
                    'pledge_1', 'INS_2', 'filing_1', date '2026-03-31',
                    1000000, 150000, 15.0::decimal(18,4), 6.0::decimal(18,4), null,
                    'nse', 'https://example.test/pledge.xml',
                    timestamp '2026-04-28 11:00:00',
                    timestamp '2026-04-28 11:00:00',
                    date '2026-03-31', 'hash_pledge', 'test', false,
                    timestamp '2026-04-28 11:00:00',
                    timestamp '2026-04-28 11:00:00'
                )
            ) as t(
                pledge_id, instrument_id, filing_id, period_end, promoter_shares,
                pledged_shares, pledged_pct_promoter_holding, pledged_pct_total_equity,
                release_or_creation_flag, source, source_url, retrieved_at, available_at,
                as_of_date, document_hash, parser_version, restated_flag, created_at, updated_at
            )
            """
        )
        con.execute(
            """
            create table insider_trades as
            select * from (
                values (
                    'trade_1', 'INS_3', 'filing_2', 'Example Promoter', 'Promoter',
                    date '2026-04-27', 'Sell', 10000, 100.0::decimal(18,4),
                    1000000.0::decimal(18,4), 90000,
                    'nse', 'https://example.test/pit.xml',
                    timestamp '2026-04-28 12:00:00',
                    timestamp '2026-04-28 12:00:00',
                    date '2026-04-27', 'hash_trade', 'test', false,
                    timestamp '2026-04-28 12:00:00',
                    timestamp '2026-04-28 12:00:00'
                )
            ) as t(
                insider_trade_id, instrument_id, filing_id, insider_name, insider_category,
                transaction_date, transaction_type, quantity, price, value_num, post_holding,
                source, source_url, retrieved_at, available_at, as_of_date, document_hash,
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

    result = build_governance_events(settings, limit=20)

    assert result.status == "success"
    assert result.outputs["tables"] == {"governance_events": 3}
    assert result.outputs["event_types"] == {
        "auditor_resignation": 1,
        "promoter_pledge": 1,
        "insider_activity": 1,
    }
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select event_type, severity, risk_flag
            from governance_events
            order by event_type
            """
        ).fetchall()
    assert rows == [
        ("auditor_resignation", "high", True),
        ("insider_activity", "medium", True),
        ("promoter_pledge", "medium", True),
    ]
