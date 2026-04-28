from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.compute_features import compute_features


def test_compute_features_pipeline_writes_gold_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    _seed_source_tables(db_path)
    settings = _settings(tmp_path, db_path)

    result = compute_features(settings, as_of_date=None)

    assert result.status == "success"
    assert result.outputs["tables"] == {"feature_snapshots": result.records_out}
    assert result.outputs["feature_families"]["technical"] == 8
    assert result.outputs["feature_families"]["governance"] == 7
    assert result.outputs["feature_families"]["fundamental"] == 18
    assert (tmp_path / "data" / "gold" / "feature_snapshots" / "current.parquet").exists()
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select feature_family, feature_name, value_num, coverage_flag
            from feature_snapshots
            where instrument_id = 'INS_1'
              and feature_name in (
                'return_5d_pct',
                'return_20d_pct',
                'promoter_pledged_pct_total_equity_latest',
                'fundamental_debt_to_assets',
                'shareholding_promoter_pct_change_1p'
              )
            order by feature_name
            """
        ).fetchall()
    by_name = {row[1]: row for row in rows}
    assert by_name["fundamental_debt_to_assets"][2] is not None
    assert by_name["fundamental_debt_to_assets"][3] is True
    assert by_name["return_20d_pct"] == ("technical", "return_20d_pct", None, False)
    assert by_name["return_5d_pct"][2] is not None
    assert by_name["return_5d_pct"][3] is True


def _seed_source_tables(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table price_daily (
                instrument_id text,
                trade_date date,
                open_price decimal(18,4),
                high_price decimal(18,4),
                low_price decimal(18,4),
                close_price decimal(18,4),
                volume bigint,
                traded_value decimal(18,4),
                deliverable_qty bigint,
                deliverable_pct decimal(18,4),
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into price_daily values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "INS_1",
                    "2026-04-21",
                    95.0,
                    101.0,
                    94.0,
                    100.0,
                    1000,
                    100000.0,
                    500,
                    50.0,
                    "2026-04-21 16:00:00",
                ),
                (
                    "INS_1",
                    "2026-04-22",
                    100.0,
                    103.0,
                    99.0,
                    102.0,
                    1100,
                    112200.0,
                    540,
                    49.1,
                    "2026-04-22 16:00:00",
                ),
                (
                    "INS_1",
                    "2026-04-23",
                    102.0,
                    105.0,
                    101.0,
                    104.0,
                    1200,
                    124800.0,
                    620,
                    51.7,
                    "2026-04-23 16:00:00",
                ),
                (
                    "INS_1",
                    "2026-04-24",
                    104.0,
                    107.0,
                    103.0,
                    106.0,
                    1300,
                    137800.0,
                    700,
                    53.8,
                    "2026-04-24 16:00:00",
                ),
                (
                    "INS_1",
                    "2026-04-27",
                    106.0,
                    109.0,
                    105.0,
                    108.0,
                    1400,
                    151200.0,
                    760,
                    54.3,
                    "2026-04-27 16:00:00",
                ),
                (
                    "INS_1",
                    "2026-04-28",
                    108.0,
                    112.0,
                    107.0,
                    110.0,
                    1500,
                    165000.0,
                    820,
                    54.7,
                    "2026-04-28 16:00:00",
                ),
            ],
        )
        con.execute(
            """
            create table governance_events (
                governance_event_id text,
                instrument_id text,
                event_date date,
                event_type text,
                severity text,
                risk_flag boolean,
                available_at timestamp
            )
            """
        )
        con.execute(
            "insert into governance_events values (?, ?, ?, ?, ?, ?, ?)",
            (
                "gov_1",
                "INS_1",
                "2026-04-20",
                "auditor_resignation",
                "high",
                True,
                "2026-04-20 12:00:00",
            ),
        )
        con.execute(
            """
            create table pledge_disclosures (
                pledge_id text,
                instrument_id text,
                period_end date,
                pledged_pct_promoter_holding decimal(18,4),
                pledged_pct_total_equity decimal(18,4),
                available_at timestamp
            )
            """
        )
        con.execute(
            "insert into pledge_disclosures values (?, ?, ?, ?, ?, ?)",
            ("pledge_1", "INS_1", "2026-03-31", 20.0, 7.5, "2026-04-10 12:00:00"),
        )
        con.execute(
            """
            create table insider_trades (
                insider_trade_id text,
                instrument_id text,
                transaction_date date,
                transaction_type text,
                value_num decimal(18,4),
                available_at timestamp
            )
            """
        )
        con.execute(
            "insert into insider_trades values (?, ?, ?, ?, ?, ?)",
            ("trade_1", "INS_1", "2026-04-27", "Sell", 1000000.0, "2026-04-27 12:00:00"),
        )
        con.execute(
            """
            create table shareholding_pattern (
                instrument_id text,
                period_end date,
                promoter_pct decimal(18,4),
                public_pct decimal(18,4),
                fii_pct decimal(18,4),
                dii_pct decimal(18,4),
                retail_pct decimal(18,4),
                other_pct decimal(18,4),
                share_count bigint,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into shareholding_pattern values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "INS_1",
                    "2025-12-31",
                    52.0,
                    48.0,
                    12.0,
                    8.0,
                    18.0,
                    10.0,
                    1000000,
                    "2026-01-10 12:00:00",
                ),
                (
                    "INS_1",
                    "2026-03-31",
                    51.0,
                    49.0,
                    13.0,
                    9.0,
                    17.0,
                    10.0,
                    1100000,
                    "2026-04-10 12:00:00",
                ),
            ],
        )
        con.execute(
            """
            create table financial_facts (
                instrument_id text,
                concept_name text,
                taxonomy_concept text,
                period_end date,
                consolidated_flag boolean,
                value_num decimal(18,4),
                document_hash text,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into financial_facts values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "INS_1",
                    "RevenueFromOperations",
                    "RevenueFromOperations",
                    "2025-12-31",
                    True,
                    1000.0,
                    "hash1",
                    "2026-01-15 12:00:00",
                ),
                (
                    "INS_1",
                    "ProfitLossForPeriod",
                    "ProfitLossForPeriod",
                    "2025-12-31",
                    True,
                    100.0,
                    "hash1",
                    "2026-01-15 12:00:00",
                ),
                (
                    "INS_1",
                    "RevenueFromOperations",
                    "RevenueFromOperations",
                    "2026-03-31",
                    True,
                    1200.0,
                    "hash2",
                    "2026-04-15 12:00:00",
                ),
                (
                    "INS_1",
                    "ProfitLossForPeriod",
                    "ProfitLossForPeriod",
                    "2026-03-31",
                    True,
                    150.0,
                    "hash2",
                    "2026-04-15 12:00:00",
                ),
                (
                    "INS_1",
                    "TotalAssets",
                    "TotalAssets",
                    "2026-03-31",
                    True,
                    5000.0,
                    "hash2",
                    "2026-04-15 12:00:00",
                ),
                (
                    "INS_1",
                    "Borrowings",
                    "Borrowings",
                    "2026-03-31",
                    True,
                    1000.0,
                    "hash2",
                    "2026-04-15 12:00:00",
                ),
            ],
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
