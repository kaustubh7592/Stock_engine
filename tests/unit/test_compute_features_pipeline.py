from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.compute_features import compute_features
from india_equity_engine.storage.parquet_store import ParquetStore


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
    assert result.outputs["feature_families"]["macro"] == 5
    assert result.outputs["feature_families"]["derivatives"] == 5
    assert result.outputs["feature_families"]["event"] == 4
    assert result.outputs["feature_families"]["peer"] == 4
    assert (tmp_path / "data" / "gold" / "feature_snapshots" / "current.parquet").exists()
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select feature_family, feature_name, value_num, coverage_flag
            from feature_snapshots
            where instrument_id = 'INS_1'
              and feature_name in (
                'return_5d_pct',
                'close_price',
                'return_20d_pct',
                'promoter_pledged_pct_total_equity_latest',
                'fundamental_debt_to_assets',
                'macro_cpi_latest',
                'macro_fpi_total_net_flow_latest',
                'derivatives_put_call_oi_ratio_latest',
                'event_net_impact_30d',
                'peer_return_5d_rank_pct',
                'shareholding_promoter_pct_change_1p'
              )
            order by feature_name
            """
        ).fetchall()
    by_name = {row[1]: row for row in rows}
    assert by_name["close_price"][2] == 110
    assert by_name["fundamental_debt_to_assets"][2] is not None
    assert by_name["fundamental_debt_to_assets"][3] is True
    assert by_name["return_20d_pct"] == ("technical", "return_20d_pct", None, False)
    assert by_name["return_5d_pct"][2] is not None
    assert by_name["return_5d_pct"][3] is True
    assert by_name["macro_cpi_latest"][2] is not None
    assert by_name["macro_fpi_total_net_flow_latest"][2] is not None
    assert by_name["derivatives_put_call_oi_ratio_latest"][2] is not None
    assert by_name["event_net_impact_30d"][2] is not None
    assert by_name["peer_return_5d_rank_pct"][3] is False


def test_compute_features_reads_price_parquet_without_duckdb_view(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    settings = _settings(tmp_path, db_path)
    records = []
    for day, close in (
        (date(2026, 4, 24), 100.0),
        (date(2026, 4, 27), 102.0),
        (date(2026, 4, 28), 104.0),
        (date(2026, 4, 29), 106.0),
    ):
        records.append(
            NormalizedRecord(
                table_name="price_daily",
                row={
                    "instrument_id": "INS_1",
                    "trade_date": day,
                    "open_price": close - 1,
                    "high_price": close + 1,
                    "low_price": close - 2,
                    "close_price": close,
                    "volume": 1000,
                    "traded_value": close * 1000,
                    "deliverable_qty": 500,
                    "deliverable_pct": 50.0,
                    "available_at": "2026-04-29 16:00:00",
                },
            )
        )
    ParquetStore(settings.silver_root).write_current_records(records)

    result = compute_features(
        settings,
        as_of_date=date(2026, 4, 29),
        families=("technical",),
    )

    assert result.status == "success"
    assert result.records_in == 4
    assert result.outputs["feature_families"] == {"technical": 8}


def test_compute_features_reads_daily_price_parquets_with_different_columns(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    settings = _settings(tmp_path, db_path)
    price_dir = settings.silver_root / "price_daily"
    price_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "INS_1",
                    "trade_date": date(2026, 4, 24),
                    "open_price": 99.0,
                    "high_price": 101.0,
                    "low_price": 98.0,
                    "close_price": 100.0,
                    "volume": 1000,
                    "traded_value": 100000.0,
                    "deliverable_qty": 500,
                    "deliverable_pct": 50.0,
                }
            ]
        ),
        price_dir / "trade_date_20260424.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "instrument_id": "INS_1",
                    "trade_date": date(2026, 4, 29),
                    "open_price": 105.0,
                    "high_price": 107.0,
                    "low_price": 104.0,
                    "close_price": 106.0,
                    "volume": 1100,
                    "traded_value": 116600.0,
                    "deliverable_qty": 550,
                    "deliverable_pct": 51.0,
                    "available_at": "2026-04-29 16:00:00",
                }
            ]
        ),
        price_dir / "trade_date_20260429.parquet",
    )

    result = compute_features(
        settings,
        as_of_date=date(2026, 4, 29),
        families=("technical",),
    )

    assert result.status == "success"
    assert result.records_in == 2
    assert result.outputs["feature_families"] == {"technical": 8}


def test_compute_features_reads_only_latest_derivatives_date(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table derivatives_eod (
                contract_id text,
                instrument_id text,
                trade_date date,
                segment text,
                expiry_date date,
                strike_price decimal(18,4),
                option_type text,
                settlement_price decimal(18,4),
                open_interest bigint,
                oi_change bigint,
                contract_volume bigint,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into derivatives_eod values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "OLD",
                    "INS_1",
                    "2026-04-28",
                    "FUTSTK",
                    "2026-04-30",
                    0,
                    None,
                    100,
                    100000,
                    1000,
                    50,
                    "2026-04-28 17:00:00",
                ),
                (
                    "NEW_CE",
                    "INS_1",
                    "2026-04-30",
                    "OPTSTK",
                    "2026-05-28",
                    100,
                    "CE",
                    10,
                    200000,
                    2000,
                    60,
                    "2026-04-30 17:00:00",
                ),
                (
                    "NEW_PE",
                    "INS_1",
                    "2026-04-30",
                    "OPTSTK",
                    "2026-05-28",
                    100,
                    "PE",
                    11,
                    250000,
                    2500,
                    70,
                    "2026-04-30 17:00:00",
                ),
            ],
        )
    settings = _settings(tmp_path, db_path)

    result = compute_features(
        settings,
        as_of_date=date(2026, 4, 30),
        families=("derivatives",),
    )

    assert result.status == "success"
    assert result.records_in == 2
    assert result.outputs["feature_families"] == {"derivatives": 5}


def _seed_source_tables(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table instruments (
                instrument_id text,
                sector_name text,
                industry_name text
            )
            """
        )
        con.execute(
            "insert into instruments values (?, ?, ?)",
            ("INS_1", "Banks", "Private Banks"),
        )
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
        con.execute(
            """
            create table macro_series (
                series_code text,
                series_name text,
                source_family text,
                observation_date date,
                value_num decimal(18,4),
                unit text,
                frequency text,
                vintage_date date,
                seasonal_adjustment text,
                source_url text,
                document_hash text,
                parser_version text,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into macro_series values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "repo",
                    "Policy Repo Rate",
                    "RBI",
                    "2026-04-28",
                    5.25,
                    "percent",
                    "daily",
                    "2026-04-28",
                    "unknown",
                    "https://rbi.org.in",
                    "hash3",
                    "test",
                    "2026-04-28 12:00:00",
                ),
                (
                    "cpi",
                    "MoSPI CPI inflation latest release",
                    "MOSPI",
                    "2026-03-31",
                    3.34,
                    "percent",
                    "monthly",
                    "2026-04-29",
                    "NSA",
                    "https://mospi.gov.in",
                    "hash4",
                    "test",
                    "2026-04-29 12:00:00",
                ),
            ],
        )
        con.execute(
            """
            create table market_flows (
                trade_date date,
                flow_type text,
                segment text,
                investor_class text,
                gross_buy decimal(18,4),
                gross_sell decimal(18,4),
                net_flow decimal(18,4),
                notes text,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into market_flows values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "2026-04-28",
                    "fpi_net_investment_total",
                    "equity",
                    "FPI",
                    10000,
                    8000,
                    2000,
                    "fixture",
                    "2026-04-28 18:00:00",
                ),
                (
                    "2026-04-27",
                    "fpi_net_investment_total",
                    "debt",
                    "FPI",
                    5000,
                    6000,
                    -1000,
                    "fixture",
                    "2026-04-27 18:00:00",
                ),
            ],
        )
        con.execute(
            """
            create table derivatives_eod (
                contract_id text,
                instrument_id text,
                trade_date date,
                segment text,
                expiry_date date,
                strike_price decimal(18,4),
                option_type text,
                settlement_price decimal(18,4),
                open_interest bigint,
                oi_change bigint,
                contract_volume bigint,
                available_at timestamp
            )
            """
        )
        con.executemany(
            "insert into derivatives_eod values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "FUT",
                    "INS_1",
                    "2026-04-28",
                    "FUTSTK",
                    "2026-04-30",
                    0,
                    None,
                    1415,
                    600000,
                    50000,
                    1200,
                    "2026-04-28 17:00:00",
                ),
                (
                    "CE",
                    "INS_1",
                    "2026-04-28",
                    "OPTSTK",
                    "2026-04-30",
                    1400,
                    "CE",
                    22,
                    300000,
                    20000,
                    600,
                    "2026-04-28 17:00:00",
                ),
                (
                    "PE",
                    "INS_1",
                    "2026-04-28",
                    "OPTSTK",
                    "2026-04-30",
                    1400,
                    "PE",
                    16,
                    450000,
                    30000,
                    700,
                    "2026-04-28 17:00:00",
                ),
            ],
        )
        con.execute(
            """
            create table event_signals (
                event_signal_id text,
                news_id text,
                instrument_id text,
                sector_name text,
                event_date date,
                horizon text,
                impact_direction text,
                impact_score decimal(18,4),
                confidence decimal(18,4),
                exposure_channel text,
                rationale_code text,
                available_at timestamp
            )
            """
        )
        con.execute(
            "insert into event_signals values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "evt_1",
                "news_1",
                None,
                "Banks",
                "2026-04-28",
                "medium",
                "mixed",
                -0.20,
                0.70,
                "rates_liquidity",
                "rates_liquidity_policy",
                "2026-04-28 12:00:00",
            ),
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
