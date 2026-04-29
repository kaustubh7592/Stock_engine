import json
from pathlib import Path

import duckdb

from india_equity_engine.core.schemas.snapshot_schema import StockSnapshot
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_stock_snapshots import build_stock_snapshots
from india_equity_engine.pipelines.explain_snapshots import explain_snapshots


def test_stock_snapshot_and_explanation_flow(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    _seed_tables(db_path)
    settings = _settings(tmp_path, db_path)

    snapshot_result = build_stock_snapshots(settings, limit=10)

    assert snapshot_result.status == "success"
    assert snapshot_result.outputs["tables"] == {"stock_snapshots": 1}
    json_path = Path(snapshot_result.outputs["json_outputs"][0])
    snapshot = StockSnapshot.model_validate_json(json_path.read_text(encoding="utf-8"))
    assert snapshot.instrument.instrument_id == "INS_1"
    assert snapshot.scores["long"].classification == "bullish"
    assert snapshot.decision.primary_horizon == "long"

    explanation_result = explain_snapshots(settings, limit=10)

    assert explanation_result.status == "success"
    assert explanation_result.outputs["tables"] == {"llm_explanations": 1}
    markdown_path = Path(explanation_result.outputs["markdown_outputs"][0])
    assert markdown_path.exists()
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute(
            """
            select output_json, safety_flags_json
            from llm_explanations
            """
        ).fetchone()
    output = json.loads(row[0])
    safety = json.loads(row[1])
    assert output["short_term_outlook"]
    assert safety["uses_only_stock_snapshot"] is True


def _seed_tables(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table instruments (
                instrument_id text,
                isin text,
                legal_name text,
                issuer_name text,
                sector_name text,
                industry_name text
            )
            """
        )
        con.execute(
            "insert into instruments values (?, ?, ?, ?, ?, ?)",
            (
                "INS_1",
                "INE000A01011",
                "Example Industries Ltd",
                "Example Industries",
                "Capital Goods",
                "Industrial Machinery",
            ),
        )
        con.execute(
            """
            create table listings (
                listing_id text,
                instrument_id text,
                exchange_code text,
                symbol text,
                bse_scrip_code text
            )
            """
        )
        con.executemany(
            "insert into listings values (?, ?, ?, ?, ?)",
            [
                ("L1", "INS_1", "NSE", "EXAMPLE", None),
                ("L2", "INS_1", "BSE", None, "500000"),
            ],
        )
        con.execute(
            """
            create table feature_snapshots (
                instrument_id text,
                as_of_date date,
                horizon text,
                feature_family text,
                feature_name text,
                value_num decimal(18,4),
                zscore decimal(18,4),
                rank_pct decimal(18,4),
                coverage_flag boolean
            )
            """
        )
        con.executemany(
            "insert into feature_snapshots values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "INS_1",
                    "2026-04-28",
                    "short",
                    "technical",
                    "return_5d_pct",
                    8.0,
                    1.0,
                    0.8,
                    True,
                ),
                (
                    "INS_1",
                    "2026-04-28",
                    "long",
                    "fundamental",
                    "fundamental_revenue_growth_1p_pct",
                    20.0,
                    1.2,
                    0.9,
                    True,
                ),
                (
                    "INS_1",
                    "2026-04-28",
                    "medium",
                    "governance",
                    "governance_risk_flag_365d",
                    0.0,
                    None,
                    None,
                    True,
                ),
            ],
        )
        con.execute(
            """
            create table score_snapshots (
                instrument_id text,
                as_of_date date,
                horizon text,
                technical_score decimal(18,4),
                fundamental_score decimal(18,4),
                governance_score decimal(18,4),
                macro_score decimal(18,4),
                event_score decimal(18,4),
                derivatives_score decimal(18,4),
                composite_score decimal(18,4),
                confidence_score decimal(18,4),
                classification text
            )
            """
        )
        con.executemany(
            "insert into score_snapshots values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "INS_1",
                    "2026-04-28",
                    "short",
                    0.65,
                    0.55,
                    0.70,
                    None,
                    None,
                    None,
                    0.62,
                    0.66,
                    "bullish",
                ),
                (
                    "INS_1",
                    "2026-04-28",
                    "medium",
                    0.58,
                    0.60,
                    0.72,
                    None,
                    None,
                    None,
                    0.61,
                    0.64,
                    "bullish",
                ),
                (
                    "INS_1",
                    "2026-04-28",
                    "long",
                    0.54,
                    0.75,
                    0.74,
                    None,
                    None,
                    None,
                    0.70,
                    0.72,
                    "bullish",
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
