from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.score_snapshots import score_snapshots


def test_score_snapshots_pipeline_writes_scores(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    _seed_feature_table(db_path)
    settings = _settings(tmp_path, db_path)

    result = score_snapshots(settings)

    assert result.status == "success"
    assert result.outputs["tables"] == {"score_snapshots": 6}
    assert result.outputs["horizons"] == {"short": 2, "medium": 2, "long": 2}
    assert (tmp_path / "data" / "gold" / "score_snapshots" / "current.parquet").exists()
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute(
            """
            select instrument_id, horizon, classification, confidence_score
            from score_snapshots
            order by instrument_id, horizon
            """
        ).fetchall()
    assert len(rows) == 6
    assert any(row[2] in {"bullish", "neutral", "bearish", "abstain"} for row in rows)


def _seed_feature_table(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as con:
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
                    "INS_GOOD",
                    "2026-04-28",
                    "short",
                    "technical",
                    "return_5d_pct",
                    12.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_GOOD",
                    "2026-04-28",
                    "short",
                    "technical",
                    "delivery_pct_latest",
                    60.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_GOOD",
                    "2026-04-28",
                    "medium",
                    "governance",
                    "governance_risk_flag_365d",
                    0.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_GOOD",
                    "2026-04-28",
                    "long",
                    "fundamental",
                    "fundamental_revenue_growth_1p_pct",
                    20.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_GOOD",
                    "2026-04-28",
                    "long",
                    "fundamental",
                    "fundamental_debt_to_assets",
                    0.20,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_WEAK",
                    "2026-04-28",
                    "short",
                    "technical",
                    "return_5d_pct",
                    -15.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_WEAK",
                    "2026-04-28",
                    "medium",
                    "governance",
                    "governance_risk_flag_365d",
                    1.0,
                    None,
                    None,
                    True,
                ),
                (
                    "INS_WEAK",
                    "2026-04-28",
                    "long",
                    "fundamental",
                    "fundamental_debt_to_assets",
                    0.90,
                    None,
                    None,
                    True,
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
