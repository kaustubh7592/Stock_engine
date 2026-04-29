from india_equity_engine.models.canonical import CANONICAL_SCHEMAS


def test_core_tables_exist() -> None:
    for table in (
        "instruments",
        "listings",
        "universe_memberships",
        "price_daily",
        "corporate_announcements",
        "corporate_actions",
        "filings",
        "filing_artifacts",
        "financial_facts",
        "shareholding_pattern",
        "pledge_disclosures",
        "insider_trades",
        "governance_events",
        "macro_series",
        "market_flows",
        "news_items",
        "event_signals",
        "feature_snapshots",
        "score_snapshots",
        "stock_snapshots",
        "llm_explanations",
    ):
        assert table in CANONICAL_SCHEMAS


def test_price_daily_has_point_in_time_lineage() -> None:
    schema = CANONICAL_SCHEMAS["price_daily"]
    assert "available_at" in schema.column_names
    assert "document_hash" in schema.column_names
