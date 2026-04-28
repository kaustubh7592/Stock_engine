import json
from datetime import datetime, timezone
from decimal import Decimal

from india_equity_engine.connectors.news.signals import (
    map_event_signals,
    news_signal_source_row_from_dict,
)


def test_map_event_signals_builds_sector_rows_from_news_item() -> None:
    row = news_signal_source_row_from_dict(
        {
            "news_id": "news-1",
            "published_at": datetime(2026, 4, 28, 10, 0, tzinfo=timezone.utc),
            "source_name": "PIB",
            "source_class": "government",
            "headline": "Cabinet approves infrastructure capex programme for railways",
            "url": "https://pib.gov.in/example",
            "event_type": "budget_policy",
            "entities_json": json.dumps({"exposure_channels": ["government_capex_budget"]}),
            "source": "pib",
            "source_url": "https://pib.gov.in/ViewRss.aspx",
        }
    )

    records = map_event_signals([row])

    assert records
    assert records[0].table_name == "event_signals"
    assert "Capital Goods" in {record.row["sector_name"] for record in records}
    assert records[0].row["impact_direction"] == "positive"
    assert records[0].row["confidence"] > Decimal("0.50")
