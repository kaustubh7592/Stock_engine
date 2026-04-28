from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_equity_engine.connectors.nse.insider_trades import NSEInsiderTradesConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSEInsiderTradesConnector:
    return NSEInsiderTradesConnector(
        source=SourceConfig(
            code="S09",
            family="nse",
            name="NSE filing discovery",
            purpose="XBRL and filing discovery",
            url="https://www.nseindia.com/companies-listing/corporate-filings-pit-annual",
            fetch_mode="web_api",
            cost="free_public",
            cadence="daily",
        ),
        user_agent="test",
        from_date=date(2026, 4, 24),
        to_date=date(2026, 4, 28),
    )


def test_parse_and_normalize_nse_insider_trades_fixture() -> None:
    artifact = RawArtifact(
        source_code="S09",
        source_family="nse",
        logical_name="nse_insider_trades_equities_all_20260424_20260428",
        source_url=(
            "https://www.nseindia.com/api/corporates-pit?"
            "index=equities&from_date=24-04-2026&to_date=28-04-2026"
        ),
        content=Path("tests/fixtures/nse_insider_trades_sample.json").read_bytes(),
        extension="json",
        retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
        content_type="application/json",
        metadata={"discovery_surface": "insider_trades_pit", "index": "equities"},
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 2
    assert len(normalized) == 2
    row = normalized[0].row
    assert row["insider_trade_id"].startswith("INSIDER_TRADE_")
    assert row["filing_id"].startswith("FILING_")
    assert row["__nse_symbol"] == "360ONE"
    assert row["insider_name"] == "Anshuman Maheshwary"
    assert row["insider_category"] == "Employees/Designated Employees"
    assert row["transaction_date"] == date(2026, 4, 24)
    assert row["transaction_type"] == "Sell"
    assert row["quantity"] == 10000
    assert row["value_num"] == Decimal("10414500")
    assert row["price"] == Decimal("1041.45")
    assert row["post_holding"] == 145152
