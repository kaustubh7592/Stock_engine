from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_equity_engine.connectors.nsdl.fpi_flows import NSDLFPIFlowsConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSDLFPIFlowsConnector:
    return NSDLFPIFlowsConnector(
        source=SourceConfig(
            code="S17",
            family="nsdl",
            name="NSDL FPI reports",
            purpose="FPI flow context",
            url="https://www.fpi.nsdl.co.in/web/Reports/ReportsListing.aspx",
            fetch_mode="web_page_report_download",
            cost="free_public",
            cadence="daily_or_monthly",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_nsdl_fpi_latest_fixture() -> None:
    artifact = RawArtifact(
        source_code="S17",
        source_family="nsdl",
        logical_name="nsdl_fpi_latest",
        source_url="https://pilot.fpi.nsdl.co.in/Reports/Latest.aspx",
        content=Path("tests/fixtures/nsdl_fpi_latest_sample.html").read_bytes(),
        extension="html",
        retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
        content_type="text/html",
        metadata={"host": "pilot.fpi.nsdl.co.in"},
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 6
    equity_subtotal = next(
        row.row
        for row in normalized
        if row.row["segment"] == "equity"
        and row.row["flow_type"] == "fpi_net_investment_subtotal"
    )
    debt_stock_exchange = next(
        row.row
        for row in normalized
        if row.row["segment"] == "debt_general_limit"
        and row.row["flow_type"] == "fpi_net_investment_stock_exchange"
    )
    assert equity_subtotal["trade_date"].isoformat() == "2025-10-30"
    assert equity_subtotal["gross_buy"] == Decimal("12594.60")
    assert equity_subtotal["gross_sell"] == Decimal("13374.94")
    assert equity_subtotal["net_flow"] == Decimal("-780.34")
    assert "usd_inr=88.2834" in equity_subtotal["notes"]
    assert debt_stock_exchange["net_flow"] == Decimal("-865.53")
