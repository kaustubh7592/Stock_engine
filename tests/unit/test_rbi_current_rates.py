from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_equity_engine.connectors.rbi.current_rates import RBICurrentRatesConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> RBICurrentRatesConnector:
    return RBICurrentRatesConnector(
        source=SourceConfig(
            code="S14",
            family="rbi",
            name="RBI DBIE",
            purpose="Rates, yields, macro series",
            url="https://data.rbi.org.in/",
            fetch_mode="web_data_query_download",
            cost="free_public",
            cadence="daily_or_monthly",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_rbi_current_rates_fixture() -> None:
    artifact = RawArtifact(
        source_code="S14",
        source_family="rbi",
        logical_name="rbi_current_rates",
        source_url="https://www.rbi.org.in/",
        content=Path("tests/fixtures/rbi_current_rates_sample.html").read_bytes(),
        extension="html",
        retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
        content_type="text/html",
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 7
    repo = next(row.row for row in normalized if row.row["series_name"] == "Policy Repo Rate")
    usd = next(row.row for row in normalized if row.row["series_name"] == "INR / 1 USD")
    assert repo["value_num"] == Decimal("5.25")
    assert repo["unit"] == "percent"
    assert repo["observation_date"].isoformat() == "2026-04-28"
    assert usd["value_num"] == Decimal("94.5210")
    assert usd["unit"] == "INR"
