from datetime import datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.nse.security_master import NSEEquityListConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSEEquityListConnector:
    return NSEEquityListConnector(
        source=SourceConfig(
            code="S01",
            family="nse",
            name="NSE EQUITY_L master file",
            purpose="Universe and ISIN mapping",
            url="https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
            fetch_mode="direct_csv_download",
            cost="free_public",
            cadence="daily_or_weekly",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_nse_equity_l_fixture() -> None:
    content = Path("tests/fixtures/nse_equity_l_sample.csv").read_bytes()
    artifact = RawArtifact(
        source_code="S01",
        source_family="nse",
        logical_name="nse_equity_l_master",
        source_url="https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
        content=content,
        extension="csv",
        retrieved_at=datetime(2026, 4, 25, 12, 30, tzinfo=timezone.utc),
        content_type="text/csv",
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 2
    assert [record.table_name for record in normalized].count("instruments") == 2
    assert [record.table_name for record in normalized].count("listings") == 2
    reliance = next(record.row for record in normalized if record.table_name == "instruments")
    assert reliance["country_code"] == "IN"
