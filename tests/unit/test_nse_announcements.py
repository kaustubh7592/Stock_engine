from datetime import datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.nse.announcements import NSEAnnouncementsRSSConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSEAnnouncementsRSSConnector:
    return NSEAnnouncementsRSSConnector(
        source=SourceConfig(
            code="S06",
            family="nse",
            name="NSE announcements RSS",
            purpose="Company announcements",
            url="https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
            fetch_mode="rss_plus_page_fetch",
            cost="free_public",
            cadence="hourly_plus_daily_archive",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_nse_announcements_rss_fixture() -> None:
    artifact = RawArtifact(
        source_code="S06",
        source_family="nse",
        logical_name="nse_announcements_rss",
        source_url="https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
        content=Path("tests/fixtures/nse_announcements_sample.xml").read_bytes(),
        extension="xml",
        retrieved_at=datetime(2026, 4, 27, 12, 30, tzinfo=timezone.utc),
        content_type="application/xml",
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 2
    assert len(normalized) == 2
    row = normalized[0].row
    assert row["exchange_code"] == "NSE"
    assert row["__nse_symbol"] == "RELIANCE"
    assert row["category"] == "outcome"
    assert row["sub_category"] == "Outcome of Board Meeting"
    assert row["attachment_url"].endswith("Outcome.pdf")
