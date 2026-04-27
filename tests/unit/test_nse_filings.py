from datetime import date, datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.nse.filings import NSEFilingDiscoveryConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSEFilingDiscoveryConnector:
    return NSEFilingDiscoveryConnector(
        source=SourceConfig(
            code="S09",
            family="nse",
            name="NSE filing discovery",
            purpose="XBRL and filing discovery",
            url="https://www.nseindia.com/companies-listing/corporate-filings-announcements-xbrl",
            fetch_mode="rss_metadata_plus_xbrl_surface",
            cost="free_public",
            cadence="daily",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_nse_filing_discovery_fixture() -> None:
    artifact = RawArtifact(
        source_code="S09",
        source_family="nse",
        logical_name="nse_filing_discovery_announcements_rss",
        source_url="https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml",
        content=Path("tests/fixtures/nse_filings_sample.xml").read_bytes(),
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
    first = normalized[0].row
    second = normalized[1].row
    assert first["filing_family"] == "financial"
    assert first["document_type"] == "XBRL"
    assert first["xbrl_flag"] is True
    assert first["period_end"] == date(2026, 3, 31)
    assert second["filing_family"] == "annual_report"
    assert second["document_type"] == "PDF"
    assert second["xbrl_flag"] is False
