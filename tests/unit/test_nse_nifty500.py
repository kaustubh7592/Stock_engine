from datetime import datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.nse.nifty500 import NSENifty500Connector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSENifty500Connector:
    return NSENifty500Connector(
        source=SourceConfig(
            code="S03",
            family="nse",
            name="Nifty 500 constituents",
            purpose="Initial universe seed",
            url="https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
            fetch_mode="direct_csv_download",
            cost="free_public",
            cadence="daily_or_weekly",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_nifty500_fixture() -> None:
    content = Path("tests/fixtures/nse_nifty500_sample.csv").read_bytes()
    artifact = RawArtifact(
        source_code="S03",
        source_family="nse",
        logical_name="nse_nifty500_constituents",
        source_url="https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
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
    assert [record.table_name for record in normalized].count("universe_memberships") == 2
    membership = next(
        record.row for record in normalized if record.table_name == "universe_memberships"
    )
    assert membership["universe_code"] == "NIFTY_500"
