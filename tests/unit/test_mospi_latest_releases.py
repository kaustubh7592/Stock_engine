from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_equity_engine.connectors.mospi.latest_releases import MoSPILatestReleasesConnector
from india_equity_engine.core.schemas.contracts import RawArtifact
from india_equity_engine.storage.registry import SourceRegistry


def test_mospi_latest_releases_connector_normalizes_macro_rows() -> None:
    source = SourceRegistry(Path("configs")).get("S16")
    connector = MoSPILatestReleasesConnector(source=source, user_agent="test")
    artifact = RawArtifact(
        source_code="S16",
        source_family="mospi",
        logical_name="fixture",
        source_url="https://mospi.gov.in/latest-releases",
        content=Path("tests/fixtures/mospi_latest_releases_sample.html").read_bytes(),
        extension="html",
        retrieved_at=datetime(2026, 4, 29, 12, 30, tzinfo=timezone.utc),
        content_type="text/html",
    )

    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    records = connector.normalize(parsed, artifact)

    assert validation.ok is True
    assert len(records) == 3
    cpi = next(record.row for record in records if "CPI" in record.row["series_name"])
    assert cpi["observation_date"] == date(2026, 3, 31)
    assert cpi["value_num"] == Decimal("3.34")
