from datetime import date, datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.nse.derivatives_eod import NSEDerivativesEODConnector
from india_equity_engine.core.schemas.contracts import RawArtifact
from india_equity_engine.storage.registry import SourceRegistry


def test_nse_derivatives_connector_normalizes_bhavcopy_rows() -> None:
    source = SourceRegistry(Path("configs")).get("S12")
    connector = NSEDerivativesEODConnector(
        source=source,
        trade_date=date(2026, 4, 28),
        user_agent="test",
    )
    artifact = RawArtifact(
        source_code="S12",
        source_family="nse",
        logical_name="fixture",
        source_url="https://example.test/fo.csv",
        content=Path("tests/fixtures/nse_derivatives_bhavcopy_sample.csv").read_bytes(),
        extension="csv",
        retrieved_at=datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc),
        content_type="text/csv",
    )

    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    records = connector.normalize(parsed, artifact)

    assert validation.ok is True
    assert len(records) == 3
    first = records[0].row
    assert first["trade_date"] == date(2026, 4, 28)
    assert first["segment"] == "FUTSTK"
    assert first["open_interest"] == 600000
    assert first["oi_change"] == 50000
