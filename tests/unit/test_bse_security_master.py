from datetime import datetime, timezone
from io import BytesIO
from zipfile import ZipFile

from india_equity_engine.connectors.bse.security_master import BSEScripMasterConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> BSEScripMasterConnector:
    return BSEScripMasterConnector(
        source=SourceConfig(
            code="S02",
            family="bse",
            name="BSE Scrip Master",
            purpose="BSE mapping and aliases",
            url="https://www.bseindia.com/downloads1/Scrip_BSE.zip",
            fetch_mode="scrip_master_zip",
            cost="free_public",
            cadence="daily_or_weekly",
        ),
        user_agent="test",
    )


def test_parse_and_normalize_bse_scrip_master_fixture() -> None:
    content = _zip_bytes(
        "BSE_EQ_SCRIP_24042026.csv",
        (
            "Scrip Code,Instrument Code,Group Name,Scrip Name,ISIN CODE,"
            "Market Lot,Face Value,Security Type Flag\n"
            "500325,RELIANCE,A,Reliance Industries Limited,INE002A01018,1,1000,EQ\n"
            "532540,TCS,A,Tata Consultancy Services Limited,INE467B01029,1,100,EQ\n"
        ),
    )
    artifact = RawArtifact(
        source_code="S02",
        source_family="bse",
        logical_name="bse_scrip_bse_master",
        source_url="https://www.bseindia.com/downloads1/Scrip_BSE.zip",
        content=content,
        extension="zip",
        retrieved_at=datetime(2026, 4, 25, 12, 30, tzinfo=timezone.utc),
        content_type="application/zip",
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 2
    assert [record.table_name for record in normalized].count("instruments") == 2
    assert [record.table_name for record in normalized].count("listings") == 2
    listing = next(record.row for record in normalized if record.table_name == "listings")
    assert listing["exchange_code"] == "BSE"
    assert listing["bse_scrip_code"] == "500325"
    assert str(listing["face_value"]) == "10"


def _zip_bytes(name: str, content: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, content)
    return buffer.getvalue()
