from datetime import date, datetime, timezone
from io import BytesIO
from zipfile import ZipFile

from india_equity_engine.connectors.nse.market_eod import (
    NSECashMarketEODConnector,
    NSESecurityWiseDeliveryConnector,
)
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def _connector() -> NSECashMarketEODConnector:
    return NSECashMarketEODConnector(
        source=SourceConfig(
            code="S04",
            family="nse",
            name="NSE All Reports cash market",
            purpose="EOD prices and market reports",
            url="https://www.nseindia.com/all-reports/",
            fetch_mode="report_download",
            cost="free_public",
            cadence="daily",
        ),
        trade_date=date(2026, 4, 24),
        user_agent="test",
    )


def test_parse_and_normalize_press_file_bhavcopy() -> None:
    content = _zip_bytes(
        "bh24042026.csv",
        (
            "SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,"
            "TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN\n"
            "RELIANCE,EQ,1400.00,1420.00,1395.00,1410.00,1411.00,1399.00,"
            "1000000,1410000000.00,24-Apr-2026,10000,INE002A01018\n"
        ),
    )
    artifact = RawArtifact(
        source_code="S04",
        source_family="nse",
        logical_name="nse_cm_bhavcopy_pr_20260424",
        source_url="https://nsearchives.nseindia.com/content/cm/PR240426.zip",
        content=content,
        extension="zip",
        retrieved_at=datetime(2026, 4, 24, 12, 30, tzinfo=timezone.utc),
        content_type="application/zip",
        metadata={"trade_date": "2026-04-24"},
    )

    connector = _connector()
    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 1
    assert len(normalized) == 1
    row = normalized[0].row
    assert row["instrument_id"].startswith("INS_")
    assert row["trade_date"] == date(2026, 4, 24)
    assert row["volume"] == 1000000
    assert str(row["close_price"]) == "1410.00"


def test_parse_and_normalize_delivery_file() -> None:
    content = (
        b"SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
        b"LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, "
        b"NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
        b"RELIANCE, EQ, 24-Apr-2026, 1399.00, 1400.00, 1420.00, 1395.00, "
        b"1411.00, 1410.00, 1408.00, 1000000, 14100.00, 10000, 450000, 45.00\n"
    )
    artifact = RawArtifact(
        source_code="S04",
        source_family="nse",
        logical_name="nse_sec_bhavdata_full_20260424",
        source_url="https://archives.nseindia.com/products/content/sec_bhavdata_full_24042026.csv",
        content=content,
        extension="csv",
        retrieved_at=datetime(2026, 4, 24, 12, 30, tzinfo=timezone.utc),
        content_type="text/csv",
        metadata={"trade_date": "2026-04-24"},
    )
    connector = NSESecurityWiseDeliveryConnector(
        source=_source(),
        trade_date=date(2026, 4, 24),
        user_agent="test",
    )

    validation = connector.validate(artifact)
    parsed = connector.parse(artifact)
    normalized = connector.normalize(parsed, artifact)

    assert validation.ok
    assert len(parsed) == 1
    assert len(normalized) == 1
    row = normalized[0].row
    assert row["__nse_symbol"] == "RELIANCE"
    assert row["deliverable_qty"] == 450000
    assert str(row["deliverable_pct"]) == "45.00"


def _zip_bytes(name: str, content: str) -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr(name, content)
    return buffer.getvalue()


def _source() -> SourceConfig:
    return SourceConfig(
        code="S04",
        family="nse",
        name="NSE All Reports cash market",
        purpose="EOD prices and market reports",
        url="https://www.nseindia.com/all-reports/",
        fetch_mode="report_download",
        cost="free_public",
        cadence="daily",
    )
