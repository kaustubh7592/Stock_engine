from pathlib import Path

from india_equity_engine.pipelines.download_filings import (
    FilingDownloadCandidate,
    _extension_for,
    _failed_row,
    _looks_like_html_error,
    _normalize_document_types,
    _success_row,
)


def test_extension_uses_url_path_not_domain() -> None:
    assert _extension_for("https://www.nseindia.com/api/corporate-announcements", "XBRL") == "xml"
    assert (
        _extension_for("https://nsearchives.nseindia.com/content/xbrl/result.xml?token=1", "XBRL")
        == "xml"
    )
    assert (
        _extension_for("https://nsearchives.nseindia.com/content/report", "PDF", "application/pdf")
        == "pdf"
    )


def test_normalize_document_types_defaults_to_structured_priority() -> None:
    assert _normalize_document_types(()) == ("XBRL", "XML", "ZIP")
    assert _normalize_document_types((" xml ", "XML", "zip")) == ("XML", "ZIP")


def test_html_error_detection_catches_error_pages() -> None:
    assert _looks_like_html_error(b"<!doctype html><title>error</title>", None)
    assert _looks_like_html_error(b"<xml></xml>", "text/html")
    assert not _looks_like_html_error(b"<?xml version='1.0'?><root/>", "application/xml")


def test_success_and_failure_rows_preserve_manifest_fields() -> None:
    candidate = FilingDownloadCandidate(
        filing_id="filing_1",
        instrument_id="instrument_1",
        exchange_code="NSE",
        document_type="XBRL",
        document_url="https://nsearchives.nseindia.com/content/xbrl/result.xml",
    )

    success = _success_row(
        candidate=candidate,
        local_path=Path("data/raw/nse/result.xml"),
        metadata_path=Path("data/raw/nse/result.xml.json"),
        content_type="application/xml",
        size_bytes=123,
        sha256="abc",
    )
    failure = _failed_row(candidate, "timeout")

    assert success["download_status"] == "success"
    assert Path(str(success["metadata_path"])).as_posix() == "data/raw/nse/result.xml.json"
    assert success["sha256"] == "abc"
    assert failure["download_status"] == "failed"
    assert failure["metadata_path"] is None
    assert failure["error_text"] == "timeout"
