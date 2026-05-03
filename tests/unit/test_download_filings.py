from pathlib import Path

import duckdb

from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.download_filings import (
    FilingDownloadCandidate,
    _extension_for,
    _failed_row,
    _load_candidates,
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


def test_load_candidates_can_filter_to_one_instrument(tmp_path: Path) -> None:
    db_path = tmp_path / "warehouse" / "test.duckdb"
    db_path.parent.mkdir(parents=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute(
            """
            create table filings (
                filing_id text,
                instrument_id text,
                exchange_code text,
                filing_family text,
                filing_date date,
                document_type text,
                document_url text,
                xbrl_flag boolean
            )
            """
        )
        con.executemany(
            "insert into filings values (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "filing_hdfc",
                    "INS_HDFC",
                    "NSE",
                    "shareholding",
                    "2026-04-30",
                    "XBRL",
                    "https://nsearchives.nseindia.com/hdfc.xml",
                    True,
                ),
                (
                    "filing_other",
                    "INS_OTHER",
                    "NSE",
                    "shareholding",
                    "2026-04-30",
                    "XBRL",
                    "https://nsearchives.nseindia.com/other.xml",
                    True,
                ),
            ],
        )
    settings = Settings(
        config_dir=Path("configs").resolve(),
        data_root=tmp_path / "data",
        raw_root=tmp_path / "data" / "raw",
        silver_root=tmp_path / "data" / "silver",
        gold_root=tmp_path / "data" / "gold",
        cache_root=tmp_path / "data" / "cache",
        logs_root=tmp_path / "data" / "logs",
        duckdb_path=db_path,
        http_timeout_seconds=30,
        user_agent="test",
        use_system_cert_store=True,
        http_trust_env=False,
        ca_bundle=None,
        parser_version="test",
    )

    candidates = _load_candidates(
        settings,
        ("XBRL",),
        "shareholding",
        "INS_HDFC",
        10,
    )

    assert [candidate.filing_id for candidate in candidates] == ["filing_hdfc"]
