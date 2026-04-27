"""NSE filing discovery connector."""

from __future__ import annotations

import json
import re
import ssl
from datetime import date, datetime

import feedparser
import httpx

from india_equity_engine.connectors.base import SourceConnector
from india_equity_engine.core.exceptions import ConnectorError
from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import (
    NormalizedRecord,
    RawArtifact,
    SourceConfig,
    SourceObject,
    ValidationResult,
)
from india_equity_engine.core.time_utils import utc_now

NSE_ANNOUNCEMENTS_RSS_URL = "https://nsearchives.nseindia.com/content/RSS/Online_announcements.xml"
NSE_SHAREHOLDING_EQUITY_API_URL = (
    "https://www.nseindia.com/api/corporate-share-holdings-master?index=equities"
)
NSE_SHAREHOLDING_SME_API_URL = "https://www.nseindia.com/api/corporate-share-holdings-master?index=sme"


class NSEFilingDiscoveryConnector(SourceConnector):
    """Discover NSE filing documents from official exchange announcement metadata.

    This is metadata-first by design. It records the filing/document inventory and document
    type so XBRL parsers can be layered in without using PDFs as the primary source.
    """

    def __init__(
        self,
        source: SourceConfig,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
    ) -> None:
        self.source = source
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env

    def discover(self) -> list[SourceObject]:
        return [
            SourceObject(
                source=self.source,
                logical_name="nse_filing_discovery_announcements_rss",
                url=NSE_ANNOUNCEMENTS_RSS_URL,
                expected_extension="xml",
                metadata={"discovery_surface": "announcements_rss"},
            ),
            SourceObject(
                source=self.source,
                logical_name="nse_shareholding_patterns_equity",
                url=NSE_SHAREHOLDING_EQUITY_API_URL,
                expected_extension="json",
                metadata={"discovery_surface": "shareholding_patterns", "index": "equities"},
            ),
            SourceObject(
                source=self.source,
                logical_name="nse_shareholding_patterns_sme",
                url=NSE_SHAREHOLDING_SME_API_URL,
                expected_extension="json",
                metadata={"discovery_surface": "shareholding_patterns", "index": "sme"},
            ),
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": _accept_header(source_object),
            "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern",
        }
        with httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers=headers,
            verify=self.verify,
            trust_env=self.trust_env,
        ) as client:
            try:
                response = client.get(str(source_object.url))
                response.raise_for_status()
                content_type = response.headers.get("content-type")
            except httpx.HTTPError as exc:
                raise ConnectorError(f"Failed to download {source_object.url}: {exc}") from exc

        return RawArtifact(
            source_code=self.source.code,
            source_family=self.source.family,
            logical_name=source_object.logical_name,
            source_url=str(source_object.url),
            content=response.content,
            extension=source_object.expected_extension,
            retrieved_at=utc_now(),
            content_type=content_type,
            metadata=source_object.metadata,
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded NSE filing discovery feed is empty.",
            )

        if raw_artifact.metadata.get("discovery_surface") == "shareholding_patterns":
            try:
                payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
            except json.JSONDecodeError as exc:
                return ValidationResult(
                    ok=False,
                    rule_name="json_parse",
                    message=f"NSE shareholding filing discovery payload is not JSON: {exc}",
                    severity="high",
                )
            if not isinstance(payload, list) or not payload:
                return ValidationResult(
                    ok=False,
                    rule_name="non_empty_rows",
                    message="NSE shareholding filing discovery payload contained no rows.",
                    severity="medium",
                )
            return ValidationResult(
                ok=True,
                rule_name="json_rows",
                message="NSE shareholding filing discovery payload looks valid.",
                metadata={"row_count": len(payload)},
            )

        feed = feedparser.parse(raw_artifact.content)
        if feed.bozo and not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="rss_parse",
                message=f"NSE filing discovery feed parse failed: {feed.bozo_exception}",
                severity="high",
            )
        if not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="non_empty_entries",
                message="NSE filing discovery feed contained no entries.",
                severity="medium",
            )

        return ValidationResult(
            ok=True,
            rule_name="rss_entries",
            message="NSE filing discovery feed looks valid.",
            metadata={"entry_count": len(feed.entries)},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        if raw_artifact.metadata.get("discovery_surface") == "shareholding_patterns":
            payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
            rows = []
            for row in payload:
                if isinstance(row, dict):
                    normalized = {_normalize_key(key): _clean(value) for key, value in row.items()}
                    normalized["__discovery_surface"] = "shareholding_patterns"
                    normalized["__index"] = raw_artifact.metadata.get("index")
                    rows.append(normalized)
            return rows

        feed = feedparser.parse(raw_artifact.content)
        rows = []
        for entry in feed.entries:
            rows.append(
                {
                    "company_name": _clean(entry.get("title")),
                    "document_url": _clean(entry.get("link")),
                    "summary": _clean(entry.get("summary")),
                    "published": _clean(entry.get("published")),
                }
            )
        return rows

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        if raw_artifact.metadata.get("discovery_surface") == "shareholding_patterns":
            return _normalize_shareholding_records(records, raw_artifact)

        output = []
        for record in records:
            company_name = _as_text(record.get("company_name"))
            document_url = _as_text(record.get("document_url"))
            summary = _as_text(record.get("summary"))
            published_at = _parse_nse_timestamp(_as_text(record.get("published")))
            headline, subject = _split_subject(summary)
            symbol = _symbol_from_url(document_url)
            document_type = _document_type(document_url)
            filing_family = _filing_family(subject, document_type)

            if not document_url:
                continue

            filing_id = stable_id("filing", "NSE", document_url, published_at or "", subject)
            output.append(
                NormalizedRecord(
                    table_name="filings",
                    row={
                        "filing_id": filing_id,
                        "instrument_id": stable_id("ins", f"NSE:{symbol}") if symbol else None,
                        "__nse_symbol": symbol,
                        "__company_name": company_name,
                        "exchange_code": "NSE",
                        "filing_family": filing_family,
                        "filing_subtype": subject or headline,
                        "period_end": _period_end_from_text(summary),
                        "filing_date": published_at.date() if published_at else None,
                        "document_type": document_type,
                        "document_url": document_url,
                        "xbrl_flag": document_type in {"XBRL", "XML"},
                        "supersedes_filing_id": None,
                        "source": raw_artifact.source_family,
                        "source_url": raw_artifact.source_url,
                        "retrieved_at": raw_artifact.retrieved_at,
                        "available_at": published_at or raw_artifact.retrieved_at,
                        "as_of_date": (published_at or raw_artifact.retrieved_at).date(),
                        "document_hash": None,
                        "parser_version": None,
                        "restated_flag": _is_restatement(summary),
                        "created_at": raw_artifact.retrieved_at,
                        "updated_at": raw_artifact.retrieved_at,
                    },
                )
            )
        return output


def _normalize_shareholding_records(
    records: list[dict[str, object]],
    raw_artifact: RawArtifact,
) -> list[NormalizedRecord]:
    output = []
    for record in records:
        symbol = _as_text(record.get("symbol"))
        document_url = _as_text(record.get("xbrl"))
        if not symbol or not document_url:
            continue

        period_end = _parse_nse_date(_as_text(record.get("date")))
        submission_at = _parse_nse_date(_as_text(record.get("submissiondate")))
        broadcast_at = _parse_nse_timestamp(_as_text(record.get("broadcastdate")))
        system_at = _parse_nse_timestamp(_as_text(record.get("systemdate")))
        available_at = broadcast_at or system_at
        if available_at is None and submission_at is not None:
            available_at = datetime.combine(submission_at, datetime.min.time())
        if available_at is None:
            available_at = raw_artifact.retrieved_at

        filing_id = stable_id(
            "filing",
            "NSE",
            "shareholding",
            symbol,
            period_end or "",
            document_url,
            record.get("recordid"),
        )
        output.append(
            NormalizedRecord(
                table_name="filings",
                row={
                    "filing_id": filing_id,
                    "instrument_id": stable_id("ins", f"NSE:{symbol}"),
                    "__nse_symbol": symbol,
                    "__company_name": _as_text(record.get("name")),
                    "exchange_code": "NSE",
                    "filing_family": "shareholding",
                    "filing_subtype": "shareholding_pattern",
                    "period_end": period_end,
                    "filing_date": submission_at or available_at.date(),
                    "document_type": _document_type(document_url),
                    "document_url": document_url,
                    "xbrl_flag": True,
                    "supersedes_filing_id": None,
                    "source": raw_artifact.source_family,
                    "source_url": raw_artifact.source_url,
                    "retrieved_at": raw_artifact.retrieved_at,
                    "available_at": available_at,
                    "as_of_date": available_at.date(),
                    "document_hash": None,
                    "parser_version": None,
                    "restated_flag": _is_restatement(_as_text(record.get("reviseddata"))),
                    "created_at": raw_artifact.retrieved_at,
                    "updated_at": raw_artifact.retrieved_at,
                },
            )
        )
    return output


def _accept_header(source_object: SourceObject) -> str:
    if source_object.expected_extension == "json":
        return "application/json,text/plain,*/*"
    return "application/rss+xml,application/xml,*/*"


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_text(value: object) -> str | None:
    return _clean(value)


def _parse_nse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_nse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.upper(), fmt).date()
        except ValueError:
            continue
    return None


def _split_subject(summary: str | None) -> tuple[str | None, str | None]:
    if not summary:
        return None, None
    parts = re.split(r"\|\s*SUBJECT\s*:", summary, maxsplit=1, flags=re.IGNORECASE)
    headline = parts[0].strip() if parts else summary.strip()
    subject = parts[1].strip() if len(parts) > 1 else None
    return headline or None, subject or None


def _symbol_from_url(document_url: str | None) -> str | None:
    if not document_url:
        return None
    file_name = document_url.rstrip("/").split("/")[-1]
    match = re.match(r"([A-Za-z0-9&._-]+?)(?:\d{3,}|_)", file_name)
    if not match:
        return None
    symbol = match.group(1).strip("_-.")
    return symbol.upper() or None


def _document_type(document_url: str | None) -> str | None:
    if not document_url:
        return None
    suffix = document_url.split("?", 1)[0].rsplit(".", 1)[-1].upper()
    if suffix in {"XML", "XBRL"}:
        return "XBRL"
    if suffix in {"PDF", "HTML", "HTM", "ZIP", "CSV", "XLS", "XLSX"}:
        return suffix if suffix != "HTM" else "HTML"
    return "URL"


def _filing_family(subject: str | None, document_type: str | None) -> str:
    text = (subject or "").lower()
    if "financial result" in text or "results" in text:
        return "financial"
    if "shareholding" in text:
        return "shareholding"
    if "annual report" in text:
        return "annual_report"
    if "pledge" in text:
        return "pledge"
    if "insider" in text or "trading" in text:
        return "insider_or_trading"
    if document_type == "XBRL":
        return "xbrl"
    return "announcement_attachment"


def _period_end_from_text(text: str | None) -> date | None:
    if not text:
        return None
    patterns = (
        r"(?:quarter|period|year)\s+ended\s+(\d{1,2}-[A-Za-z]{3}-\d{4})",
        r"(?:quarter|period|year)\s+ended\s+(\d{1,2}/\d{1,2}/\d{4})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1)
        for fmt in ("%d-%b-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _is_restatement(text: str | None) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(token in lowered for token in ("revised", "corrigendum", "rectified", "restated"))


def _normalize_key(value: str | None) -> str:
    text = "" if value is None else value.strip().lower()
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")
