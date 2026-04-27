"""NSE corporate announcements RSS connector."""

from __future__ import annotations

import re
import ssl
from datetime import datetime

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


class NSEAnnouncementsRSSConnector(SourceConnector):
    """Connector for NSE's corporate announcements RSS feed."""

    def __init__(
        self,
        source: SourceConfig,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
    ) -> None:
        self.source = source
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify

    def discover(self) -> list[SourceObject]:
        return [
            SourceObject(
                source=self.source,
                logical_name="nse_announcements_rss",
                url=NSE_ANNOUNCEMENTS_RSS_URL,
                expected_extension="xml",
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/rss+xml,application/xml,*/*",
        }
        with httpx.Client(
            timeout=self.timeout_seconds,
            follow_redirects=True,
            headers=headers,
            verify=self.verify,
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
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded NSE announcements RSS is empty.",
            )

        feed = feedparser.parse(raw_artifact.content)
        if feed.bozo and not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="rss_parse",
                message=f"NSE announcements RSS parse failed: {feed.bozo_exception}",
                severity="high",
            )

        if not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="non_empty_entries",
                message="NSE announcements RSS contained no entries.",
                severity="medium",
            )

        return ValidationResult(
            ok=True,
            rule_name="rss_entries",
            message="NSE announcements RSS looks valid.",
            metadata={"entry_count": len(feed.entries)},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        feed = feedparser.parse(raw_artifact.content)
        rows = []
        for entry in feed.entries:
            rows.append(
                {
                    "company_name": _clean(entry.get("title")),
                    "link": _clean(entry.get("link")),
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
        output = []
        for record in records:
            company_name = _as_text(record.get("company_name"))
            link = _as_text(record.get("link"))
            summary = _as_text(record.get("summary"))
            announced_at = _parse_nse_rss_timestamp(_as_text(record.get("published")))
            headline, subject = _split_summary(summary)
            symbol = _symbol_from_link(link)

            announcement_id = stable_id(
                "ann",
                "NSE",
                link,
                announced_at.isoformat() if announced_at else "",
                headline or company_name,
            )
            output.append(
                NormalizedRecord(
                    table_name="corporate_announcements",
                    row={
                        "announcement_id": announcement_id,
                        "instrument_id": stable_id("ins", f"NSE:{symbol}") if symbol else None,
                        "__nse_symbol": symbol,
                        "exchange_code": "NSE",
                        "announced_at": announced_at,
                        "headline": headline or company_name,
                        "category": _category_from_subject(subject),
                        "sub_category": subject,
                        "summary_text": summary,
                        "attachment_url": link,
                        "filing_family": _filing_family_from_subject(subject),
                        "source": raw_artifact.source_family,
                        "source_url": raw_artifact.source_url,
                        "retrieved_at": raw_artifact.retrieved_at,
                        "available_at": announced_at or raw_artifact.retrieved_at,
                        "as_of_date": (announced_at or raw_artifact.retrieved_at).date(),
                        "document_hash": None,
                        "parser_version": None,
                        "restated_flag": False,
                        "created_at": raw_artifact.retrieved_at,
                        "updated_at": raw_artifact.retrieved_at,
                    },
                )
            )
        return output


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_text(value: object) -> str | None:
    return _clean(value)


def _parse_nse_rss_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _split_summary(summary: str | None) -> tuple[str | None, str | None]:
    if not summary:
        return None, None
    parts = re.split(r"\|\s*SUBJECT\s*:", summary, maxsplit=1, flags=re.IGNORECASE)
    headline = parts[0].strip() if parts else summary.strip()
    subject = parts[1].strip() if len(parts) > 1 else None
    return headline or None, subject or None


def _symbol_from_link(link: str | None) -> str | None:
    if not link:
        return None
    file_name = link.rstrip("/").split("/")[-1]
    match = re.match(r"([A-Za-z0-9&._-]+?)(?:\d{3,}|_)", file_name)
    if not match:
        return None
    symbol = match.group(1).strip("_-.")
    return symbol.upper() or None


def _category_from_subject(subject: str | None) -> str | None:
    if not subject:
        return None
    text = subject.lower()
    if "outcome" in text:
        return "outcome"
    if "financial result" in text or "results" in text:
        return "financial_results"
    if "board meeting" in text:
        return "board_meeting"
    if "newspaper" in text:
        return "newspaper_publication"
    if "shareholding" in text:
        return "shareholding"
    if "dividend" in text:
        return "dividend"
    return "general"


def _filing_family_from_subject(subject: str | None) -> str | None:
    category = _category_from_subject(subject)
    if category in {"financial_results", "shareholding"}:
        return category
    if category in {"board_meeting", "dividend", "outcome"}:
        return "corporate_action_or_event"
    return "announcement"
