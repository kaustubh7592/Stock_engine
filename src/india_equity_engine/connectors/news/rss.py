"""Official RSS news/event connectors."""

from __future__ import annotations

import ssl
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
import httpx

from india_equity_engine.connectors.base import SourceConnector
from india_equity_engine.connectors.news.classification import (
    classify_event_type,
    entities_json,
    normalized_sentiment_score,
)
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

RBI_RSS_FEEDS = (
    {
        "logical_name": "rbi_press_releases_rss",
        "url": "https://rbi.org.in/pressreleases_rss.xml",
        "source_name": "RBI Press Releases",
        "source_class": "regulator",
        "language_code": "en",
    },
    {
        "logical_name": "rbi_notifications_rss",
        "url": "https://rbi.org.in/notifications_rss.xml",
        "source_name": "RBI Notifications",
        "source_class": "regulator",
        "language_code": "en",
    },
    {
        "logical_name": "rbi_speeches_rss",
        "url": "https://rbi.org.in/speeches_rss.xml",
        "source_name": "RBI Speeches",
        "source_class": "regulator",
        "language_code": "en",
    },
)

PIB_RSS_FEEDS = (
    {
        "logical_name": "pib_press_releases_rss",
        "url": "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=3",
        "source_name": "PIB Press Releases",
        "source_class": "government",
        "language_code": None,
    },
    {
        "logical_name": "pib_media_invitations_rss",
        "url": "https://pib.gov.in/RssMain.aspx?ModId=10&Lang=2&Regid=3",
        "source_name": "PIB Media Invitations",
        "source_class": "government",
        "language_code": None,
    },
)


class OfficialRSSNewsConnector(SourceConnector):
    """Normalize official RSS entries into canonical news_items rows."""

    def __init__(
        self,
        source: SourceConfig,
        feeds: tuple[dict[str, Any], ...],
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
    ) -> None:
        self.source = source
        self.feeds = feeds
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env

    def discover(self) -> list[SourceObject]:
        return [
            SourceObject(
                source=self.source,
                logical_name=str(feed["logical_name"]),
                url=str(feed["url"]),
                expected_extension="xml",
                metadata={
                    "source_name": feed["source_name"],
                    "source_class": feed["source_class"],
                    "language_code": feed.get("language_code"),
                    "geography": "India",
                },
            )
            for feed in self.feeds
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/rss+xml,application/xml,text/xml,*/*",
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
            content_type=response.headers.get("content-type"),
            metadata=source_object.metadata,
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message=f"{raw_artifact.logical_name} returned an empty RSS artifact.",
            )
        feed = feedparser.parse(raw_artifact.content)
        if feed.bozo and not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="rss_parse",
                message=f"{raw_artifact.logical_name} RSS parse failed: {feed.bozo_exception}",
                severity="high",
            )
        if not feed.entries:
            return ValidationResult(
                ok=False,
                rule_name="rss_entries",
                message=f"{raw_artifact.logical_name} contained no RSS entries.",
                severity="medium",
            )
        return ValidationResult(
            ok=True,
            rule_name="rss_entries",
            message=f"{raw_artifact.logical_name} contains RSS entries.",
            metadata={"entry_count": len(feed.entries)},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        feed = feedparser.parse(raw_artifact.content)
        rows = []
        for entry in feed.entries:
            rows.append(
                {
                    "title": _clean(entry.get("title")),
                    "link": _clean(entry.get("link")),
                    "summary": _clean(entry.get("summary") or entry.get("description")),
                    "published": _clean(entry.get("published") or entry.get("updated")),
                    "language": _clean(entry.get("language") or feed.feed.get("language")),
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
            headline = _clean(record.get("title"))
            url = _clean(record.get("link"))
            summary = _clean(record.get("summary"))
            if not headline and not url:
                continue
            published_at = _parse_rss_datetime(_clean(record.get("published")))
            available_at = published_at or raw_artifact.retrieved_at
            event_type = classify_event_type(headline, summary, raw_artifact.source_family)
            news_id = stable_id(
                "news",
                raw_artifact.logical_name,
                url,
                published_at.isoformat() if published_at else "",
                headline,
            )
            row = {
                "news_id": news_id,
                "published_at": published_at,
                "source_name": (
                    raw_artifact.metadata.get("source_name")
                    or raw_artifact.source_family.upper()
                ),
                "source_class": raw_artifact.metadata.get("source_class"),
                "headline": headline,
                "url": url or raw_artifact.source_url,
                "language_code": _clean(record.get("language"))
                or raw_artifact.metadata.get("language_code"),
                "geography": raw_artifact.metadata.get("geography") or "India",
                "event_type": event_type,
                "sentiment_score": normalized_sentiment_score(record.get("tone")),
                "novelty_score": Decimal("1"),
                "entities_json": entities_json(headline, summary),
                "source": raw_artifact.source_family,
                "source_url": raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": available_at,
                "as_of_date": available_at.date(),
                "document_hash": None,
                "parser_version": None,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="news_items", row=row))
        return output


def _parse_rss_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, IndexError, OverflowError):
        pass
    for fmt in ("%a, %d %b %Y %H:%M:%S", "%d %b %Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(value.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split()).strip()
    return text or None
