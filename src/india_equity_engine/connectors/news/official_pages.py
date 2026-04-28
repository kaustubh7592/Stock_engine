"""Official-page event discovery for non-RSS policy surfaces."""

from __future__ import annotations

import re
import ssl
from datetime import datetime
from decimal import Decimal
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from india_equity_engine.connectors.base import SourceConnector
from india_equity_engine.connectors.news.classification import classify_event_type, entities_json
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

OFFICIAL_PAGE_KEYWORDS = (
    "budget",
    "capex",
    "circular",
    "document",
    "election",
    "expenditure",
    "finance",
    "notification",
    "press",
    "result",
    "schedule",
    "statement",
    "tax",
)


class OfficialPageNewsConnector(SourceConnector):
    """Extract high-signal policy/event links from official web pages."""

    def __init__(
        self,
        source: SourceConfig,
        source_name: str,
        source_class: str,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
        max_links: int = 50,
    ) -> None:
        self.source = source
        self.source_name = source_name
        self.source_class = source_class
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env
        self.max_links = max_links

    def discover(self) -> list[SourceObject]:
        return [
            SourceObject(
                source=self.source,
                logical_name=f"{self.source.family}_official_page",
                url=str(self.source.url),
                expected_extension="html",
                metadata={
                    "source_name": self.source_name,
                    "source_class": self.source_class,
                    "geography": "India",
                },
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "text/html,*/*"}
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
                message=f"{raw_artifact.logical_name} returned an empty page.",
            )
        rows = self.parse(raw_artifact)
        if not rows:
            return ValidationResult(
                ok=False,
                rule_name="official_page_links",
                message=f"{raw_artifact.logical_name} contained no relevant policy/event links.",
                severity="medium",
            )
        return ValidationResult(
            ok=True,
            rule_name="official_page_links",
            message=f"{raw_artifact.logical_name} contains relevant policy/event links.",
            metadata={"link_count": len(rows)},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        html = raw_artifact.content.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "lxml")
        rows = []
        seen = set()
        for anchor in soup.find_all("a"):
            headline = _clean(anchor.get_text(" ", strip=True))
            href = _clean(anchor.get("href"))
            if not headline or not href:
                continue
            text = f"{headline} {href}".lower()
            if not any(keyword in text for keyword in OFFICIAL_PAGE_KEYWORDS):
                continue
            url = urljoin(raw_artifact.source_url, href)
            key = (headline.lower(), url)
            if key in seen:
                continue
            seen.add(key)
            rows.append({"title": headline, "link": url})
            if len(rows) >= self.max_links:
                break
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
            if not headline:
                continue
            published_at = _date_from_text(headline) or raw_artifact.retrieved_at
            event_type = classify_event_type(headline, None, raw_artifact.source_family)
            news_id = stable_id("news", raw_artifact.logical_name, url, headline)
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
                "language_code": None,
                "geography": raw_artifact.metadata.get("geography") or "India",
                "event_type": event_type,
                "sentiment_score": None,
                "novelty_score": Decimal("1"),
                "entities_json": entities_json(headline),
                "source": raw_artifact.source_family,
                "source_url": raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": published_at,
                "as_of_date": published_at.date(),
                "document_hash": None,
                "parser_version": None,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="news_items", row=row))
        return output


def _date_from_text(value: str) -> datetime | None:
    match = re.search(r"\b(\d{1,2})[-/](\d{1,2})[-/](20\d{2})\b", value)
    if not match:
        return None
    day, month, year = match.groups()
    try:
        return datetime(int(year), int(month), int(day))
    except ValueError:
        return None


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None
