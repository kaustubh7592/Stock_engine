"""GDELT DOC API connector for global event context."""

from __future__ import annotations

import json
import ssl
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlencode

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

GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_GDELT_QUERY = (
    "India (RBI OR rupee OR oil OR crude OR budget OR election OR tariff OR conflict)"
)


class GDELTDocConnector(SourceConnector):
    """Fetch GDELT article metadata for India/global context events."""

    def __init__(
        self,
        source: SourceConfig,
        user_agent: str,
        query: str = DEFAULT_GDELT_QUERY,
        max_records: int = 50,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
    ) -> None:
        self.source = source
        self.user_agent = user_agent
        self.query = query
        self.max_records = max(1, min(max_records, 250))
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env

    def discover(self) -> list[SourceObject]:
        params = urlencode(
            {
                "query": self.query,
                "mode": "ArtList",
                "format": "json",
                "maxrecords": str(self.max_records),
                "sort": "HybridRel",
            }
        )
        return [
            SourceObject(
                source=self.source,
                logical_name="gdelt_doc_india_context",
                url=f"{GDELT_DOC_API_URL}?{params}",
                expected_extension="json",
                metadata={
                    "source_name": "GDELT DOC",
                    "source_class": "global_news",
                    "geography": "global",
                    "query": self.query,
                },
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "application/json,*/*"}
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
                message="GDELT returned no content.",
            )
        try:
            payload = json.loads(raw_artifact.content.decode("utf-8", errors="ignore"))
        except json.JSONDecodeError as exc:
            return ValidationResult(
                ok=False,
                rule_name="json_parse",
                message=f"GDELT JSON parse failed: {exc}",
                severity="high",
            )
        if not payload.get("articles"):
            return ValidationResult(
                ok=False,
                rule_name="articles",
                message="GDELT returned no article records for the configured query.",
                severity="medium",
            )
        return ValidationResult(
            ok=True,
            rule_name="articles",
            message="GDELT returned article records.",
            metadata={"article_count": len(payload.get("articles", []))},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        payload = json.loads(raw_artifact.content.decode("utf-8", errors="ignore"))
        rows = []
        for article in payload.get("articles", []):
            if isinstance(article, dict):
                rows.append(article)
        return rows

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output = []
        for record in records:
            headline = _clean(record.get("title"))
            url = _clean(record.get("url"))
            if not headline or not url:
                continue
            published_at = _parse_gdelt_datetime(_clean(record.get("seendate")))
            available_at = published_at or raw_artifact.retrieved_at
            domain = _clean(record.get("domain"))
            event_type = classify_event_type(headline, domain, raw_artifact.source_family)
            news_id = stable_id("news", "gdelt", url, headline)
            row = {
                "news_id": news_id,
                "published_at": published_at,
                "source_name": domain or raw_artifact.metadata.get("source_name") or "GDELT DOC",
                "source_class": raw_artifact.metadata.get("source_class"),
                "headline": headline,
                "url": url,
                "language_code": _clean(record.get("language")),
                "geography": raw_artifact.metadata.get("geography") or "global",
                "event_type": event_type,
                "sentiment_score": normalized_sentiment_score(record.get("tone")),
                "novelty_score": Decimal("1"),
                "entities_json": entities_json(
                    headline,
                    domain,
                    {"domain": domain, "source_country": _clean(record.get("sourcecountry"))},
                ),
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


def _parse_gdelt_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split()).strip()
    return text or None
