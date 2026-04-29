"""MoSPI latest releases connector for macro_series."""

from __future__ import annotations

import re
import ssl
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

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

MOSPI_LATEST_RELEASES_URL = "https://mospi.gov.in/latest-releases"
PARSER_VERSION = "mospi-latest-releases-html-v1"


class MoSPILatestReleasesConnector(SourceConnector):
    """Parse MoSPI release-list pages into point-in-time macro observations."""

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
                logical_name="mospi_latest_releases",
                url=MOSPI_LATEST_RELEASES_URL,
                expected_extension="html",
                metadata={"discovery_surface": "mospi_latest_releases"},
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
            source_url=str(response.url),
            content=response.content,
            extension=source_object.expected_extension,
            retrieved_at=utc_now(),
            content_type=response.headers.get("content-type"),
            metadata=source_object.metadata,
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        text = raw_artifact.content.decode("utf-8", errors="ignore")
        if not re.search(r"MoSPI|Statistics|Programme Implementation|latest releases", text, re.I):
            return ValidationResult(
                ok=False,
                rule_name="mospi_latest_release_markers",
                message="MoSPI latest-releases page did not contain expected markers.",
                severity="medium",
            )
        return ValidationResult(
            ok=True,
            rule_name="mospi_latest_release_markers",
            message="MoSPI latest-releases page contains expected markers.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        soup = BeautifulSoup(raw_artifact.content.decode("utf-8", errors="ignore"), "lxml")
        rows = []
        for block in _candidate_blocks(soup):
            text = _clean(block.get_text(" ", strip=True))
            if not text:
                continue
            indicator = _indicator_for(text)
            if indicator is None:
                continue
            value = _value_from_text(text)
            if value is None:
                continue
            release_date = _date_from_text(text) or raw_artifact.retrieved_at.date()
            observation_date = _period_end_from_text(text) or release_date
            link = block.find("a", href=True)
            rows.append(
                {
                    "indicator": indicator,
                    "series_name": _series_name(indicator, text),
                    "observation_date": observation_date,
                    "value_num": value,
                    "unit": "percent",
                    "frequency": "quarterly" if indicator == "gdp" else "monthly",
                    "vintage_date": release_date,
                    "seasonal_adjustment": "NSA",
                    "source_url": urljoin(raw_artifact.source_url, link["href"]) if link else None,
                }
            )
        return _dedupe_parsed(rows)

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output = []
        for record in records:
            series_name = str(record["series_name"])
            series_code = stable_id("macro", "MoSPI", record["indicator"], series_name)
            row = {
                "series_code": series_code,
                "series_name": series_name,
                "source_family": raw_artifact.source_family.upper(),
                "observation_date": record["observation_date"],
                "value_num": record["value_num"],
                "unit": record["unit"],
                "frequency": record["frequency"],
                "vintage_date": record["vintage_date"],
                "seasonal_adjustment": record["seasonal_adjustment"],
                "source": raw_artifact.source_family,
                "source_url": record.get("source_url") or raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": raw_artifact.retrieved_at,
                "as_of_date": record["observation_date"],
                "document_hash": None,
                "parser_version": PARSER_VERSION,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="macro_series", row=row))
        return output


def _candidate_blocks(soup: BeautifulSoup) -> list[object]:
    blocks = []
    for selector in ("tr", "li", "article", "div.views-row", "div.release"):
        blocks.extend(soup.select(selector))
    if not blocks:
        blocks = soup.find_all(["p", "div"])
    return blocks


def _indicator_for(text: str) -> str | None:
    lowered = text.lower()
    if "consumer price index" in lowered or re.search(r"\bcpi\b", lowered):
        return "cpi"
    if "index of industrial production" in lowered or re.search(r"\biip\b", lowered):
        return "iip"
    if "gross domestic product" in lowered or re.search(r"\bgdp\b", lowered):
        return "gdp"
    return None


def _series_name(indicator: str, text: str) -> str:
    lowered = text.lower()
    if indicator == "cpi":
        return "MoSPI CPI inflation latest release"
    if indicator == "iip":
        if "growth" in lowered:
            return "MoSPI IIP growth latest release"
        return "MoSPI IIP latest release"
    if indicator == "gdp":
        return "MoSPI GDP growth latest release"
    return "MoSPI macro latest release"


def _value_from_text(text: str) -> Decimal | None:
    patterns = (
        r"(?:inflation|growth|rate|increased|expanded)[^\d\-]{0,40}(-?\d+(?:\.\d+)?)\s*%",
        r"(-?\d+(?:\.\d+)?)\s*%\s*(?:inflation|growth|rate)",
        r"(?:cpi|iip|gdp)[^\d\-]{0,80}(-?\d+(?:\.\d+)?)\s*%",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            try:
                return Decimal(match.group(1))
            except (InvalidOperation, ValueError):
                return None
    percent_values = re.findall(r"(-?\d+(?:\.\d+)?)\s*%", text)
    if percent_values:
        try:
            return Decimal(percent_values[-1])
        except (InvalidOperation, ValueError):
            return None
    return None


def _period_end_from_text(text: str) -> date | None:
    quarter_match = re.search(r"Q([1-4])\s*(?:FY)?\s*(20\d{2})(?:[-/](\d{2}))?", text, re.I)
    if quarter_match:
        quarter = int(quarter_match.group(1))
        year = int(quarter_match.group(2))
        return {
            1: date(year, 6, 30),
            2: date(year, 9, 30),
            3: date(year, 12, 31),
            4: date(year + 1, 3, 31),
        }[quarter]
    month_matches = re.findall(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(20\d{2})",
        text,
        re.I,
    )
    if month_matches:
        month_name, year_text = month_matches[-1]
        month = datetime.strptime(month_name[:3], "%b").month
        year = int(year_text)
        next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
        return date.fromordinal(next_month.toordinal() - 1)
    return None


def _date_from_text(text: str) -> date | None:
    for pattern in (
        r"\b(\d{1,2}[/-]\d{1,2}[/-]20\d{2})\b",
        r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2})\b",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        value = match.group(1)
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _dedupe_parsed(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    deduped = {}
    for row in rows:
        key = (row["indicator"], row["series_name"], row["observation_date"], row["vintage_date"])
        deduped[key] = row
    return list(deduped.values())


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None
