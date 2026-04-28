"""RBI current rates connector for macro_series."""

from __future__ import annotations

import re
import ssl
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

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

RBI_HOME_URL = "https://www.rbi.org.in/"
PARSER_VERSION = "rbi-current-rates-html-v1"


class RBICurrentRatesConnector(SourceConnector):
    """Parse RBI's official current-rates panel into macro_series rows."""

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
                logical_name="rbi_current_rates",
                url=RBI_HOME_URL,
                expected_extension="html",
                metadata={"discovery_surface": "rbi_current_rates"},
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
        text = raw_artifact.content.decode("utf-8", errors="ignore")
        if "Policy Repo Rate" not in text:
            return ValidationResult(
                ok=False,
                rule_name="rbi_current_rates_markers",
                message="RBI current-rates page did not contain expected policy-rate markers.",
                severity="high",
            )
        return ValidationResult(
            ok=True,
            rule_name="rbi_current_rates_markers",
            message="RBI current-rates page contains expected markers.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        html = raw_artifact.content.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "lxml")
        observation_date = _observation_date(soup) or raw_artifact.retrieved_at.date()
        rows = []
        for section, table in _section_tables(soup):
            for table_row in table.find_all("tr"):
                cells = table_row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                name = _clean(cells[0].get_text(" ", strip=True))
                raw_value = _clean(cells[1].get_text(" ", strip=True).lstrip(":").strip())
                value = _decimal_or_none(raw_value)
                if not name or value is None or _is_range(raw_value):
                    continue
                rows.append(
                    {
                        "section": section,
                        "series_name": name,
                        "value_num": value,
                        "unit": _unit_for(name, raw_value),
                        "observation_date": observation_date,
                        "frequency": "daily",
                        "seasonal_adjustment": "unknown",
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
            series_name = str(record["series_name"])
            section = str(record["section"])
            observation_date = record["observation_date"]
            series_code = _series_code(section, series_name)
            row = {
                "series_code": series_code,
                "series_name": series_name,
                "source_family": raw_artifact.source_family.upper(),
                "observation_date": observation_date,
                "value_num": record["value_num"],
                "unit": record["unit"],
                "frequency": record["frequency"],
                "vintage_date": raw_artifact.retrieved_at.date(),
                "seasonal_adjustment": record["seasonal_adjustment"],
                "source": raw_artifact.source_family,
                "source_url": raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": raw_artifact.retrieved_at,
                "as_of_date": observation_date,
                "document_hash": None,
                "parser_version": PARSER_VERSION,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="macro_series", row=row))
        return output


def _section_tables(soup: BeautifulSoup) -> list[tuple[str, object]]:
    output = []
    for heading in soup.find_all("h3", class_="accordionButton"):
        section = _clean(heading.get_text(" ", strip=True))
        content = heading.find_next_sibling("div", class_="accordionContent")
        if not section or content is None:
            continue
        table = content.find("table")
        if table is not None:
            output.append((section, table))
    return output


def _observation_date(soup: BeautifulSoup) -> date | None:
    text = soup.get_text(" ", strip=True)
    match = re.search(r"As at\s+[^ ]+\s+of\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text)
    if match:
        return _parse_date(match.group(1))
    match = re.search(r"Last Updated\s*:?\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if match:
        return _parse_date(match.group(1))
    return None


def _parse_date(value: str) -> date | None:
    for fmt in ("%B %d, %Y", "%d %B %Y", "%b %d, %Y", "%d %b %Y"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _series_code(section: str, series_name: str) -> str:
    return stable_id("macro", "RBI", section, series_name)


def _unit_for(series_name: str, raw_value: str | None) -> str:
    text = f"{series_name} {raw_value or ''}".lower()
    if "%" in text or "rate" in text or "crr" in text or "slr" in text:
        return "percent"
    if "inr" in text:
        return "INR"
    return "value"


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("%", "").strip()
    text = text.removeprefix(":").strip()
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return Decimal(match.group(0))
    except (InvalidOperation, ValueError):
        return None


def _is_range(value: str | None) -> bool:
    return bool(value and re.search(r"\d(?:\.\d+)?%?\s*-\s*\d", value))


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None
