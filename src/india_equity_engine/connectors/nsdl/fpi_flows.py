"""NSDL FPI daily flow connector for market_flows."""

from __future__ import annotations

import re
import ssl
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from india_equity_engine.connectors.base import SourceConnector
from india_equity_engine.core.exceptions import ConnectorError
from india_equity_engine.core.schemas.contracts import (
    NormalizedRecord,
    RawArtifact,
    SourceConfig,
    SourceObject,
    ValidationResult,
)
from india_equity_engine.core.time_utils import utc_now

NSDL_FPI_LATEST_URLS = (
    "https://www.fpi.nsdl.co.in/Reports/Latest.aspx",
    "https://pilot.fpi.nsdl.co.in/Reports/Latest.aspx",
)
PARSER_VERSION = "nsdl-fpi-latest-html-v1"


class NSDLFPIFlowsConnector(SourceConnector):
    """Parse NSDL daily FPI/FII investment trends into market_flows."""

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
                logical_name="nsdl_fpi_latest",
                url=NSDL_FPI_LATEST_URLS[0],
                expected_extension="html",
                metadata={
                    "discovery_surface": "nsdl_fpi_latest",
                    "fallback_urls": list(NSDL_FPI_LATEST_URLS[1:]),
                },
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "text/html,*/*"}
        errors = []
        urls = [str(source_object.url), *source_object.metadata.get("fallback_urls", [])]
        for url in urls:
            with httpx.Client(
                timeout=self.timeout_seconds,
                follow_redirects=True,
                headers=headers,
                verify=self.verify,
                trust_env=self.trust_env,
            ) as client:
                try:
                    response = client.get(url)
                    response.raise_for_status()
                    return RawArtifact(
                        source_code=self.source.code,
                        source_family=self.source.family,
                        logical_name=source_object.logical_name,
                        source_url=str(response.url),
                        content=response.content,
                        extension=source_object.expected_extension,
                        retrieved_at=utc_now(),
                        content_type=response.headers.get("content-type"),
                        metadata={
                            **source_object.metadata,
                            "host": urlparse(str(response.url)).netloc,
                        },
                    )
                except httpx.HTTPError as exc:
                    errors.append(f"{url}: {exc}")
                    continue
        raise ConnectorError(f"Failed to download NSDL FPI latest report: {'; '.join(errors)}")

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        text = raw_artifact.content.decode("utf-8", errors="ignore")
        if "Daily Trends in FPI" not in text or "Gross Purchases" not in text:
            return ValidationResult(
                ok=False,
                rule_name="nsdl_fpi_markers",
                message="NSDL FPI latest page did not contain expected table markers.",
                severity="high",
            )
        return ValidationResult(
            ok=True,
            rule_name="nsdl_fpi_markers",
            message="NSDL FPI latest page contains expected markers.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        soup = BeautifulSoup(raw_artifact.content.decode("utf-8", errors="ignore"), "lxml")
        table = soup.find("div", id="rpt")
        if table is None:
            return []
        rows = []
        current_date: date | None = None
        current_segment: str | None = None
        conversion_rate: Decimal | None = None
        title_date = _date_from_title(table.get_text(" ", strip=True))
        for tr in table.find_all("tr"):
            cells = [_cell_text(cell) for cell in tr.find_all(["td", "th"])]
            if not cells or "Gross Purchases" in " ".join(cells):
                continue
            if len(cells) >= 8 and _parse_date(cells[0]):
                current_date = _parse_date(cells[0])
                current_segment = cells[1]
                route = cells[2]
                values = cells[3:7]
                conversion_rate = _decimal_or_none(cells[7]) or conversion_rate
            elif len(cells) >= 6 and current_date and _looks_like_segment(cells[0]):
                current_segment = cells[0]
                route = cells[1]
                values = cells[2:6]
            elif len(cells) >= 5 and current_date and current_segment:
                route = cells[0]
                values = cells[1:5]
            else:
                continue
            gross_buy, gross_sell, net_flow, net_usd = (_decimal_or_none(value) for value in values)
            if gross_buy is None and gross_sell is None and net_flow is None:
                continue
            rows.append(
                {
                    "trade_date": current_date or title_date,
                    "segment": current_segment,
                    "route": route,
                    "gross_buy": gross_buy,
                    "gross_sell": gross_sell,
                    "net_flow": net_flow,
                    "net_flow_usd_mn": net_usd,
                    "conversion_rate": conversion_rate,
                }
            )
        return [row for row in rows if row.get("trade_date") is not None]

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output = []
        for record in records:
            route = str(record["route"])
            segment = str(record["segment"])
            flow_type = f"fpi_net_investment_{_route_token(route)}"
            notes = (
                f"route={route}; net_flow_usd_mn={record.get('net_flow_usd_mn')}; "
                f"usd_inr={record.get('conversion_rate')}; host={raw_artifact.metadata.get('host')}"
            )
            row = {
                "trade_date": record["trade_date"],
                "flow_type": flow_type,
                "segment": _segment_token(segment),
                "investor_class": "FPI",
                "gross_buy": record.get("gross_buy"),
                "gross_sell": record.get("gross_sell"),
                "net_flow": record.get("net_flow"),
                "notes": notes,
                "source": raw_artifact.source_family,
                "source_url": raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": raw_artifact.retrieved_at,
                "as_of_date": record["trade_date"],
                "document_hash": None,
                "parser_version": PARSER_VERSION,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="market_flows", row=row))
        return output


def _cell_text(cell: object) -> str:
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _date_from_title(text: str) -> date | None:
    match = re.search(r"Daily Trends in FPI Investments on\s+(\d{1,2}-[A-Za-z]{3}-\d{4})", text)
    return _parse_date(match.group(1)) if match else None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("Rs.", "").replace("$", "").strip()
    negative = text.startswith("(") and text.endswith(")")
    text = text.strip("() ")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        parsed = Decimal(match.group(0))
    except (InvalidOperation, ValueError):
        return None
    return -parsed if negative else parsed


def _route_token(value: str) -> str:
    text = value.lower()
    if "sub" in text and "total" in text:
        return "subtotal"
    if "stock" in text:
        return "stock_exchange"
    if "primary" in text:
        return "primary_market"
    if "total" in text:
        return "total"
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_") or "unknown"


def _segment_token(value: str) -> str:
    text = value.lower().replace("/", "_")
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_") or "unknown"


def _looks_like_segment(value: str) -> bool:
    text = value.lower()
    return any(token in text for token in ("equity", "debt", "hybrid"))
