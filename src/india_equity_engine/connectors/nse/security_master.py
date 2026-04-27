"""NSE security-master connector for EQUITY_L.csv."""

from __future__ import annotations

import csv
import io
import ssl
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

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


class NSEEquityListConnector(SourceConnector):
    """Connector for NSE's public EQUITY_L.csv master file."""

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
                logical_name="nse_equity_l_master",
                url=self.source.url,
                expected_extension="csv",
                as_of_date=date.today(),
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "text/csv,*/*"}
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
                message="Downloaded file is empty.",
            )

        text = raw_artifact.content[:512].decode("utf-8-sig", errors="ignore")
        required_headers = ("SYMBOL", "ISIN")
        missing = [header for header in required_headers if header not in text.upper()]
        if missing:
            return ValidationResult(
                ok=False,
                rule_name="expected_headers",
                message=f"Missing expected header markers: {', '.join(missing)}",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_headers",
            message="EQUITY_L.csv looks valid.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        text = raw_artifact.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        return [{_normalize_header(k): _clean(v) for k, v in row.items()} for row in reader]

    def normalize(
        self, records: list[dict[str, object]], raw_artifact: RawArtifact
    ) -> list[NormalizedRecord]:
        output: list[NormalizedRecord] = []

        for record in records:
            isin = _first(record, "isin_number", "isin")
            symbol = _first(record, "symbol")
            company_name = _first(record, "name_of_company", "company_name", "name")
            series = _first(record, "series")

            if not isin and not symbol:
                continue

            instrument_id = stable_id("ins", isin or f"NSE:{symbol}")
            listing_id = stable_id("listing", "NSE", symbol, series or "NA")

            output.append(
                NormalizedRecord(
                    table_name="instruments",
                    row={
                        "instrument_id": instrument_id,
                        "isin": isin,
                        "legal_name": company_name,
                        "issuer_name": company_name,
                        "country_code": "IN",
                        "industry_code": None,
                        "industry_name": None,
                        "sector_name": None,
                        "fo_eligible_flag": None,
                        "active_flag": True,
                    },
                )
            )
            output.append(
                NormalizedRecord(
                    table_name="listings",
                    row={
                        "listing_id": listing_id,
                        "instrument_id": instrument_id,
                        "exchange_code": "NSE",
                        "symbol": symbol,
                        "series": series,
                        "bse_scrip_code": None,
                        "listing_date": _parse_nse_date(_first(record, "date_of_listing")),
                        "delisting_date": None,
                        "face_value": _decimal_or_none(_first(record, "face_value")),
                        "market_lot": _int_or_none(_first(record, "market_lot")),
                    },
                )
            )

        return output


def _normalize_header(value: str | None) -> str:
    text = "" if value is None else value.strip().lower()
    return text.replace(" ", "_").replace(".", "").replace("/", "_")


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first(record: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _parse_nse_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _decimal_or_none(value: str | None) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(value.replace(",", ""))
    except (InvalidOperation, AttributeError):
        return None


def _int_or_none(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value.replace(",", ""))
    except ValueError:
        return None
