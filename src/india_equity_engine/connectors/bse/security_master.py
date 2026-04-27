"""BSE security-master connectors for listing aliases."""

from __future__ import annotations

import csv
import io
import json
import ssl
import zipfile
from datetime import date
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

BSE_LIST_OF_SECURITIES_API = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
BSE_SCRIP_BSE_ZIP_URL = "https://www.bseindia.com/downloads1/Scrip_BSE.zip"
BSE_SCRIP_ZIP_URL = "https://www.bseindia.com/downloads1/SCRIP.zip"


class BSEScripMasterConnector(SourceConnector):
    """Connector for BSE's official SCRIP.zip / Scrip_BSE.zip security master."""

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
                logical_name="bse_scrip_bse_master",
                url=BSE_SCRIP_BSE_ZIP_URL,
                expected_extension="zip",
                as_of_date=date.today(),
                metadata={"coverage": "bse_only"},
            ),
            SourceObject(
                source=self.source,
                logical_name="bse_scrip_master",
                url=BSE_SCRIP_ZIP_URL,
                expected_extension="zip",
                as_of_date=date.today(),
                metadata={"coverage": "bse_plus_nse_exclusive"},
            ),
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/zip,*/*",
            "Referer": "https://www.bseindia.com/downloads/help/file/",
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
            metadata=source_object.metadata,
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded BSE scrip master artifact is empty.",
            )

        try:
            csv_name, csv_bytes = _extract_scrip_master_file(raw_artifact.content)
        except (zipfile.BadZipFile, ConnectorError) as exc:
            return ValidationResult(
                ok=False,
                rule_name="zip_contains_scrip_master",
                message=str(exc),
                severity="high",
            )

        header = csv_bytes[:1024].decode("utf-8-sig", errors="ignore")
        rows = _read_delimited_rows(header + "\n")
        header_keys = set(rows[0]) if rows else set()
        if not (set(_SCRIP_CODE_KEYS) & header_keys and set(_ISIN_KEYS) & header_keys):
            return ValidationResult(
                ok=False,
                rule_name="expected_scrip_master_headers",
                message=f"{csv_name} does not expose expected scrip-code and ISIN fields.",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_scrip_master_headers",
            message=f"{csv_name} looks like a BSE scrip master file.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        csv_name, csv_bytes = _extract_scrip_master_file(raw_artifact.content)
        text = csv_bytes.decode("utf-8-sig", errors="replace")
        rows = _read_delimited_rows(text)
        for row in rows:
            row["_source_file"] = csv_name
        return rows

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output: list[NormalizedRecord] = []

        for record in records:
            scrip_code = _first(record, *_SCRIP_CODE_KEYS)
            scrip_id = _first(record, *_SYMBOL_KEYS)
            security_name = _first(record, *_SECURITY_NAME_KEYS)
            isin = _first(record, *_ISIN_KEYS)
            group = _first(record, *_GROUP_KEYS)
            security_type = _first(record, *_SECURITY_TYPE_KEYS)

            if security_type and security_type.strip().upper() not in {"EQ", "EQUITY SECURITY"}:
                continue
            if not scrip_code and not isin:
                continue

            instrument_id = stable_id("ins", isin or f"BSE:{scrip_code}")
            listing_id = stable_id("listing", "BSE", scrip_code or scrip_id)

            output.append(
                NormalizedRecord(
                    table_name="instruments",
                    row={
                        "instrument_id": instrument_id,
                        "isin": isin,
                        "legal_name": security_name,
                        "issuer_name": security_name,
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
                        "exchange_code": "BSE",
                        "symbol": scrip_id,
                        "series": group,
                        "bse_scrip_code": scrip_code,
                        "listing_date": None,
                        "delisting_date": None,
                        "face_value": _bse_face_value(_first(record, *_FACE_VALUE_KEYS)),
                        "market_lot": _int_or_none(_first(record, *_MARKET_LOT_KEYS)),
                    },
                )
            )

        return output


class BSEListOfSecuritiesConnector(SourceConnector):
    """Connector for the BSE list-of-securities API used by the public page."""

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
                logical_name="bse_list_of_securities_active_equity",
                url=BSE_LIST_OF_SECURITIES_API,
                expected_extension="json",
                as_of_date=date.today(),
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.bseindia.com/corporates/List_Scrips.html",
            "Origin": "https://www.bseindia.com",
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
                message="Downloaded BSE securities payload is empty.",
            )

        try:
            payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
        except json.JSONDecodeError as exc:
            return ValidationResult(
                ok=False,
                rule_name="json_parse",
                message=f"BSE securities payload is not valid JSON: {exc}",
                severity="high",
            )

        rows = _extract_rows(payload)
        if not rows:
            return ValidationResult(
                ok=False,
                rule_name="non_empty_rows",
                message="BSE securities payload did not contain rows.",
                severity="high",
            )

        sample = {_normalize_key(key) for key in rows[0]}
        if not ({"scrip_cd", "scripcode", "security_code"} & sample):
            return ValidationResult(
                ok=False,
                rule_name="expected_scrip_code",
                message="BSE securities payload did not expose a scrip-code field.",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_payload",
            message="BSE securities payload looks valid.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
        rows = _extract_rows(payload)
        return [{_normalize_key(k): _clean(v) for k, v in row.items()} for row in rows]

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output: list[NormalizedRecord] = []

        for record in records:
            scrip_code = _first(record, "scrip_cd", "scripcode", "security_code")
            scrip_id = _first(record, "scrip_id", "security_id", "symbol")
            security_name = _first(record, "scrip_name", "security_name", "issuer_name")
            issuer_name = _first(record, "issuer_name", "company_name") or security_name
            isin = _first(record, "isin_number", "isin_no", "isin")
            status = _first(record, "status")
            group = _first(record, "group")

            if not scrip_code and not isin:
                continue

            instrument_id = stable_id("ins", isin or f"BSE:{scrip_code}")
            listing_id = stable_id("listing", "BSE", scrip_code or scrip_id)

            output.append(
                NormalizedRecord(
                    table_name="instruments",
                    row={
                        "instrument_id": instrument_id,
                        "isin": isin,
                        "legal_name": issuer_name,
                        "issuer_name": issuer_name,
                        "country_code": "IN",
                        "industry_code": None,
                        "industry_name": None,
                        "sector_name": None,
                        "fo_eligible_flag": None,
                        "active_flag": _is_active(status),
                    },
                )
            )
            output.append(
                NormalizedRecord(
                    table_name="listings",
                    row={
                        "listing_id": listing_id,
                        "instrument_id": instrument_id,
                        "exchange_code": "BSE",
                        "symbol": scrip_id,
                        "series": group,
                        "bse_scrip_code": scrip_code,
                        "listing_date": None,
                        "delisting_date": None,
                        "face_value": _decimal_or_none(_first(record, "face_value")),
                        "market_lot": None,
                    },
                )
            )

        return output


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("Table", "Table1", "data", "Data", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


_SCRIP_CODE_KEYS = (
    "scrip_code",
    "scrip_cd",
    "scripcode",
    "security_code",
    "sc_code",
)
_SYMBOL_KEYS = (
    "instrument_code",
    "scrip_id",
    "security_id",
    "symbol",
    "sc_symbol",
)
_GROUP_KEYS = ("group_name", "group", "sc_group")
_SECURITY_NAME_KEYS = (
    "scrip_name",
    "security_name",
    "issuer_name",
    "company_name",
    "sc_name",
)
_ISIN_KEYS = ("isin_code", "isin_number", "isin_no", "isin")
_MARKET_LOT_KEYS = ("market_lot", "marketlot")
_FACE_VALUE_KEYS = ("face_value", "facevalue")
_SECURITY_TYPE_KEYS = ("security_type_flag", "security_type", "sc_type")


def _extract_scrip_master_file(zip_bytes: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        candidates = []
        for name in archive.namelist():
            file_name = name.lower().split("/")[-1]
            if not file_name.endswith((".csv", ".txt")):
                continue
            if "scrip" in file_name:
                candidates.append(name)
        if not candidates:
            raise ConnectorError("BSE zip does not contain a scrip master CSV/TXT file.")
        selected = sorted(candidates, key=_scrip_file_priority)[0]
        return selected, archive.read(selected)


def _scrip_file_priority(name: str) -> tuple[int, str]:
    file_name = name.lower().split("/")[-1]
    if file_name.startswith("bse_eq_scrip"):
        return (0, file_name)
    if file_name.startswith("scrip_bse"):
        return (1, file_name)
    if file_name.startswith("scrip"):
        return (2, file_name)
    return (3, file_name)


def _read_delimited_rows(text: str) -> list[dict[str, object]]:
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",|;\t")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows = []
    for row in reader:
        rows.append({_normalize_key(k): _clean(v) for k, v in row.items() if k is not None})
    return rows


def _normalize_key(value: str | None) -> str:
    text = "" if value is None else value.strip().lower()
    text = text.replace(" ", "_").replace(".", "").replace("/", "_")
    return text


def _clean(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return None if text in ("", "-", "null", "None") else text


def _first(record: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
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


def _bse_face_value(value: str | None) -> Decimal | None:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None
    if parsed >= Decimal("100"):
        return parsed / Decimal("100")
    return parsed


def _is_active(status: str | None) -> bool:
    if status is None:
        return True
    return status.strip().lower() in {"active", "a", "listed"}
