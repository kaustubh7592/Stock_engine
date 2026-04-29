"""NSE derivatives EOD bhavcopy connector."""

from __future__ import annotations

import csv
import io
import ssl
import zipfile
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

NSE_FO_ARCHIVE_ROOT = "https://nsearchives.nseindia.com/content/fo"
NSE_LEGACY_DERIVATIVES_ROOT = "https://archives.nseindia.com/content/historical/DERIVATIVES"
PARSER_VERSION = "nse-derivatives-eod-v1"


class NSEDerivativesEODConnector(SourceConnector):
    """Connector for NSE F&O daily bhavcopy archives."""

    def __init__(
        self,
        source: SourceConfig,
        trade_date: date,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
    ) -> None:
        self.source = source
        self.trade_date = trade_date
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env

    def discover(self) -> list[SourceObject]:
        yyyymmdd = self.trade_date.strftime("%Y%m%d")
        ddmmmyyyy = self.trade_date.strftime("%d%b%Y").upper()
        year = self.trade_date.strftime("%Y")
        month = self.trade_date.strftime("%b").upper()
        return [
            SourceObject(
                source=self.source,
                logical_name=f"nse_fo_udiff_bhavcopy_{yyyymmdd}",
                url=(
                    f"{NSE_FO_ARCHIVE_ROOT}/"
                    f"BhavCopy_NSE_FO_0_0_0_{yyyymmdd}_F_0000.csv.zip"
                ),
                expected_extension="zip",
                as_of_date=self.trade_date,
                metadata={"report_type": "fo_udiff_common_bhavcopy_final"},
            ),
            SourceObject(
                source=self.source,
                logical_name=f"nse_fo_legacy_bhavcopy_{yyyymmdd}",
                url=(
                    f"{NSE_LEGACY_DERIVATIVES_ROOT}/{year}/{month}/"
                    f"fo{ddmmmyyyy}bhav.csv.zip"
                ),
                expected_extension="zip",
                as_of_date=self.trade_date,
                metadata={"report_type": "fo_legacy_bhavcopy"},
            ),
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "application/zip,text/csv,*/*"}
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
            metadata={"trade_date": self.trade_date.isoformat(), **source_object.metadata},
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded NSE derivatives EOD artifact is empty.",
                severity="high",
            )

        try:
            csv_name, csv_bytes = _extract_derivatives_csv(raw_artifact.content)
        except (zipfile.BadZipFile, ConnectorError) as exc:
            return ValidationResult(
                ok=False,
                rule_name="zip_contains_derivatives_bhavcopy",
                message=str(exc),
                severity="high",
            )

        header = csv_bytes[:1024].decode("utf-8-sig", errors="ignore")
        normalized_header = {
            _normalize_header(column) for column in header.splitlines()[0].split(",")
        }
        legacy_required = {"instrument", "symbol", "expiry_dt", "open_int"}
        udiff_required = {"fininstrmtp", "tckrsymb", "xprydt", "opnintrst"}
        if not (legacy_required <= normalized_header or udiff_required <= normalized_header):
            return ValidationResult(
                ok=False,
                rule_name="expected_derivatives_headers",
                message=f"{csv_name} is missing expected NSE derivatives bhavcopy headers.",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_derivatives_headers",
            message=f"{csv_name} looks like an NSE derivatives bhavcopy file.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        csv_name, csv_bytes = _extract_derivatives_csv(raw_artifact.content)
        text = csv_bytes.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        rows = [{_normalize_header(k): _clean(v) for k, v in row.items()} for row in reader]
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
            segment = _first(record, "instrument", "fininstrmtp", "segment")
            symbol = _first(record, "symbol", "tckrsymb", "underlying")
            expiry_date = _parse_date(_first(record, "expiry_dt", "xprydt", "exprydt"))
            trade_date = _parse_date(
                _first(record, "timestamp", "trad_dt", "traddt", "bizdt"),
                default=self.trade_date,
            )
            option_type = _option_type(_first(record, "option_typ", "optn_tp", "optnpt"))
            strike_price = _decimal_or_none(_first(record, "strike_pr", "strk_pric", "strkpric"))
            if not segment or not symbol or expiry_date is None or trade_date is None:
                continue

            contract_id = stable_id(
                "derivatives_contract",
                "NSE",
                segment,
                symbol,
                expiry_date,
                strike_price,
                option_type,
            )
            row = {
                "contract_id": contract_id,
                "instrument_id": stable_id("ins", f"NSE:{symbol}"),
                "__nse_symbol": symbol,
                "trade_date": trade_date,
                "segment": segment.upper(),
                "expiry_date": expiry_date,
                "strike_price": strike_price,
                "option_type": option_type,
                "settlement_price": _decimal_or_none(
                    _first(record, "settle_pr", "sttlmpric", "settlement_price")
                ),
                "open_interest": _int_or_none(_first(record, "open_int", "opnintrst")),
                "oi_change": _int_or_none(_first(record, "chg_in_oi", "chnginopnintrst")),
                "contract_volume": _int_or_none(
                    _first(record, "contracts", "ttltradgvol", "contract_volume")
                ),
                "source": raw_artifact.source_family,
                "source_url": raw_artifact.source_url,
                "retrieved_at": raw_artifact.retrieved_at,
                "available_at": raw_artifact.retrieved_at,
                "as_of_date": trade_date,
                "document_hash": None,
                "parser_version": PARSER_VERSION,
                "restated_flag": False,
                "created_at": raw_artifact.retrieved_at,
                "updated_at": raw_artifact.retrieved_at,
            }
            output.append(NormalizedRecord(table_name="derivatives_eod", row=row))
        return output


def _extract_derivatives_csv(content: bytes) -> tuple[str, bytes]:
    if content[:2] != b"PK":
        return "derivatives_bhavcopy.csv", content

    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        candidates = []
        for name in archive.namelist():
            file_name = name.lower().split("/")[-1]
            if not file_name.endswith(".csv"):
                continue
            if file_name.startswith("fo") or file_name.startswith("bhavcopy_nse_fo"):
                candidates.append(name)
        if not candidates:
            raise ConnectorError("NSE derivatives zip does not contain an F&O bhavcopy CSV.")
        selected = sorted(candidates)[0]
        return selected, archive.read(selected)


def _normalize_header(value: str | None) -> str:
    text = "" if value is None else value.strip().lower()
    return (
        text.replace(" ", "_")
        .replace(".", "")
        .replace("/", "_")
        .replace("-", "_")
        .replace("__", "_")
    )


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
        return int(Decimal(value.replace(",", "")))
    except (InvalidOperation, ValueError, AttributeError):
        return None


def _parse_date(value: str | None, default: date | None = None) -> date | None:
    if not value:
        return default
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return default


def _option_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().upper()
    if normalized in {"XX", "-", "NA", "NIL"}:
        return None
    if normalized in {"CE", "CA"}:
        return "CE"
    if normalized in {"PE", "PA"}:
        return "PE"
    return normalized
