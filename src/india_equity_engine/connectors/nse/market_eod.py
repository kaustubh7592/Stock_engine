"""NSE cash-market EOD bhavcopy connector."""

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

NSE_CM_ARCHIVE_ROOT = "https://nsearchives.nseindia.com/content/cm"


class NSECashMarketEODConnector(SourceConnector):
    """Connector for NSE cash-market press-file bhavcopy zips."""

    def __init__(
        self,
        source: SourceConfig,
        trade_date: date,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
    ) -> None:
        self.source = source
        self.trade_date = trade_date
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify

    def discover(self) -> list[SourceObject]:
        ddmmyy = self.trade_date.strftime("%d%m%y")
        yyyymmdd = self.trade_date.strftime("%Y%m%d")
        return [
            SourceObject(
                source=self.source,
                logical_name=f"nse_cm_udiff_bhavcopy_{self.trade_date:%Y%m%d}",
                url=(
                    f"{NSE_CM_ARCHIVE_ROOT}/"
                    f"BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
                ),
                expected_extension="zip",
                as_of_date=self.trade_date,
                metadata={"report_type": "cm_udiff_common_bhavcopy_final"},
            ),
            SourceObject(
                source=self.source,
                logical_name=f"nse_cm_bhavcopy_pr_{self.trade_date:%Y%m%d}",
                url=f"{NSE_CM_ARCHIVE_ROOT}/PR{ddmmyy}.zip",
                expected_extension="zip",
                as_of_date=self.trade_date,
                metadata={"report_type": "cm_bhavcopy_pr"},
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {"User-Agent": self.user_agent, "Accept": "application/zip,*/*"}
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
            metadata={"trade_date": self.trade_date.isoformat()},
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded NSE cash-market EOD artifact is empty.",
            )

        try:
            csv_name, csv_bytes = _extract_bhavcopy_csv(raw_artifact.content)
        except (zipfile.BadZipFile, ConnectorError) as exc:
            return ValidationResult(
                ok=False,
                rule_name="zip_contains_bhavcopy",
                message=str(exc),
                severity="high",
            )

        header = csv_bytes[:512].decode("utf-8-sig", errors="ignore")
        normalized_header = {
            _normalize_header(column) for column in header.splitlines()[0].split(",")
        }
        old_required = {"symbol", "open", "high", "low", "close"}
        udiff_required = {"tckrsymb", "opnpric", "hghpric", "lwpric", "clspric"}
        if not (old_required <= normalized_header or udiff_required <= normalized_header):
            return ValidationResult(
                ok=False,
                rule_name="expected_price_headers",
                message=f"{csv_name} is missing expected bhavcopy price headers.",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_price_headers",
            message=f"{csv_name} looks like an NSE bhavcopy price file.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        csv_name, csv_bytes = _extract_bhavcopy_csv(raw_artifact.content)
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
            symbol = _first(record, "symbol", "tckrsymb")
            series = _first(record, "series", "sctysrs")
            isin = _first(record, "isin", "isin_number")

            if series and series.upper() not in {"EQ", "BE", "BZ", "SM", "ST"}:
                continue
            if not symbol:
                continue

            instrument_id = stable_id("ins", isin or f"NSE:{symbol}")
            trade_date = _parse_trade_date(
                _first(record, "timestamp", "date1", "date", "traddt", "bizdt"),
                default=self.trade_date,
            )
            traded_value = _decimal_or_none(
                _first(record, "tottrdval", "ttltrfval", "turnover_lacs", "turnover")
            )
            if traded_value is not None and "turnover_lacs" in record:
                traded_value *= Decimal("100000")

            output.append(
                NormalizedRecord(
                    table_name="price_daily",
                    row={
                        "instrument_id": instrument_id,
                        "__nse_symbol": symbol,
                        "trade_date": trade_date,
                        "open_price": _decimal_or_none(
                            _first(record, "open", "open_price", "opnpric")
                        ),
                        "high_price": _decimal_or_none(
                            _first(record, "high", "high_price", "hghpric")
                        ),
                        "low_price": _decimal_or_none(
                            _first(record, "low", "low_price", "lwpric")
                        ),
                        "close_price": _decimal_or_none(
                            _first(record, "close", "close_price", "clspric")
                        ),
                        "volume": _int_or_none(
                            _first(
                                record,
                                "tottrdqty",
                                "ttl_trd_qnty",
                                "ttltradgvol",
                                "volume",
                            )
                        ),
                        "traded_value": traded_value,
                        "deliverable_qty": _int_or_none(
                            _first(record, "deliv_qty", "deliverable_qty")
                        ),
                        "deliverable_pct": _decimal_or_none(
                            _first(record, "deliv_per", "deliverable_pct")
                        ),
                        "adj_close_price": None,
                        "adjustment_factor": None,
                        "source": raw_artifact.source_family,
                        "source_url": raw_artifact.source_url,
                        "retrieved_at": raw_artifact.retrieved_at,
                        "available_at": raw_artifact.retrieved_at,
                        "as_of_date": trade_date,
                        "document_hash": None,
                        "parser_version": None,
                        "restated_flag": False,
                        "created_at": raw_artifact.retrieved_at,
                        "updated_at": raw_artifact.retrieved_at,
                    },
                )
            )

        return output


class NSESecurityWiseDeliveryConnector(SourceConnector):
    """Connector for NSE security-wise full bhavcopy delivery data."""

    def __init__(
        self,
        source: SourceConfig,
        trade_date: date,
        user_agent: str,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
    ) -> None:
        self.source = source
        self.trade_date = trade_date
        self.user_agent = user_agent
        self.timeout_seconds = timeout_seconds
        self.verify = verify

    def discover(self) -> list[SourceObject]:
        ddmmyyyy = self.trade_date.strftime("%d%m%Y")
        return [
            SourceObject(
                source=self.source,
                logical_name=f"nse_sec_bhavdata_full_{self.trade_date:%Y%m%d}",
                url=(
                    "https://archives.nseindia.com/products/content/"
                    f"sec_bhavdata_full_{ddmmyyyy}.csv"
                ),
                expected_extension="csv",
                as_of_date=self.trade_date,
                metadata={"report_type": "security_wise_delivery_bhavcopy"},
            ),
            SourceObject(
                source=self.source,
                logical_name=f"nse_sec_bhavdata_full_{self.trade_date:%Y%m%d}",
                url=(
                    "https://nsearchives.nseindia.com/products/content/"
                    f"sec_bhavdata_full_{ddmmyyyy}.csv"
                ),
                expected_extension="csv",
                as_of_date=self.trade_date,
                metadata={"report_type": "security_wise_delivery_bhavcopy"},
            ),
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
            metadata={"trade_date": self.trade_date.isoformat()},
        )

    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        if not raw_artifact.content:
            return ValidationResult(
                ok=False,
                rule_name="non_empty",
                message="Downloaded NSE delivery artifact is empty.",
            )

        header = raw_artifact.content[:512].decode("utf-8-sig", errors="ignore")
        normalized_header = {
            _normalize_header(column) for column in header.splitlines()[0].split(",")
        }
        required = {"symbol", "series", "date1", "deliv_qty", "deliv_per"}
        if not required <= normalized_header:
            return ValidationResult(
                ok=False,
                rule_name="expected_delivery_headers",
                message="NSE delivery file is missing expected delivery headers.",
                severity="high",
            )

        return ValidationResult(
            ok=True,
            rule_name="expected_delivery_headers",
            message="NSE delivery file looks valid.",
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        text = raw_artifact.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        return [{_normalize_header(k): _clean(v) for k, v in row.items()} for row in reader]

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output: list[NormalizedRecord] = []

        for record in records:
            symbol = _first(record, "symbol")
            series = _first(record, "series")
            if series and series.upper() not in {"EQ", "BE", "BZ", "SM", "ST"}:
                continue
            if not symbol:
                continue

            trade_date = _parse_trade_date(_first(record, "date1", "date"), self.trade_date)
            traded_value = _decimal_or_none(_first(record, "turnover_lacs"))
            if traded_value is not None:
                traded_value *= Decimal("100000")

            output.append(
                NormalizedRecord(
                    table_name="price_daily",
                    row={
                        "instrument_id": stable_id("ins", f"NSE:{symbol}"),
                        "__nse_symbol": symbol,
                        "trade_date": trade_date,
                        "open_price": _decimal_or_none(_first(record, "open_price")),
                        "high_price": _decimal_or_none(_first(record, "high_price")),
                        "low_price": _decimal_or_none(_first(record, "low_price")),
                        "close_price": _decimal_or_none(_first(record, "close_price")),
                        "volume": _int_or_none(_first(record, "ttl_trd_qnty")),
                        "traded_value": traded_value,
                        "deliverable_qty": _int_or_none(_first(record, "deliv_qty")),
                        "deliverable_pct": _decimal_or_none(_first(record, "deliv_per")),
                        "adj_close_price": None,
                        "adjustment_factor": None,
                        "source": raw_artifact.source_family,
                        "source_url": raw_artifact.source_url,
                        "retrieved_at": raw_artifact.retrieved_at,
                        "available_at": raw_artifact.retrieved_at,
                        "as_of_date": trade_date,
                        "document_hash": None,
                        "parser_version": None,
                        "restated_flag": False,
                        "created_at": raw_artifact.retrieved_at,
                        "updated_at": raw_artifact.retrieved_at,
                    },
                )
            )

        return output


def _extract_bhavcopy_csv(zip_bytes: bytes) -> tuple[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        candidates = []
        for name in archive.namelist():
            file_name = name.lower().split("/")[-1]
            if not file_name.endswith(".csv"):
                continue
            if file_name.startswith("bh") or file_name.startswith("bhavcopy_nse_cm"):
                candidates.append(name)
        if not candidates:
            raise ConnectorError("NSE press-file zip does not contain a bh*.csv bhavcopy file.")
        selected = sorted(candidates)[0]
        return selected, archive.read(selected)


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


def _parse_trade_date(value: str | None, default: date) -> date:
    if not value:
        return default
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return default
