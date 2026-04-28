"""NSE insider trading (PIT) connector."""

from __future__ import annotations

import json
import ssl
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

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

NSE_PIT_API_URL = "https://www.nseindia.com/api/corporates-pit"
NSE_PIT_PAGE_URL = "https://www.nseindia.com/companies-listing/corporate-filings-pit-annual"


class NSEInsiderTradesConnector(SourceConnector):
    """Connector for NSE's public PIT insider-trading table."""

    def __init__(
        self,
        source: SourceConfig,
        user_agent: str,
        from_date: date,
        to_date: date,
        index: str = "equities",
        symbol: str | None = None,
        timeout_seconds: float = 30,
        verify: bool | str | ssl.SSLContext = True,
        trust_env: bool = False,
    ) -> None:
        self.source = source
        self.user_agent = user_agent
        self.from_date = from_date
        self.to_date = to_date
        self.index = index
        self.symbol = symbol.upper() if symbol else None
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.trust_env = trust_env

    def discover(self) -> list[SourceObject]:
        params = {
            "index": self.index,
            "from_date": _format_nse_date(self.from_date),
            "to_date": _format_nse_date(self.to_date),
        }
        if self.symbol:
            params["symbol"] = self.symbol
        url = f"{NSE_PIT_API_URL}?{urlencode(params)}"
        logical_name = f"nse_pit_{self.index}_{self.symbol or 'all'}_{self.to_date:%Y%m%d}"
        return [
            SourceObject(
                source=self.source,
                logical_name=logical_name,
                url=url,
                expected_extension="json",
                as_of_date=self.to_date,
                metadata={
                    "discovery_surface": "insider_trades_pit",
                    "index": self.index,
                    "symbol": self.symbol,
                    "from_date": self.from_date.isoformat(),
                    "to_date": self.to_date.isoformat(),
                },
            )
        ]

    def download(self, source_object: SourceObject) -> RawArtifact:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/json,text/plain,*/*",
            "Referer": NSE_PIT_PAGE_URL,
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
                message="Downloaded NSE PIT payload is empty.",
            )
        try:
            payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
        except json.JSONDecodeError as exc:
            return ValidationResult(
                ok=False,
                rule_name="json_parse",
                message=f"NSE PIT payload is not JSON: {exc}",
                severity="high",
            )

        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return ValidationResult(
                ok=False,
                rule_name="json_data_list",
                message="NSE PIT payload did not contain a data list.",
                severity="high",
            )
        if not rows:
            return ValidationResult(
                ok=False,
                rule_name="non_empty_rows",
                message="NSE PIT payload contained no insider-trading rows.",
                severity="medium",
            )
        return ValidationResult(
            ok=True,
            rule_name="json_rows",
            message="NSE PIT payload looks valid.",
            metadata={"row_count": len(rows)},
        )

    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        payload = json.loads(raw_artifact.content.decode("utf-8-sig"))
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        output = []
        for row in rows:
            if isinstance(row, dict):
                output.append(row)
        return output

    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        output = []
        for record in records:
            symbol = _text(record.get("symbol"))
            xbrl_url = _text(record.get("xbrl"))
            insider_name = _text(record.get("acqName"))
            transaction_type = _transaction_type(record)
            transaction_date = _transaction_date(record)
            quantity = _quantity(record, transaction_type)
            value_num = _decimal_or_none(record.get("secVal"))
            price = _price(value_num, quantity)
            post_holding = _int_or_none(record.get("afterAcqSharesNo"))
            filing_id = stable_id(
                "filing",
                "NSE",
                "insider_trading",
                symbol,
                xbrl_url or record.get("pid"),
            )
            insider_trade_id = stable_id(
                "insider_trade",
                "NSE",
                record.get("did") or record.get("pid"),
                symbol,
                insider_name,
                transaction_type,
                transaction_date,
                quantity,
                xbrl_url,
            )

            output.append(
                NormalizedRecord(
                    table_name="insider_trades",
                    row={
                        "insider_trade_id": insider_trade_id,
                        "instrument_id": stable_id("ins", f"NSE:{symbol}") if symbol else None,
                        "__nse_symbol": symbol,
                        "filing_id": filing_id,
                        "insider_name": insider_name,
                        "insider_category": _text(record.get("personCategory")),
                        "transaction_date": transaction_date,
                        "transaction_type": transaction_type,
                        "quantity": quantity,
                        "price": price,
                        "value_num": value_num,
                        "post_holding": post_holding,
                        "source": raw_artifact.source_family,
                        "source_url": xbrl_url or raw_artifact.source_url,
                        "retrieved_at": raw_artifact.retrieved_at,
                        "available_at": _parse_nse_datetime(_text(record.get("date")))
                        or raw_artifact.retrieved_at,
                        "as_of_date": transaction_date
                        or _parse_nse_date(_text(record.get("intimDt")))
                        or raw_artifact.retrieved_at.date(),
                        "document_hash": None,
                        "parser_version": None,
                        "restated_flag": False,
                        "created_at": raw_artifact.retrieved_at,
                        "updated_at": raw_artifact.retrieved_at,
                    },
                )
            )
        return output


def _format_nse_date(value: date) -> str:
    return value.strftime("%d-%m-%Y")


def _transaction_type(record: dict[str, object]) -> str | None:
    value = _text(record.get("tdpTransactionType"))
    if value and value != "-":
        return value
    mode = _text(record.get("acqMode"))
    if mode:
        lowered = mode.lower()
        if "sale" in lowered or "sell" in lowered:
            return "Sell"
        if "buy" in lowered or "purchase" in lowered or "acquisition" in lowered:
            return "Buy"
        if "pledge" in lowered:
            return mode
    return value


def _transaction_date(record: dict[str, object]) -> date | None:
    return (
        _parse_nse_date(_text(record.get("acqtoDt")))
        or _parse_nse_date(_text(record.get("acqfromDt")))
        or _parse_nse_datetime(_text(record.get("date")), date_only=True)
    )


def _quantity(record: dict[str, object], transaction_type: str | None) -> int | None:
    primary = _int_or_none(record.get("secAcq"))
    if primary is not None:
        return primary
    lowered = (transaction_type or "").lower()
    if "sell" in lowered or "sale" in lowered:
        return _int_or_none(record.get("sellquantity"))
    if "buy" in lowered:
        return _int_or_none(record.get("buyQuantity"))
    return _int_or_none(record.get("buyQuantity")) or _int_or_none(record.get("sellquantity"))


def _price(value_num: Decimal | None, quantity: int | None) -> Decimal | None:
    if value_num is None or value_num == 0 or quantity in {None, 0}:
        return None
    return (value_num / Decimal(quantity)).quantize(Decimal("0.0001"))


def _parse_nse_datetime(value: str | None, date_only: bool = False) -> datetime | date | None:
    if not value:
        return None
    for fmt in ("%d-%b-%Y %H:%M", "%d-%b-%Y %H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
            return parsed.date() if date_only else parsed
        except ValueError:
            continue
    parsed_date = _parse_nse_date(value)
    if parsed_date and date_only:
        return parsed_date
    if parsed_date:
        return datetime.combine(parsed_date, datetime.min.time())
    return None


def _parse_nse_date(value: str | None) -> date | None:
    if not value or value == "-":
        return None
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _decimal_or_none(value: object) -> Decimal | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return Decimal(text.replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: object) -> int | None:
    parsed = _decimal_or_none(value)
    if parsed is None:
        return None
    return int(parsed.to_integral_value())


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "-":
        return None
    return text
