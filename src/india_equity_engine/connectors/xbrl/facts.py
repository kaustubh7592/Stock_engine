"""Lightweight XBRL numeric fact extraction.

This parser intentionally extracts only source-backed numeric facts. It does not try to
interpret PDFs or infer missing filing content.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from xml.etree import ElementTree

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord

XBRL_NAMESPACE_HINTS = (
    "http://www.xbrl.org/",
    "http://xbrl.org/",
)


@dataclass(frozen=True)
class XBRLContext:
    context_id: str
    period_end: date | None
    period_type: str
    consolidated_flag: bool | None
    context_text: str


@dataclass(frozen=True)
class XBRLArtifactMetadata:
    filing_id: str
    instrument_id: str | None
    source_url: str | None
    document_hash: str | None
    available_at: object | None
    as_of_date: object | None
    source: str | None = None


def parse_financial_facts_from_xml(
    content: bytes,
    metadata: XBRLArtifactMetadata,
    parser_version: str,
) -> list[NormalizedRecord]:
    """Parse numeric financial facts from one XBRL/XML artifact."""

    root = ElementTree.fromstring(content)
    contexts = _parse_contexts(root)
    units = _parse_units(root)
    output: list[NormalizedRecord] = []

    for element in root.iter():
        context_ref = element.attrib.get("contextRef")
        if not context_ref or context_ref not in contexts:
            continue

        value_num = _parse_decimal(element.text)
        if value_num is None:
            continue

        local_name = _local_name(element.tag)
        if _is_xbrl_infrastructure_name(local_name):
            continue

        context = contexts[context_ref]
        unit_ref = element.attrib.get("unitRef")
        unit = units.get(unit_ref or "", unit_ref)
        taxonomy_concept = _taxonomy_concept(element.tag)
        decimals = element.attrib.get("decimals")
        row = {
            "fact_id": stable_id(
                "fact",
                metadata.filing_id,
                taxonomy_concept,
                context_ref,
                unit_ref or "",
                str(value_num),
            ),
            "instrument_id": metadata.instrument_id,
            "filing_id": metadata.filing_id,
            "statement_type": _statement_type(local_name),
            "concept_name": _canonical_concept_name(local_name),
            "taxonomy_concept": taxonomy_concept,
            "period_end": context.period_end,
            "period_type": context.period_type,
            "consolidated_flag": context.consolidated_flag,
            "unit": unit,
            "value_num": value_num,
            "scale": f"decimals={decimals}" if decimals else None,
            "source": metadata.source or "xbrl",
            "source_url": metadata.source_url,
            "retrieved_at": metadata.available_at,
            "available_at": metadata.available_at,
            "as_of_date": metadata.as_of_date,
            "document_hash": metadata.document_hash,
            "parser_version": parser_version,
            "restated_flag": False,
            "created_at": metadata.available_at,
            "updated_at": metadata.available_at,
        }
        output.append(NormalizedRecord(table_name="financial_facts", row=row))

    return _dedupe_facts(output)


def _parse_contexts(root: ElementTree.Element) -> dict[str, XBRLContext]:
    contexts = {}
    for element in root.iter():
        if _local_name(element.tag) != "context":
            continue
        context_id = element.attrib.get("id")
        if not context_id:
            continue
        period_start = None
        period_end = None
        instant = None
        for child in element.iter():
            name = _local_name(child.tag)
            if name == "startDate":
                period_start = _parse_date(child.text)
            elif name == "endDate":
                period_end = _parse_date(child.text)
            elif name == "instant":
                instant = _parse_date(child.text)
        final_period_end = instant or period_end
        context_text = " ".join(text.strip() for text in element.itertext() if text.strip())
        contexts[context_id] = XBRLContext(
            context_id=context_id,
            period_end=final_period_end,
            period_type=_period_type(period_start, period_end, instant),
            consolidated_flag=_consolidated_flag(context_id, context_text),
            context_text=context_text,
        )
    return contexts


def _parse_units(root: ElementTree.Element) -> dict[str, str]:
    units = {}
    for element in root.iter():
        if _local_name(element.tag) != "unit":
            continue
        unit_id = element.attrib.get("id")
        if not unit_id:
            continue
        measures = [
            _clean_measure(child.text)
            for child in element.iter()
            if _local_name(child.tag) == "measure" and child.text
        ]
        units[unit_id] = " / ".join(measures) if measures else unit_id
    return units


def _parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    text = value.strip().replace(",", "")
    if not text or "-" in text[1:]:
        return None
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1]
    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return None
    return -parsed if negative else parsed


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _period_type(
    period_start: date | None,
    period_end: date | None,
    instant: date | None,
) -> str:
    if instant:
        return "instant"
    if not period_start or not period_end:
        return "duration"
    days = (period_end - period_start).days + 1
    if 75 <= days <= 100:
        return "Q"
    if 160 <= days <= 220:
        return "H"
    if 330 <= days <= 380:
        return "FY"
    return "duration"


def _consolidated_flag(context_id: str, context_text: str) -> bool | None:
    text = f"{context_id} {context_text}".lower()
    if "standalone" in text:
        return False
    if "consolidated" in text:
        return True
    return None


def _statement_type(local_name: str) -> str:
    normalized = local_name.lower()
    if any(token in normalized for token in ("cashflow", "cashflows", "operatingactivities")):
        return "cash_flow"
    if any(
        token in normalized
        for token in (
            "revenue",
            "income",
            "expense",
            "profit",
            "loss",
            "tax",
            "sales",
            "turnover",
            "depreciation",
            "earnings",
        )
    ):
        return "income_statement"
    if any(
        token in normalized
        for token in (
            "asset",
            "liabilit",
            "equity",
            "capital",
            "reserve",
            "debt",
            "receivable",
            "payable",
            "inventory",
        )
    ):
        return "balance_sheet"
    return "other"


def _canonical_concept_name(local_name: str) -> str:
    output = []
    for index, char in enumerate(local_name):
        if char.isupper() and index > 0 and local_name[index - 1].islower():
            output.append("_")
        output.append(char.lower() if char.isalnum() else "_")
    return "_".join(part for part in "".join(output).split("_") if part)


def _taxonomy_concept(tag: str) -> str:
    namespace, local_name = _namespace_and_local_name(tag)
    if namespace:
        return f"{namespace}#{local_name}"
    return local_name


def _namespace_and_local_name(tag: str) -> tuple[str | None, str]:
    if tag.startswith("{"):
        namespace, local_name = tag[1:].split("}", 1)
        return namespace, local_name
    return None, tag


def _local_name(tag: str) -> str:
    return _namespace_and_local_name(tag)[1]


def _clean_measure(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().split(":")[-1]


def _is_xbrl_infrastructure_name(local_name: str) -> bool:
    return local_name in {
        "context",
        "entity",
        "identifier",
        "period",
        "startDate",
        "endDate",
        "instant",
        "unit",
        "measure",
        "divide",
        "unitNumerator",
        "unitDenominator",
    }


def _dedupe_facts(records: list[NormalizedRecord]) -> list[NormalizedRecord]:
    deduped = {}
    for record in records:
        deduped[record.row["fact_id"]] = record
    return list(deduped.values())
