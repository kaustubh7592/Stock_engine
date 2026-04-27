"""Map parsed XBRL numeric facts into canonical shareholding rows."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now

SHAREHOLDING_FIELDS = (
    "promoter_pct",
    "public_pct",
    "fii_pct",
    "dii_pct",
    "retail_pct",
    "other_pct",
    "share_count",
)


@dataclass(frozen=True)
class ShareholdingFact:
    instrument_id: str | None
    filing_id: str | None
    concept_name: str
    taxonomy_concept: str | None
    period_end: object | None
    consolidated_flag: bool | None
    unit: str | None
    value_num: Decimal
    source: str | None
    source_url: str | None
    retrieved_at: object | None
    available_at: object | None
    as_of_date: object | None
    document_hash: str | None
    parser_version: str | None
    restated_flag: bool | None
    created_at: object | None
    updated_at: object | None


def map_shareholding_pattern(facts: list[ShareholdingFact]) -> list[NormalizedRecord]:
    """Map numeric ownership facts into one canonical row per period/context."""

    grouped: dict[tuple[object, object, object], dict[str, Any]] = defaultdict(dict)
    lineage: dict[tuple[object, object, object], dict[str, Any]] = {}

    for fact in facts:
        field_name = shareholding_field_for_concept(fact.concept_name, fact.taxonomy_concept)
        if field_name is None or fact.instrument_id is None or fact.period_end is None:
            continue

        consolidated_flag = (
            bool(fact.consolidated_flag) if fact.consolidated_flag is not None else False
        )
        key = (fact.instrument_id, fact.period_end, consolidated_flag)
        row = grouped[key]
        if not row:
            row.update(
                {
                    "instrument_id": fact.instrument_id,
                    "filing_id": fact.filing_id,
                    "period_end": fact.period_end,
                    "consolidated_flag": consolidated_flag,
                    "promoter_pct": None,
                    "public_pct": None,
                    "fii_pct": None,
                    "dii_pct": None,
                    "retail_pct": None,
                    "other_pct": None,
                    "share_count": None,
                }
            )
            lineage[key] = _lineage_from_fact(fact)

        value = _int_value(fact.value_num) if field_name == "share_count" else fact.value_num
        if row.get(field_name) is None or _is_more_specific(fact, lineage[key]):
            row[field_name] = value
            lineage[key] = _lineage_from_fact(fact)

    output = []
    now = utc_now()
    for key, row in grouped.items():
        if not any(row.get(field) is not None for field in SHAREHOLDING_FIELDS):
            continue
        row.update(_lineage_defaults(lineage.get(key, {}), now))
        output.append(NormalizedRecord(table_name="shareholding_pattern", row=row))
    return output


def shareholding_field_for_concept(
    concept_name: str,
    taxonomy_concept: str | None = None,
) -> str | None:
    """Return the canonical shareholding field for a parsed fact concept."""

    text = _normalize_text(f"{concept_name} {taxonomy_concept or ''}")
    if not _looks_like_shareholding_text(text):
        return None

    if _has_any(text, ("totalnumberofshares", "totalshareholding", "totalsharesheld")):
        return "share_count"
    if _has_any(text, ("promoterandpromotergroup", "promotergroup", "promoter")):
        return "promoter_pct"
    if _has_any(text, ("foreigninstitution", "foreignportfolio", "fpi", "fii")):
        return "fii_pct"
    if _has_any(text, ("domesticinstitution", "mutualfund", "insurancecompan", "dii")):
        return "dii_pct"
    if _has_any(text, ("retail", "individualshareholder", "noninstitution")):
        return "retail_pct"
    if _has_any(text, ("public", "publicshareholding", "publiccategory")):
        return "public_pct"
    if _has_any(text, ("other", "others")):
        return "other_pct"
    return None


def shareholding_fact_from_row(row: dict[str, Any]) -> ShareholdingFact | None:
    value = _decimal_value(row.get("value_num"))
    if value is None:
        return None
    return ShareholdingFact(
        instrument_id=_string_or_none(row.get("instrument_id")),
        filing_id=_string_or_none(row.get("filing_id")),
        concept_name=str(row.get("concept_name") or ""),
        taxonomy_concept=_string_or_none(row.get("taxonomy_concept")),
        period_end=row.get("period_end"),
        consolidated_flag=row.get("consolidated_flag"),
        unit=_string_or_none(row.get("unit")),
        value_num=value,
        source=_string_or_none(row.get("source")),
        source_url=_string_or_none(row.get("source_url")),
        retrieved_at=row.get("retrieved_at"),
        available_at=row.get("available_at"),
        as_of_date=row.get("as_of_date"),
        document_hash=_string_or_none(row.get("document_hash")),
        parser_version=_string_or_none(row.get("parser_version")),
        restated_flag=row.get("restated_flag"),
        created_at=row.get("created_at"),
        updated_at=row.get("updated_at"),
    )


def _looks_like_shareholding_text(text: str) -> bool:
    return _has_any(
        text,
        (
            "shareholding",
            "shareholder",
            "sharesheld",
            "holdingof",
            "promoter",
            "public",
            "foreigninstitution",
            "domesticinstitution",
            "fii",
            "dii",
        ),
    )


def _normalize_text(value: str) -> str:
    return "".join(char.lower() for char in value if char.isalnum())


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def _decimal_value(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_value(value: Decimal) -> int:
    return int(value.to_integral_value())


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _lineage_from_fact(fact: ShareholdingFact) -> dict[str, Any]:
    return {
        "filing_id": fact.filing_id,
        "source": fact.source,
        "source_url": fact.source_url,
        "retrieved_at": fact.retrieved_at,
        "available_at": fact.available_at,
        "as_of_date": fact.as_of_date or fact.period_end,
        "document_hash": fact.document_hash,
        "parser_version": fact.parser_version,
        "restated_flag": fact.restated_flag,
        "created_at": fact.created_at,
        "updated_at": fact.updated_at,
    }


def _lineage_defaults(lineage: dict[str, Any], now: object) -> dict[str, Any]:
    return {
        "source": lineage.get("source") or "xbrl",
        "source_url": lineage.get("source_url"),
        "retrieved_at": lineage.get("retrieved_at") or now,
        "available_at": lineage.get("available_at") or now,
        "as_of_date": lineage.get("as_of_date"),
        "document_hash": lineage.get("document_hash"),
        "parser_version": lineage.get("parser_version"),
        "restated_flag": (
            bool(lineage.get("restated_flag"))
            if lineage.get("restated_flag") is not None
            else False
        ),
        "created_at": lineage.get("created_at") or now,
        "updated_at": lineage.get("updated_at") or now,
    }


def _is_more_specific(fact: ShareholdingFact, current_lineage: dict[str, Any]) -> bool:
    return bool(fact.document_hash and not current_lineage.get("document_hash"))
