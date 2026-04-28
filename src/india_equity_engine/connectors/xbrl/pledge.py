"""Map parsed XBRL numeric facts into canonical pledge disclosure rows."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now

PLEDGE_OUTPUT_FIELDS = (
    "promoter_shares",
    "pledged_shares",
    "pledged_pct_promoter_holding",
    "pledged_pct_total_equity",
)
_INTERNAL_FIELDS = PLEDGE_OUTPUT_FIELDS + ("total_shares",)


@dataclass(frozen=True)
class PledgeFact:
    instrument_id: str | None
    filing_id: str | None
    concept_name: str
    taxonomy_concept: str | None
    context_id: str | None
    context_text: str | None
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


def map_pledge_disclosures(facts: list[PledgeFact]) -> list[NormalizedRecord]:
    """Map pledge and encumbrance facts into one canonical row per filing period."""

    grouped: dict[tuple[object, object, object], dict[str, Any]] = defaultdict(dict)
    lineage: dict[tuple[object, object, object], dict[str, Any]] = {}
    priorities: dict[tuple[object, object, object], dict[str, int]] = defaultdict(dict)

    for fact in facts:
        field_name = pledge_field_for_concept(
            fact.concept_name,
            fact.taxonomy_concept,
            fact.context_text or fact.context_id,
        )
        if field_name is None or fact.instrument_id is None or fact.period_end is None:
            continue

        key = (fact.instrument_id, fact.filing_id, fact.period_end)
        row = grouped[key]
        if not row:
            row.update(
                {
                    "pledge_id": stable_id(
                        "pledge",
                        fact.instrument_id,
                        fact.filing_id,
                        fact.period_end,
                    ),
                    "instrument_id": fact.instrument_id,
                    "filing_id": fact.filing_id,
                    "period_end": fact.period_end,
                    "promoter_shares": None,
                    "pledged_shares": None,
                    "pledged_pct_promoter_holding": None,
                    "pledged_pct_total_equity": None,
                    "release_or_creation_flag": None,
                    "total_shares": None,
                    "_has_pledge_fact": False,
                }
            )
            lineage[key] = _lineage_from_fact(fact)

        value = _normalize_pledge_value(field_name, fact.value_num)
        priority = _fact_priority(field_name, fact)
        if priority >= priorities[key].get(field_name, -1):
            row[field_name] = value
            priorities[key][field_name] = priority
            lineage[key] = _lineage_from_fact(fact)
        if field_name in {
            "pledged_shares",
            "pledged_pct_promoter_holding",
            "pledged_pct_total_equity",
        }:
            row["_has_pledge_fact"] = True

    output = []
    now = utc_now()
    for key, row in grouped.items():
        _derive_missing_percentages(row)
        if not row.pop("_has_pledge_fact", False):
            continue
        row.pop("total_shares", None)
        if not any(row.get(field) is not None for field in PLEDGE_OUTPUT_FIELDS):
            continue
        row.update(_lineage_defaults(lineage.get(key, {}), now))
        output.append(NormalizedRecord(table_name="pledge_disclosures", row=row))
    return output


def pledge_field_for_concept(
    concept_name: str,
    taxonomy_concept: str | None = None,
    context_text: str | None = None,
) -> str | None:
    """Return the canonical pledge field for a parsed fact concept/context."""

    concept_text = _normalize_text(f"{concept_name} {taxonomy_concept or ''}")
    context = _normalize_text(context_text or "")

    if _is_pledged_share_count_concept(concept_text):
        return "pledged_shares"
    if _is_encumbrance_pct_concept(concept_text):
        if _is_promoter_group_context(context):
            return "pledged_pct_promoter_holding"
        if _is_total_shareholding_context(context) or not context:
            return "pledged_pct_total_equity"
        return None
    if _is_plain_share_count_concept(concept_text):
        if _is_promoter_group_context(context):
            return "promoter_shares"
        if _is_total_shareholding_context(context):
            return "total_shares"
    return None


def pledge_fact_from_row(row: dict[str, Any]) -> PledgeFact | None:
    value = _decimal_value(row.get("value_num"))
    if value is None:
        return None
    return PledgeFact(
        instrument_id=_string_or_none(row.get("instrument_id")),
        filing_id=_string_or_none(row.get("filing_id")),
        concept_name=str(row.get("concept_name") or ""),
        taxonomy_concept=_string_or_none(row.get("taxonomy_concept")),
        context_id=_string_or_none(row.get("context_id")),
        context_text=_string_or_none(row.get("context_text")),
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


def _derive_missing_percentages(row: dict[str, Any]) -> None:
    pledged_shares = _decimal_value(row.get("pledged_shares"))
    promoter_shares = _decimal_value(row.get("promoter_shares"))
    total_shares = _decimal_value(row.get("total_shares"))
    hundred = Decimal("100")

    if (
        row.get("pledged_pct_promoter_holding") is None
        and pledged_shares is not None
        and promoter_shares is not None
        and promoter_shares != 0
    ):
        row["pledged_pct_promoter_holding"] = pledged_shares / promoter_shares * hundred
    if (
        row.get("pledged_pct_total_equity") is None
        and pledged_shares is not None
        and total_shares is not None
        and total_shares != 0
    ):
        row["pledged_pct_total_equity"] = pledged_shares / total_shares * hundred


def _fact_priority(field_name: str, fact: PledgeFact) -> int:
    concept_text = _normalize_text(f"{fact.concept_name} {fact.taxonomy_concept or ''}")
    context = _normalize_text(fact.context_text or fact.context_id or "")
    priority = 0
    if field_name in {"promoter_shares", "pledged_pct_promoter_holding"}:
        if "shareholdingofpromoterandpromotergroup" in context:
            priority += 40
        elif _is_promoter_group_context(context):
            priority += 30
    elif field_name == "pledged_pct_total_equity":
        if _is_total_shareholding_context(context):
            priority += 40
    elif field_name in {"pledged_shares", "total_shares"}:
        if _is_total_shareholding_context(context):
            priority += 35
        elif "shareholdingofpromoterandpromotergroup" in context:
            priority += 30
        elif _is_promoter_group_context(context):
            priority += 20

    if "underpledged" in concept_text:
        priority += 10
    if "numberofshares" == concept_text or "numberofshares" in concept_text:
        priority += 3
    if fact.document_hash:
        priority += 1
    return priority


def _is_pledged_share_count_concept(text: str) -> bool:
    return _has_any(
        text,
        (
            "numberofsharesencumberedunderpledged",
            "numberofsharespledged",
            "numberofsharesencumbered",
        ),
    )


def _is_encumbrance_pct_concept(text: str) -> bool:
    return _has_any(
        text,
        (
            "encumberedshareunderpledgedaspercentage",
            "encumberedsharesheldaspercentage",
            "pledgedaspercentage",
            "encumberedaspercentage",
        ),
    )


def _is_plain_share_count_concept(text: str) -> bool:
    if _has_any(
        text,
        (
            "percentage",
            "numberofsharesundersubcategory",
            "numberofsharesonfullydilutedbasis",
            "numberofsharesoutstanding",
            "numberofsharesunderlyingoutstanding",
            "numberofequitysharesheldindematerializedform",
        ),
    ):
        return False
    return _has_any(
        text,
        (
            "numberofshares",
            "numberoffullypaidupequityshares",
            "totalnumberofsharesheld",
        ),
    ) and not _has_any(text, ("encumber", "pledge", "shareholder"))


def _is_promoter_group_context(text: str) -> bool:
    return _has_any(text, ("shareholdingofpromoterandpromotergroup", "promoterandpromotergroup"))


def _is_total_shareholding_context(text: str) -> bool:
    return _has_any(text, ("shareholdingpattern", "totalshareholding", "grandtotal"))


def _normalize_pledge_value(field_name: str, value: Decimal) -> Decimal | int:
    if field_name in {"promoter_shares", "pledged_shares", "total_shares"}:
        return _int_value(value)
    if Decimal("0") <= value <= Decimal("1"):
        return value * Decimal("100")
    return value


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


def _lineage_from_fact(fact: PledgeFact) -> dict[str, Any]:
    return {
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
