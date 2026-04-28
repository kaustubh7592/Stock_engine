"""Rule-based governance event mapping from canonical disclosure tables."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now


@dataclass(frozen=True)
class GovernanceSourceRow:
    source_table: str
    source_id: str
    instrument_id: str | None
    event_date: object | None
    headline: str | None
    event_text: str | None
    related_filing_id: str | None
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


@dataclass(frozen=True)
class GovernanceClassification:
    event_type: str
    severity: str
    risk_flag: bool


def map_governance_events(rows: list[GovernanceSourceRow]) -> list[NormalizedRecord]:
    """Map source disclosure rows into normalized governance events."""

    output = []
    now = utc_now()
    for source_row in rows:
        classification = classify_governance_event(
            source_row.headline,
            source_row.event_text,
            source_row.source_table,
        )
        if classification is None or source_row.event_date is None:
            continue
        headline = source_row.headline or classification.event_type.replace("_", " ").title()
        event_id = stable_id(
            "gov_event",
            source_row.source_table,
            source_row.source_id,
            source_row.instrument_id,
            classification.event_type,
            source_row.event_date,
            headline,
        )
        row = {
            "governance_event_id": event_id,
            "instrument_id": source_row.instrument_id,
            "event_date": source_row.event_date,
            "event_type": classification.event_type,
            "severity": classification.severity,
            "headline": headline,
            "event_text": source_row.event_text,
            "related_filing_id": source_row.related_filing_id,
            "risk_flag": classification.risk_flag,
            "source": source_row.source or source_row.source_table,
            "source_url": source_row.source_url,
            "retrieved_at": source_row.retrieved_at or now,
            "available_at": source_row.available_at or now,
            "as_of_date": source_row.as_of_date or source_row.event_date,
            "document_hash": source_row.document_hash,
            "parser_version": source_row.parser_version,
            "restated_flag": bool(source_row.restated_flag)
            if source_row.restated_flag is not None
            else False,
            "created_at": source_row.created_at or now,
            "updated_at": source_row.updated_at or now,
        }
        output.append(NormalizedRecord(table_name="governance_events", row=row))
    return output


def classify_governance_event(
    headline: str | None,
    event_text: str | None = None,
    source_table: str | None = None,
) -> GovernanceClassification | None:
    """Classify a disclosure into a governance event using conservative keyword rules."""

    text = _normalize_text(f"{headline or ''} {event_text or ''}")
    if source_table == "pledge_disclosures":
        return _pledge_classification(event_text)
    if source_table == "insider_trades":
        return _insider_trade_classification(text)

    if _has_all(text, ("auditor", "resignation")) or _has_all(text, ("auditor", "resigned")):
        return GovernanceClassification("auditor_resignation", "high", True)
    if _has_any(
        text,
        (
            "auditqualification",
            "qualifiedopinion",
            "adverseopinion",
            "disclaimerofopinion",
            "statementonimpactofauditqualification",
        ),
    ):
        return GovernanceClassification("audit_qualification", "high", True)
    if _has_any(
        text,
        (
            "noncompliance",
            "penalty",
            "showcause",
            "regulatoryaction",
            "forensicaudit",
            "fraud",
            "default",
            "suspension",
        ),
    ) or ("sebi" in text and _has_any(text, ("order", "warning", "adjudication", "settlement"))):
        return GovernanceClassification("compliance_issue", "high", True)
    if _has_any(
        text,
        (
            "resignationofdirector",
            "resignationofindependentdirector",
            "appointmentofdirector",
            "changeindirector",
            "cessationofdirector",
            "keymanagerialpersonnel",
            "kmp",
        ),
    ):
        severity = "medium" if "resignation" in text or "cessation" in text else "low"
        return GovernanceClassification("board_change", severity, severity == "medium")
    if _has_any(
        text,
        (
            "qualifiedinstitutionalplacement",
            "preferentialissue",
            "preferentialallotment",
            "warrant",
            "rightissue",
            "fundraising",
            "fundraise",
            "conversionofsecurities",
            "allotmentofequity",
        ),
    ):
        return GovernanceClassification("dilution_capital_raise", "medium", False)
    if _has_any(text, ("pledge", "encumbrance", "encumbered", "invocation")):
        risk_flag = not _has_any(text, ("release", "revoke", "revocation"))
        return GovernanceClassification("promoter_pledge", "medium", risk_flag)
    return None


def governance_source_row_from_dict(row: dict[str, Any]) -> GovernanceSourceRow:
    return GovernanceSourceRow(
        source_table=str(row.get("source_table") or ""),
        source_id=str(row.get("source_id") or ""),
        instrument_id=_string_or_none(row.get("instrument_id")),
        event_date=row.get("event_date"),
        headline=_string_or_none(row.get("headline")),
        event_text=_string_or_none(row.get("event_text")),
        related_filing_id=_string_or_none(row.get("related_filing_id")),
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


def _pledge_classification(event_text: str | None) -> GovernanceClassification:
    pledge_pct = _extract_decimal_after(event_text, "pledged_pct_total_equity")
    if pledge_pct is not None and pledge_pct >= Decimal("10"):
        return GovernanceClassification("promoter_pledge", "high", True)
    if pledge_pct is not None and pledge_pct > Decimal("0"):
        return GovernanceClassification("promoter_pledge", "medium", True)
    return GovernanceClassification("promoter_pledge", "low", False)


def _insider_trade_classification(text: str) -> GovernanceClassification:
    if "pledge" in text:
        risk_flag = not _has_any(text, ("release", "revoke", "revocation"))
        return GovernanceClassification("insider_pledge_activity", "medium", risk_flag)
    if "promoter" in text and _has_any(text, ("sell", "sale")):
        return GovernanceClassification("insider_activity", "medium", True)
    return GovernanceClassification("insider_activity", "low", False)


def _normalize_text(value: str) -> str:
    return "".join(char.lower() for char in value if char.isalnum())


def _has_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def _has_all(text: str, tokens: tuple[str, ...]) -> bool:
    return all(token in text for token in tokens)


def _extract_decimal_after(value: str | None, key: str) -> Decimal | None:
    if not value:
        return None
    marker = f"{key}="
    for part in value.split(";"):
        text = part.strip()
        if not text.startswith(marker):
            continue
        try:
            return Decimal(text.removeprefix(marker).strip())
        except (InvalidOperation, ValueError):
            return None
    return None


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
