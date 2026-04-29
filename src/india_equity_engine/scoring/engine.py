"""Deterministic score snapshot engine."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from india_equity_engine.core.ids import stable_id
from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.core.time_utils import utc_now
from india_equity_engine.scoring.rules import (
    COMPONENTS,
    ComponentScore,
    component_scores_for_instrument,
)

HORIZONS = ("short", "medium", "long")


@dataclass(frozen=True)
class ConfidenceRules:
    minimum_confidence: dict[str, Decimal]
    missing_component_penalty: Decimal
    stale_market_data_penalty: Decimal
    stale_filing_data_penalty: Decimal
    severe_conflict_penalty: Decimal
    abstain_enabled: bool
    max_severe_conflicts: int


@dataclass(frozen=True)
class ScoreDecision:
    classification: str
    confidence: Decimal
    conflict_count: int
    abstain: bool
    conflicts: tuple[str, ...]
    abstain_reasons: tuple[str, ...]
    missing_components: tuple[str, ...]
    stale_components: tuple[str, ...]


def build_score_records(
    feature_rows: list[dict[str, Any]],
    event_signal_rows: list[dict[str, Any]],
    *,
    as_of_date: date,
    score_weights: dict[str, dict[str, Decimal]],
    confidence_rules: ConfidenceRules,
) -> list[NormalizedRecord]:
    """Build canonical score_snapshots records."""

    output = []
    by_instrument = _group_by_instrument(feature_rows)
    events_by_instrument = _group_by_instrument(event_signal_rows)
    for instrument_id in sorted(set(by_instrument) | set(events_by_instrument)):
        component_scores = component_scores_for_instrument(
            by_instrument.get(instrument_id, []),
            events_by_instrument.get(instrument_id, []),
        )
        for horizon in HORIZONS:
            weights = score_weights[horizon]
            composite = _composite_score(component_scores, weights)
            decision = _decision_for(
                component_scores,
                by_instrument.get(instrument_id, []),
                weights,
                composite,
                horizon,
                confidence_rules,
            )
            output.append(
                _score_record(
                    instrument_id,
                    as_of_date,
                    horizon,
                    component_scores,
                    composite,
                    decision,
                )
            )
    return output


def confidence_rules_from_config(raw: dict[str, Any]) -> ConfidenceRules:
    penalties = raw.get("penalties", {}) or {}
    abstain = raw.get("abstain", {}) or {}
    minimum = raw.get("minimum_confidence", {}) or {}
    return ConfidenceRules(
        minimum_confidence={
            horizon: _decimal(minimum.get(horizon), Decimal("0.5")) for horizon in HORIZONS
        },
        missing_component_penalty=_decimal(penalties.get("missing_component"), Decimal("0.08")),
        stale_market_data_penalty=_decimal(penalties.get("stale_market_data"), Decimal("0.12")),
        stale_filing_data_penalty=_decimal(penalties.get("stale_filing_data"), Decimal("0.15")),
        severe_conflict_penalty=_decimal(penalties.get("severe_conflict"), Decimal("0.20")),
        abstain_enabled=bool(abstain.get("enabled", True)),
        max_severe_conflicts=int(abstain.get("max_severe_conflicts", 1)),
    )


def score_weights_from_config(raw: dict[str, Any]) -> dict[str, dict[str, Decimal]]:
    output = {}
    for horizon in HORIZONS:
        weights = raw.get(horizon, {}) or {}
        horizon_weights = {
            component: _decimal(weights.get(component), Decimal("0"))
            for component in COMPONENTS
        }
        total = sum(horizon_weights.values())
        if total <= 0:
            horizon_weights = {component: Decimal("0") for component in COMPONENTS}
            horizon_weights["technical"] = Decimal("1")
        else:
            horizon_weights = {
                component: weight / total for component, weight in horizon_weights.items()
            }
        output[horizon] = horizon_weights
    return output


def _score_record(
    instrument_id: str,
    as_of_date: date,
    horizon: str,
    component_scores: dict[str, ComponentScore],
    composite: Decimal,
    decision: ScoreDecision,
) -> NormalizedRecord:
    now = utc_now()
    return NormalizedRecord(
        table_name="score_snapshots",
        row={
            "instrument_id": instrument_id,
            "as_of_date": as_of_date,
            "horizon": horizon,
            "technical_score": _score_or_none(component_scores["technical"].score),
            "fundamental_score": _score_or_none(component_scores["fundamental"].score),
            "governance_score": _score_or_none(component_scores["governance"].score),
            "macro_score": _score_or_none(component_scores["macro"].score),
            "event_score": _score_or_none(component_scores["event"].score),
            "derivatives_score": _score_or_none(component_scores["derivatives"].score),
            "peer_score": _score_or_none(component_scores["peer"].score),
            "composite_score": _quantize(composite),
            "confidence_score": _quantize(decision.confidence),
            "classification": decision.classification,
            "conflict_count": decision.conflict_count,
            "conflicts_json": _json_list(decision.conflicts),
            "abstain_reasons_json": _json_list(decision.abstain_reasons),
            "missing_components_json": _json_list(decision.missing_components),
            "positive_drivers_json": _json_list(_drivers(component_scores, "positive")),
            "negative_drivers_json": _json_list(_drivers(component_scores, "negative")),
            "risk_flags_json": _json_list(
                tuple(decision.stale_components) + _drivers(component_scores, "risk")
            ),
            "source": "deterministic_scoring_engine",
            "source_url": None,
            "retrieved_at": now,
            "available_at": now,
            "document_hash": stable_id(
                "score_input",
                instrument_id,
                as_of_date,
                horizon,
                composite,
                decision.confidence,
            ),
            "parser_version": "score-snapshots-v1",
            "restated_flag": False,
            "created_at": now,
            "updated_at": now,
        },
    )


def _composite_score(
    component_scores: dict[str, ComponentScore],
    weights: dict[str, Decimal],
) -> Decimal:
    numerator = Decimal("0")
    denominator = Decimal("0")
    for component, weight in weights.items():
        score = component_scores[component].score
        if score is None:
            continue
        numerator += score * weight
        denominator += weight
    if denominator == 0:
        return Decimal("0.5")
    return _clamp(numerator / denominator)


def _decision_for(
    component_scores: dict[str, ComponentScore],
    feature_rows: list[dict[str, Any]],
    weights: dict[str, Decimal],
    composite: Decimal,
    horizon: str,
    rules: ConfidenceRules,
) -> ScoreDecision:
    missing_components = tuple(
        component
        for component, weight in weights.items()
        if weight > 0 and component_scores[component].score is None
    )
    missing_weight = sum(weights[component] for component in missing_components)
    coverage_weight = sum(
        weights[component] * component_scores[component].coverage
        for component in COMPONENTS
        if component_scores[component].score is not None
    )
    conflicts = _conflicts(component_scores)
    stale_components = _stale_components(feature_rows, as_of_date=None)
    stale_penalty = _stale_penalty(stale_components, weights, rules)
    conflict_count = len(conflicts)
    confidence = (
        Decimal("0.35")
        + Decimal("0.55") * coverage_weight
        - rules.missing_component_penalty * missing_weight
        - rules.severe_conflict_penalty * Decimal(conflict_count)
        - stale_penalty
    )
    confidence = _clamp(confidence)
    abstain_reasons = []
    if confidence < rules.minimum_confidence[horizon]:
        abstain_reasons.append(
            f"confidence_below_{horizon}_minimum:{confidence:.4f}<{rules.minimum_confidence[horizon]:.4f}"
        )
    if conflict_count > rules.max_severe_conflicts:
        abstain_reasons.append(f"severe_conflicts:{conflict_count}")
    if missing_components:
        abstain_reasons.append("missing_components:" + ",".join(missing_components))
    if stale_components:
        abstain_reasons.append("stale_components:" + ",".join(stale_components))
    abstain = bool(
        rules.abstain_enabled
        and abstain_reasons
        and (
            confidence < rules.minimum_confidence[horizon]
            or conflict_count > rules.max_severe_conflicts
        )
    )
    if abstain:
        classification = "abstain"
    elif composite >= Decimal("0.60"):
        classification = "bullish"
    elif composite <= Decimal("0.40"):
        classification = "bearish"
    else:
        classification = "neutral"
    return ScoreDecision(
        classification=classification,
        confidence=confidence,
        conflict_count=conflict_count,
        abstain=abstain,
        conflicts=conflicts,
        abstain_reasons=tuple(abstain_reasons),
        missing_components=missing_components,
        stale_components=stale_components,
    )


def _conflicts(component_scores: dict[str, ComponentScore]) -> tuple[str, ...]:
    high = [
        (component, score.score)
        for component, score in component_scores.items()
        if score.score is not None and score.score >= Decimal("0.65")
    ]
    low = [
        (component, score.score)
        for component, score in component_scores.items()
        if score.score is not None and score.score <= Decimal("0.35")
    ]
    conflicts = []
    for high_component, high_score in high:
        for low_component, low_score in low:
            if high_component == low_component:
                continue
            conflicts.append(
                f"{high_component}_strong:{high_score:.4f}|{low_component}_weak:{low_score:.4f}"
            )
    return tuple(conflicts[:8])


def _stale_components(
    feature_rows: list[dict[str, Any]],
    *,
    as_of_date: date | None,
) -> tuple[str, ...]:
    grouped: dict[str, list[date]] = {}
    for row in feature_rows:
        family = row.get("feature_family")
        if not family:
            continue
        available_date = _date_or_none(row.get("available_at"))
        row_as_of_date = as_of_date or _date_or_none(row.get("as_of_date"))
        if available_date is None or row_as_of_date is None:
            continue
        grouped.setdefault(str(family), []).append(available_date)

    stale = []
    thresholds = {
        "technical": 7,
        "derivatives": 7,
        "peer": 7,
        "event": 14,
        "macro": 45,
        "fundamental": 120,
        "governance": 120,
    }
    for family, dates in grouped.items():
        latest = max(dates)
        reference = as_of_date or max(
            _date_or_none(row.get("as_of_date")) or latest
            for row in feature_rows
            if row.get("feature_family") == family
        )
        if (reference - latest).days > thresholds.get(family, 30):
            stale.append(family)
    return tuple(sorted(stale))


def _stale_penalty(
    stale_components: tuple[str, ...],
    weights: dict[str, Decimal],
    rules: ConfidenceRules,
) -> Decimal:
    penalty = Decimal("0")
    for component in stale_components:
        weight = weights.get(component, Decimal("0"))
        if component in {"technical", "derivatives", "peer", "event"}:
            penalty += rules.stale_market_data_penalty * weight
        elif component in {"fundamental", "governance"}:
            penalty += rules.stale_filing_data_penalty * weight
        else:
            penalty += (rules.stale_market_data_penalty / Decimal("2")) * weight
    return penalty


def _drivers(
    component_scores: dict[str, ComponentScore],
    kind: str,
) -> tuple[str, ...]:
    output = []
    for component, score in component_scores.items():
        if kind == "positive":
            output.extend(f"{component}:{driver}" for driver in score.positive_drivers)
        elif kind == "negative":
            output.extend(f"{component}:{driver}" for driver in score.negative_drivers)
        elif kind == "risk":
            output.extend(f"{component}:{driver}" for driver in score.risk_flags)
    return tuple(output[:12])


def _group_by_instrument(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        instrument_id = row.get("instrument_id")
        if not instrument_id:
            continue
        grouped.setdefault(str(instrument_id), []).append(row)
    return grouped


def _score_or_none(value: Decimal | None) -> Decimal | None:
    return _quantize(value) if value is not None else None


def _quantize(value: Decimal) -> Decimal:
    return _clamp(value).quantize(Decimal("0.0001"))


def _decimal(value: object, default: Decimal) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default


def _clamp(value: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), value))


def _json_list(values: tuple[str, ...]) -> str:
    return json.dumps(list(values), sort_keys=True)


def _date_or_none(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    text = str(value)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).date()
        except ValueError:
            continue
    return None
