"""Deterministic score snapshot engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
            "composite_score": _quantize(composite),
            "confidence_score": _quantize(decision.confidence),
            "classification": decision.classification,
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
    weights: dict[str, Decimal],
    composite: Decimal,
    horizon: str,
    rules: ConfidenceRules,
) -> ScoreDecision:
    missing_weight = sum(
        weight for component, weight in weights.items() if component_scores[component].score is None
    )
    coverage_weight = sum(
        weights[component] * component_scores[component].coverage
        for component in COMPONENTS
        if component_scores[component].score is not None
    )
    conflict_count = _conflict_count(component_scores)
    confidence = (
        Decimal("0.35")
        + Decimal("0.55") * coverage_weight
        - rules.missing_component_penalty * missing_weight
        - rules.severe_conflict_penalty * Decimal(conflict_count)
    )
    confidence = _clamp(confidence)
    abstain = bool(
        rules.abstain_enabled
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
    )


def _conflict_count(component_scores: dict[str, ComponentScore]) -> int:
    available = [score.score for score in component_scores.values() if score.score is not None]
    high = any(score >= Decimal("0.65") for score in available)
    low = any(score <= Decimal("0.35") for score in available)
    return 1 if high and low else 0


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
