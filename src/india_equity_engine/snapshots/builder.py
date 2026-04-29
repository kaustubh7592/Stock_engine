"""Build strict stock_snapshot JSON payloads from deterministic tables."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

from india_equity_engine.core.schemas.snapshot_schema import (
    DecisionSnapshot,
    HorizonScore,
    InstrumentSnapshot,
    SignalSnapshot,
    SnapshotMeta,
    StockSnapshot,
)
from india_equity_engine.core.time_utils import utc_now
from india_equity_engine.features.common import decimal_or_none

SNAPSHOT_SCHEMA_VERSION = "1.0.0"
HORIZONS = ("short", "medium", "long")


def build_stock_snapshot_payload(
    *,
    instrument_id: str,
    as_of_date: date,
    instrument_row: dict[str, Any] | None,
    listing_rows: list[dict[str, Any]],
    feature_rows: list[dict[str, Any]],
    score_rows: list[dict[str, Any]],
) -> StockSnapshot:
    """Build and validate one stock_snapshot payload."""

    scores = _score_payload(score_rows)
    features = _feature_payload(feature_rows)
    positives, negatives, risks = _drivers(features, scores)
    decision = _decision(scores, positives, negatives, risks)
    return StockSnapshot(
        schema_version=SNAPSHOT_SCHEMA_VERSION,
        instrument=_instrument_payload(instrument_id, instrument_row, listing_rows),
        snapshot_meta=SnapshotMeta(
            as_of_date=as_of_date,
            generated_at=utc_now(),
            data_freshness=_freshness(features, scores),
        ),
        features=features,
        signals=_signals(scores, positives, negatives),
        scores=scores,
        decision=decision,
    )


def snapshot_driver_json(snapshot: StockSnapshot) -> tuple[list[str], list[str], list[str]]:
    return (
        snapshot.decision.top_positive_drivers,
        snapshot.decision.top_negative_drivers,
        snapshot.decision.key_risks,
    )


def _instrument_payload(
    instrument_id: str,
    instrument_row: dict[str, Any] | None,
    listing_rows: list[dict[str, Any]],
) -> InstrumentSnapshot:
    nse_listing = next(
        (row for row in listing_rows if str(row.get("exchange_code") or "").upper() == "NSE"),
        None,
    )
    bse_listing = next(
        (row for row in listing_rows if str(row.get("exchange_code") or "").upper() == "BSE"),
        None,
    )
    name = (
        _text((instrument_row or {}).get("legal_name"))
        or _text((instrument_row or {}).get("issuer_name"))
        or _text((nse_listing or {}).get("symbol"))
        or instrument_id
    )
    return InstrumentSnapshot(
        instrument_id=instrument_id,
        isin=_text((instrument_row or {}).get("isin")),
        name=name,
        nse_symbol=_text((nse_listing or {}).get("symbol")),
        bse_scrip_code=_text((bse_listing or {}).get("bse_scrip_code")),
        sector=_text((instrument_row or {}).get("sector_name")),
        industry=_text((instrument_row or {}).get("industry_name")),
    )


def _feature_payload(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = defaultdict(dict)
    for row in rows:
        family = _text(row.get("feature_family"))
        name = _text(row.get("feature_name"))
        if not family or not name:
            continue
        output[family][name] = {
            "value": _json_number(row.get("value_num")),
            "coverage": bool(row.get("coverage_flag")),
            "zscore": _json_number(row.get("zscore")),
            "rank_pct": _json_number(row.get("rank_pct")),
        }
    return dict(output)


def _score_payload(rows: list[dict[str, Any]]) -> dict[str, HorizonScore]:
    output = {}
    for row in rows:
        horizon = _text(row.get("horizon"))
        if horizon not in HORIZONS:
            continue
        output[horizon] = HorizonScore(
            technical=_float_or_none(row.get("technical_score")),
            fundamental=_float_or_none(row.get("fundamental_score")),
            governance=_float_or_none(row.get("governance_score")),
            macro=_float_or_none(row.get("macro_score")),
            events=_float_or_none(row.get("event_score")),
            derivatives=_float_or_none(row.get("derivatives_score")),
            composite=_float_or_default(row.get("composite_score"), 0.5),
            confidence=_float_or_default(row.get("confidence_score"), 0),
            classification=_classification(row.get("classification")),
        )
    return output


def _decision(
    scores: dict[str, HorizonScore],
    positives: list[str],
    negatives: list[str],
    risks: list[str],
) -> DecisionSnapshot:
    primary_horizon = _primary_horizon(scores)
    primary_score = scores.get(primary_horizon)
    classification = primary_score.classification if primary_score else "abstain"
    abstain = classification == "abstain"
    stance = {
        "bullish": "buy_candidate",
        "neutral": "watchlist",
        "bearish": "avoid_or_review",
        "abstain": "abstain_insufficient_confidence",
    }[classification]
    return DecisionSnapshot(
        primary_horizon=primary_horizon,
        stance=stance,
        abstain=abstain,
        key_risks=risks[:8],
        top_positive_drivers=positives[:8],
        top_negative_drivers=negatives[:8],
    )


def _primary_horizon(scores: dict[str, HorizonScore]) -> str:
    if "long" in scores and scores["long"].classification != "abstain":
        return "long"
    if not scores:
        return "long"
    return max(scores, key=lambda horizon: scores[horizon].confidence)


def _drivers(
    features: dict[str, dict[str, Any]],
    scores: dict[str, HorizonScore],
) -> tuple[list[str], list[str], list[str]]:
    positives = []
    negatives = []
    risks = []
    for horizon, score in scores.items():
        if score.composite >= 0.60:
            positives.append(f"{horizon} composite score {score.composite:.2f}")
        elif score.composite <= 0.40:
            negatives.append(f"{horizon} composite score {score.composite:.2f}")
        if score.classification == "abstain":
            risks.append(f"{horizon} horizon abstains due to low confidence or conflict")
    for family, family_features in features.items():
        for feature_name, payload in family_features.items():
            value = payload.get("value")
            covered = payload.get("coverage")
            if not covered:
                risks.append(f"missing coverage: {family}.{feature_name}")
                continue
            _feature_driver(family, feature_name, value, positives, negatives, risks)
    return positives, negatives, risks


def _feature_driver(
    family: str,
    feature_name: str,
    value: object,
    positives: list[str],
    negatives: list[str],
    risks: list[str],
) -> None:
    number = _float_or_none(value)
    if number is None:
        return
    label = f"{family}.{feature_name}={number:.4g}"
    if "return_" in feature_name and number > 0:
        positives.append(label)
    elif "return_" in feature_name and number < 0:
        negatives.append(label)
    elif "growth" in feature_name and number > 0:
        positives.append(label)
    elif "growth" in feature_name and number < 0:
        negatives.append(label)
    elif "risk_flag" in feature_name and number > 0:
        risks.append(label)
    elif "pledged_pct" in feature_name and number > 0:
        risks.append(label)
    elif "debt_to_assets" in feature_name and number > 0.7:
        risks.append(label)


def _signals(
    scores: dict[str, HorizonScore],
    positives: list[str],
    negatives: list[str],
) -> list[SignalSnapshot]:
    output = []
    for horizon, score in scores.items():
        direction = "unknown"
        if score.classification == "bullish":
            direction = "positive"
        elif score.classification == "bearish":
            direction = "negative"
        elif score.classification == "neutral":
            direction = "mixed"
        output.append(
            SignalSnapshot(
                name=f"{horizon}_deterministic_score",
                direction=direction,
                confidence=score.confidence,
                evidence_ids=[f"score_snapshots:{horizon}"],
            )
        )
    for index, driver in enumerate(positives[:3]):
        output.append(
            SignalSnapshot(
                name=f"positive_driver_{index + 1}",
                direction="positive",
                confidence=0.55,
                evidence_ids=[driver],
            )
        )
    for index, driver in enumerate(negatives[:3]):
        output.append(
            SignalSnapshot(
                name=f"negative_driver_{index + 1}",
                direction="negative",
                confidence=0.55,
                evidence_ids=[driver],
            )
        )
    return output


def _freshness(
    features: dict[str, dict[str, Any]],
    scores: dict[str, HorizonScore],
) -> dict[str, str]:
    return {
        "features": "fresh" if features else "missing",
        "scores": "fresh" if scores else "missing",
        "market_eod": "fresh" if features.get("technical") else "missing",
        "filings": "fresh"
        if features.get("fundamental") or features.get("governance")
        else "missing",
        "events": "fresh"
        if any(score.events is not None for score in scores.values())
        else "missing",
    }


def _classification(value: object) -> str:
    text = str(value or "abstain").lower()
    if text in {"bullish", "neutral", "bearish", "abstain"}:
        return text
    return "abstain"


def _json_number(value: object) -> float | None:
    parsed = _float_or_none(value)
    return parsed


def _float_or_default(value: object, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


def _float_or_none(value: object) -> float | None:
    decimal = decimal_or_none(value)
    if decimal is None:
        return None
    return float(decimal)


def _text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
