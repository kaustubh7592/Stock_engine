"""Feature-to-component scoring rules."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from india_equity_engine.features.common import decimal_or_none

COMPONENTS = ("technical", "fundamental", "governance", "macro", "event", "derivatives")


@dataclass(frozen=True)
class ComponentScore:
    component: str
    score: Decimal | None
    coverage: Decimal
    covered_features: int
    total_features: int
    positive_drivers: tuple[str, ...]
    negative_drivers: tuple[str, ...]
    risk_flags: tuple[str, ...]


def component_scores_for_instrument(
    feature_rows: list[dict[str, Any]],
    event_signal_rows: list[dict[str, Any]] | None = None,
) -> dict[str, ComponentScore]:
    """Score all supported components for one instrument from covered feature rows."""

    grouped = _group_features(feature_rows)
    scores = {
        "technical": _score_feature_group(
            "technical",
            grouped.get("technical", []),
            _technical_feature_score,
        ),
        "governance": _score_feature_group(
            "governance",
            grouped.get("governance", []),
            _governance_feature_score,
        ),
        "fundamental": _score_feature_group(
            "fundamental",
            grouped.get("fundamental", []),
            _fundamental_feature_score,
        ),
        "macro": _score_feature_group(
            "macro",
            grouped.get("macro", []),
            _macro_feature_score,
        ),
        "event": _event_score(
            grouped.get("event", []),
            event_signal_rows or [],
        ),
        "derivatives": _score_feature_group(
            "derivatives",
            grouped.get("derivatives", []),
            _derivatives_feature_score,
        ),
    }
    return scores


def _score_feature_group(
    component: str,
    rows: list[dict[str, Any]],
    scorer,
) -> ComponentScore:
    covered = [
        row
        for row in rows
        if row.get("coverage_flag") is True and row.get("value_num") is not None
    ]
    scored = [(row, scorer(row)) for row in covered]
    scored = [(row, score) for row, score in scored if score is not None]
    total_features = len(rows)
    if not scored:
        return ComponentScore(
            component=component,
            score=None,
            coverage=Decimal("0"),
            covered_features=0,
            total_features=total_features,
            positive_drivers=(),
            negative_drivers=(),
            risk_flags=(),
        )

    avg = sum(score for _, score in scored) / Decimal(len(scored))
    positives = []
    negatives = []
    risks = []
    for row, score in scored:
        name = str(row.get("feature_name") or "")
        value = row.get("value_num")
        if score >= Decimal("0.62"):
            positives.append(f"{name}={value}")
        elif score <= Decimal("0.38"):
            negatives.append(f"{name}={value}")
        if component == "governance" and score <= Decimal("0.35"):
            risks.append(name)
    coverage = Decimal(len(covered)) / Decimal(total_features or len(covered))
    return ComponentScore(
        component=component,
        score=_clamp_score(avg),
        coverage=coverage,
        covered_features=len(covered),
        total_features=total_features,
        positive_drivers=tuple(positives[:5]),
        negative_drivers=tuple(negatives[:5]),
        risk_flags=tuple(risks[:5]),
    )


def _technical_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name == "close_price":
        return None
    if name == "daily_return_pct":
        return _directional_score(value, Decimal("10"))
    if name == "return_5d_pct":
        return _directional_score(value, Decimal("20"))
    if name == "return_20d_pct":
        return _directional_score(value, Decimal("40"))
    if name == "volatility_20d_pct":
        return Decimal("1") - _bounded(value / Decimal("35"))
    if name == "delivery_pct_latest":
        return _directional_score(value - Decimal("45"), Decimal("55"))
    if name == "delivery_pct_vs_20d_avg":
        return _directional_score(value, Decimal("25"))
    if name == "intraday_range_pct":
        return Decimal("1") - _bounded(value / Decimal("18"))
    return None


def _governance_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name == "governance_risk_flag_365d":
        return Decimal("0.15") if value > 0 else Decimal("0.85")
    if name == "governance_high_severity_count_365d":
        return Decimal("1") - _bounded(value / Decimal("4"))
    if name == "governance_risk_event_count_365d":
        return Decimal("1") - _bounded(value / Decimal("6"))
    if name == "promoter_pledged_pct_total_equity_latest":
        return Decimal("1") - _bounded(value / Decimal("20"))
    if name == "promoter_pledged_pct_promoter_holding_latest":
        return Decimal("1") - _bounded(value / Decimal("50"))
    if name == "insider_net_value_180d":
        return _signed_money_score(value)
    if name == "insider_sell_value_180d":
        return Decimal("1") - _money_pressure(value)
    return None


def _fundamental_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name.endswith("_growth_1p_pct"):
        return _directional_score(value, Decimal("40"))
    if name == "fundamental_debt_to_assets":
        return Decimal("1") - _bounded(value / Decimal("0.8"))
    if name == "fundamental_net_profit_latest":
        return Decimal("0.65") if value > 0 else Decimal("0.30")
    if name == "fundamental_revenue_latest":
        return Decimal("0.60") if value > 0 else Decimal("0.40")
    if name == "fundamental_total_assets_latest":
        return Decimal("0.55") if value > 0 else Decimal("0.45")
    if name == "fundamental_total_borrowings_latest":
        return Decimal("0.50")
    if "promoter_pct_latest" in name:
        return _directional_score(value - Decimal("45"), Decimal("45"))
    if "fii_pct_latest" in name or "dii_pct_latest" in name:
        return _directional_score(value - Decimal("5"), Decimal("25"))
    if "retail_pct_latest" in name or "public_pct_latest" in name or "other_pct_latest" in name:
        return Decimal("0.50")
    if "share_count_latest" in name:
        return None
    if name.endswith("_change_1p"):
        return _directional_score(value, Decimal("8"))
    return None


def _macro_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name == "macro_policy_repo_rate_latest":
        return Decimal("1") - _bounded((value - Decimal("4")) / Decimal("6"))
    if name == "macro_reverse_repo_rate_latest":
        return Decimal("1") - _bounded((value - Decimal("3")) / Decimal("6"))
    if name == "macro_cpi_latest":
        return Decimal("1") - _bounded((value - Decimal("2")) / Decimal("6"))
    if name == "macro_iip_latest":
        return _directional_score(value, Decimal("10"))
    if name == "macro_gdp_latest":
        return _directional_score(value - Decimal("4"), Decimal("6"))
    if name in {
        "macro_fpi_total_net_flow_latest",
        "macro_fpi_equity_net_flow_latest",
        "macro_fpi_total_net_flow_5d",
    }:
        return _signed_money_score(value)
    return None


def _derivatives_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name == "derivatives_open_interest_latest":
        return None
    if name == "derivatives_contract_volume_latest":
        return None
    if name == "derivatives_oi_change_latest":
        return _directional_score(value, Decimal("500000"))
    if name == "derivatives_oi_change_pct_latest":
        return _directional_score(value, Decimal("30"))
    if name == "derivatives_put_call_oi_ratio_latest":
        return Decimal("1") - _bounded((value - Decimal("0.7")) / Decimal("1.3"))
    return None


def _event_feature_score(row: dict[str, Any]) -> Decimal | None:
    name = str(row.get("feature_name") or "")
    value = decimal_or_none(row.get("value_num"))
    if value is None:
        return None
    if name == "event_net_impact_30d":
        return _clamp_score(Decimal("0.5") + value)
    if name == "event_negative_signal_count_30d":
        return Decimal("1") - _bounded(value / Decimal("5"))
    if name == "event_positive_signal_count_30d":
        return _directional_score(value, Decimal("5"))
    if name == "event_signal_count_30d":
        return None
    return None


def _event_score(
    feature_rows: list[dict[str, Any]],
    event_signal_rows: list[dict[str, Any]],
) -> ComponentScore:
    feature_score = _score_feature_group("event", feature_rows, _event_feature_score)
    if feature_score.score is not None:
        return feature_score
    return _event_component_score(event_signal_rows)


def _event_component_score(rows: list[dict[str, Any]]) -> ComponentScore:
    instrument_rows = [row for row in rows if row.get("instrument_id")]
    if not instrument_rows:
        return _empty_score("event")
    values = []
    positives = []
    negatives = []
    risks = []
    for row in instrument_rows:
        impact = decimal_or_none(row.get("impact_score"))
        confidence = decimal_or_none(row.get("confidence")) or Decimal("0.5")
        direction = str(row.get("impact_direction") or "").lower()
        if impact is None:
            if direction == "positive":
                impact = Decimal("0.20")
            elif direction == "negative":
                impact = Decimal("-0.20")
            else:
                impact = Decimal("0")
        score = _clamp_score(Decimal("0.5") + (impact * confidence))
        values.append(score)
        label = f"{row.get('exposure_channel')}:{row.get('rationale_code')}"
        if score >= Decimal("0.62"):
            positives.append(label)
        elif score <= Decimal("0.38"):
            negatives.append(label)
        if score <= Decimal("0.35"):
            risks.append(label)
    return ComponentScore(
        component="event",
        score=sum(values) / Decimal(len(values)),
        coverage=Decimal("1"),
        covered_features=len(values),
        total_features=len(values),
        positive_drivers=tuple(positives[:5]),
        negative_drivers=tuple(negatives[:5]),
        risk_flags=tuple(risks[:5]),
    )


def _empty_score(component: str) -> ComponentScore:
    return ComponentScore(
        component=component,
        score=None,
        coverage=Decimal("0"),
        covered_features=0,
        total_features=0,
        positive_drivers=(),
        negative_drivers=(),
        risk_flags=(),
    )


def _group_features(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        family = str(row.get("feature_family") or "")
        if family:
            grouped[family].append(row)
    return grouped


def _directional_score(value: Decimal, scale: Decimal) -> Decimal:
    if scale == 0:
        return Decimal("0.5")
    return _clamp_score(Decimal("0.5") + Decimal("0.5") * _bounded_signed(value / scale))


def _signed_money_score(value: Decimal) -> Decimal:
    if value == 0:
        return Decimal("0.5")
    magnitude = Decimal(str(min(1.0, math.log10(abs(float(value)) + 1) / 8)))
    signed_magnitude = magnitude / Decimal("2") if value > 0 else -magnitude / Decimal("2")
    return _clamp_score(Decimal("0.5") + signed_magnitude)


def _money_pressure(value: Decimal) -> Decimal:
    if value <= 0:
        return Decimal("0")
    return Decimal(str(min(1.0, math.log10(float(value) + 1) / 8)))


def _bounded(value: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), value))


def _bounded_signed(value: Decimal) -> Decimal:
    return max(Decimal("-1"), min(Decimal("1"), value))


def _clamp_score(value: Decimal) -> Decimal:
    return max(Decimal("0"), min(Decimal("1"), value))
