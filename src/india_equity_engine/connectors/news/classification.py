"""Deterministic event/news classification helpers."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

EXPOSURE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "oil_fuel": (
        "crude",
        "diesel",
        "fuel",
        "gas",
        "lng",
        "oil",
        "petroleum",
        "refinery",
    ),
    "fx": (
        "currency",
        "dollar",
        "export",
        "exchange rate",
        "fema",
        "forex",
        "import",
        "inr",
        "rupee",
        "usd",
    ),
    "rates_liquidity": (
        "crr",
        "liquidity",
        "monetary policy",
        "mpc",
        "policy rate",
        "rate",
        "repo",
        "sdf",
        "slr",
    ),
    "government_capex_budget": (
        "budget",
        "capex",
        "capital expenditure",
        "defence",
        "epc",
        "infrastructure",
        "railway",
        "road",
        "tax",
        "union budget",
    ),
    "rural_agriculture": (
        "agri",
        "agriculture",
        "crop",
        "farmer",
        "fertiliser",
        "fertilizer",
        "irrigation",
        "kharif",
        "rabi",
        "rural",
        "tractor",
    ),
    "commodity_input_cost": (
        "aluminium",
        "cement",
        "coal",
        "commodity",
        "copper",
        "metal",
        "steel",
    ),
    "governance_regulation": (
        "circular",
        "compliance",
        "guideline",
        "nbfc",
        "notification",
        "regulation",
        "regulatory",
        "sebi",
    ),
    "election": (
        "election",
        "eci",
        "model code",
        "poll",
        "result",
        "voting",
    ),
    "geopolitical": (
        "conflict",
        "geopolitical",
        "sanction",
        "tariff",
        "war",
    ),
}


EVENT_TYPE_BY_CHANNEL: dict[str, str] = {
    "oil_fuel": "commodity_energy",
    "fx": "fx_currency",
    "rates_liquidity": "macro_policy",
    "government_capex_budget": "budget_policy",
    "rural_agriculture": "rural_agriculture",
    "commodity_input_cost": "commodity_input_cost",
    "governance_regulation": "regulation",
    "election": "election",
    "geopolitical": "geopolitical",
}


def classify_event_type(
    headline: str | None,
    summary: str | None = None,
    source_family: str | None = None,
) -> str:
    """Classify an item into a coarse event type from transparent keyword rules."""

    text = normalize_search_text(f"{headline or ''} {summary or ''}")
    if source_family == "gdelt" and _has_any(text, EXPOSURE_KEYWORDS["geopolitical"]):
        return "geopolitical"
    for channel, keywords in EXPOSURE_KEYWORDS.items():
        if _has_any(text, keywords):
            return EVENT_TYPE_BY_CHANNEL[channel]
    if source_family == "rbi":
        return "macro_policy"
    if source_family == "budget":
        return "budget_policy"
    if source_family == "eci":
        return "election"
    if source_family == "pib":
        return "government_policy"
    return "general_news"


def entities_json(
    headline: str | None,
    summary: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    """Return compact JSON with extracted themes, channels, and sector buckets."""

    text = normalize_search_text(f"{headline or ''} {summary or ''}")
    channels = [
        channel
        for channel, keywords in EXPOSURE_KEYWORDS.items()
        if _has_any(text, keywords)
    ]
    sectors = sorted({sector for channel in channels for sector in sectors_for_channel(channel)})
    payload: dict[str, Any] = {
        "exposure_channels": channels,
        "sectors": sectors,
    }
    if extra:
        payload.update({key: value for key, value in extra.items() if value not in (None, "", [])})
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def sectors_for_channel(channel: str) -> tuple[str, ...]:
    """Return broad affected sector buckets used by the first event mapper."""

    return {
        "oil_fuel": ("Aviation", "Chemicals", "Logistics", "Oil & Gas", "Paints"),
        "fx": ("Aviation", "IT Services", "Import-Heavy Manufacturing", "Pharmaceuticals"),
        "rates_liquidity": ("Banks", "Infrastructure", "NBFC", "Real Estate", "Utilities"),
        "government_capex_budget": (
            "Capital Goods",
            "Cement",
            "Defence",
            "Infrastructure",
            "Railways",
        ),
        "rural_agriculture": (
            "Agrochemicals",
            "Fertilizers",
            "FMCG",
            "Irrigation",
            "Two Wheelers",
        ),
        "commodity_input_cost": ("Auto Ancillaries", "Cement", "Chemicals", "Metals", "Packaging"),
        "governance_regulation": ("Banks", "Capital Markets", "Insurance", "NBFC"),
        "election": ("FMCG", "Infrastructure", "Public Sector", "Rural Demand"),
        "geopolitical": ("Aviation", "Defence", "Oil & Gas", "Shipping"),
    }.get(channel, ())


def normalized_sentiment_score(value: object) -> Decimal | None:
    """Normalize optional source tone into the storage scale when supplied."""

    if value is None:
        return None
    try:
        score = Decimal(str(value))
    except Exception:
        return None
    if score > 1 or score < -1:
        score = score / Decimal("100")
    return max(Decimal("-1"), min(Decimal("1"), score))


def normalize_search_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower()).strip()


def _has_any(text: str, keywords: tuple[str, ...]) -> bool:
    compact = re.sub(r"[^a-z0-9]+", "", text)
    return any(
        keyword in text or re.sub(r"[^a-z0-9]+", "", keyword) in compact
        for keyword in keywords
    )
