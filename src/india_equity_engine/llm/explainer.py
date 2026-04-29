"""Schema-validated explanation rendering for stock snapshots."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from india_equity_engine.core.schemas.explanation_schema import StockExplainerOutput
from india_equity_engine.core.schemas.snapshot_schema import StockSnapshot

MODEL_NAME = "local-structured-template-v1"


def explain_snapshot(snapshot: StockSnapshot) -> tuple[StockExplainerOutput, str, dict[str, Any]]:
    """Create a restrained explanation from a validated stock_snapshot only."""

    short = snapshot.scores.get("short")
    long = snapshot.scores.get("long")
    output = StockExplainerOutput(
        short_term_outlook=_outlook_text("short", short.classification if short else "abstain"),
        long_term_outlook=_outlook_text("long", long.classification if long else "abstain"),
        likes=snapshot.decision.top_positive_drivers[:5],
        dislikes=snapshot.decision.top_negative_drivers[:5],
        major_risks=snapshot.decision.key_risks[:6],
        conflicting_or_missing_evidence=_missing_evidence(snapshot),
    )
    markdown = render_markdown(snapshot, output)
    safety = {
        "uses_only_stock_snapshot": True,
        "calculates_indicators": False,
        "promises_returns": False,
        "schema_validated": True,
        "abstain": snapshot.decision.abstain,
    }
    return output, markdown, safety


def snapshot_input_hash(snapshot: StockSnapshot) -> str:
    payload = snapshot.model_dump(mode="json")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def render_markdown(snapshot: StockSnapshot, output: StockExplainerOutput) -> str:
    """Render the validated explanation as human-readable markdown."""

    lines = [
        f"# {snapshot.instrument.name}",
        "",
        f"- Instrument: `{snapshot.instrument.instrument_id}`",
        f"- As of: `{snapshot.snapshot_meta.as_of_date}`",
        f"- Deterministic stance: `{snapshot.decision.stance}`",
        f"- Abstain: `{snapshot.decision.abstain}`",
        "",
        "## Short-Term Outlook",
        output.short_term_outlook,
        "",
        "## Long-Term Outlook",
        output.long_term_outlook,
        "",
        "## Likes",
        *_bullets(output.likes),
        "",
        "## Dislikes",
        *_bullets(output.dislikes),
        "",
        "## Risks / Missing Evidence",
        *_bullets(output.major_risks + output.conflicting_or_missing_evidence),
    ]
    return "\n".join(lines).strip() + "\n"


def _outlook_text(horizon: str, classification: str) -> str:
    if classification == "bullish":
        return (
            f"The {horizon}-term deterministic score is constructive, but it should be "
            "treated as a research signal rather than a prediction."
        )
    if classification == "bearish":
        return (
            f"The {horizon}-term deterministic score is cautious, mainly reflecting "
            "weaker scored evidence or risk pressure."
        )
    if classification == "neutral":
        return (
            f"The {horizon}-term deterministic score is balanced, with no strong "
            "directional conclusion from the current evidence."
        )
    return (
        f"The {horizon}-term deterministic engine abstains because confidence or "
        "coverage is not strong enough for a directional stance."
    )


def _missing_evidence(snapshot: StockSnapshot) -> list[str]:
    notes = []
    for family, values in snapshot.features.items():
        missing = [name for name, payload in values.items() if not payload.get("coverage")]
        if missing:
            notes.append(f"{family} missing coverage: {', '.join(missing[:5])}")
    for horizon, score in snapshot.scores.items():
        if score.classification == "abstain":
            reason = "; ".join(score.abstain_reasons[:3]) or "confidence or conflict threshold"
            notes.append(
                f"{horizon} score abstains at confidence {score.confidence:.2f}: {reason}"
            )
        for conflict in score.conflicts[:3]:
            notes.append(f"{horizon} conflict: {conflict}")
        for component in score.missing_components[:3]:
            notes.append(f"{horizon} missing component: {component}")
    notes.extend(snapshot.decision.conflicts[:4])
    notes.extend(snapshot.decision.missing_evidence[:4])
    return notes[:8]


def _bullets(items: list[str]) -> list[str]:
    if not items:
        return ["- No major item surfaced in the structured snapshot."]
    return [f"- {item}" for item in items]
