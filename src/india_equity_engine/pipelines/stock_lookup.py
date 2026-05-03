"""Symbol-oriented stock lookup and coverage diagnostics."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.settings import Settings


def stock_lookup(
    settings: Settings,
    symbol_or_id: str,
    *,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """Return the latest snapshot, scores, and coverage summary for a symbol or instrument id."""

    resolved = resolve_instrument(settings, symbol_or_id)
    if resolved is None:
        return {"found": False, "query": symbol_or_id}
    snapshot = _snapshot(settings, resolved["instrument_id"], as_of_date)
    scores = _scores(settings, resolved["instrument_id"], as_of_date)
    coverage = coverage_diagnostics(settings, symbol_or_id, as_of_date=as_of_date)
    return {
        "found": True,
        "query": symbol_or_id,
        "instrument": resolved,
        "summary": stock_summary(resolved, snapshot, scores, coverage),
        "snapshot": snapshot,
        "scores": scores,
        "coverage": coverage,
    }


def stock_summary(
    instrument: dict[str, Any],
    snapshot: dict[str, Any] | None,
    scores: list[dict[str, Any]],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    """Build a concise plain-English summary for a stock lookup payload."""

    name = _instrument_name(instrument, snapshot)
    as_of_date = (
        (snapshot or {}).get("snapshot_meta", {}).get("as_of_date")
        or coverage.get("as_of_date")
    )
    if snapshot is None:
        return {
            "headline": f"No stock snapshot is available for {name}.",
            "as_of_date": as_of_date,
            "plain_english": [
                "The stock was found, but the engine has not built a score snapshot for this date.",
                (
                    "Run compute-features, score-snapshots, and build-stock-snapshots "
                    "for the target date."
                ),
            ],
            "available_data": _raw_available_components(coverage),
            "missing_data": _raw_missing_components(coverage),
        }

    decision = snapshot.get("decision") or {}
    snapshot_scores = snapshot.get("scores") or {}
    primary_horizon = str(decision.get("primary_horizon") or "short")
    primary_score = snapshot_scores.get(primary_horizon) or {}
    stance = str(decision.get("stance") or "unknown")
    classification = str(primary_score.get("classification") or "unknown")
    composite = _round_or_none(primary_score.get("composite"))
    confidence = _round_or_none(primary_score.get("confidence"))
    components = _component_scores(primary_score)
    positives = [str(item) for item in decision.get("top_positive_drivers") or []]
    negatives = [str(item) for item in decision.get("top_negative_drivers") or []]
    missing = _missing_components(coverage)
    raw_available = _raw_available_components(coverage)
    horizon_views = _horizon_views(snapshot_scores)
    scored_available = _scored_components(components)
    plain_english = [
        _plain_view_sentence(name, primary_horizon, classification, stance, composite, confidence),
        _plain_evidence_sentence(scored_available, raw_available, missing),
    ]
    if positives:
        plain_english.append(f"What supports the view: {', '.join(positives[:4])}.")
    if negatives:
        plain_english.append(f"What works against it: {', '.join(negatives[:4])}.")
    if missing:
        plain_english.append(
            "This is still incomplete for serious long-term analysis because "
            f"{', '.join(missing[:6])} data is missing."
        )

    return {
        "headline": _headline(name, primary_horizon, classification, stance),
        "as_of_date": as_of_date,
        "primary_horizon": primary_horizon,
        "stance": stance,
        "classification": classification,
        "composite_score": composite,
        "confidence_score": confidence,
        "horizon_views": horizon_views,
        "component_scores": components,
        "available_data": scored_available,
        "raw_data_available": raw_available,
        "missing_data": missing,
        "key_positive_drivers": positives[:5],
        "key_negative_drivers": negatives[:5],
        "plain_english": plain_english,
    }


def coverage_diagnostics(
    settings: Settings,
    symbol_or_id: str,
    *,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """Explain data coverage by score component for one stock."""

    resolved = resolve_instrument(settings, symbol_or_id)
    if resolved is None:
        return {"found": False, "query": symbol_or_id}
    instrument_id = resolved["instrument_id"]
    resolved_as_of = as_of_date or _latest_as_of(settings)
    feature_rows = _feature_rows(settings, instrument_id, resolved_as_of)
    score_rows = _scores(settings, instrument_id, resolved_as_of)
    families = _family_summary(feature_rows)
    components = {
        "technical": _technical_coverage(settings, instrument_id),
        "fundamental": _fundamental_coverage(settings, instrument_id),
        "governance": _governance_coverage(settings, instrument_id),
        "macro": _macro_coverage(settings),
        "event": _event_coverage(settings, resolved),
        "derivatives": _derivatives_coverage(settings, instrument_id),
        "peer": _peer_coverage(families),
    }
    score_missing = sorted(
        {
            component
            for row in score_rows
            for component in _json_list(row.get("missing_components_json"))
        }
    )
    return {
        "found": True,
        "query": symbol_or_id,
        "instrument": resolved,
        "as_of_date": resolved_as_of.isoformat() if resolved_as_of else None,
        "feature_families": families,
        "score_missing_components": score_missing,
        "components": components,
    }


def resolve_instrument(settings: Settings, symbol_or_id: str) -> dict[str, Any] | None:
    """Resolve an NSE/BSE symbol or instrument id to local instrument metadata."""

    query = symbol_or_id.strip()
    if not query:
        return None
    rows = _query_rows(
        settings,
        f"""
        with listing_matches as (
            select instrument_id, exchange_code, symbol, bse_scrip_code
            from {_source(settings, "listings", "silver")}
            where upper(symbol) = upper(?)
               or upper(bse_scrip_code) = upper(?)
               or upper(instrument_id) = upper(?)
        ),
        resolved_ids as (
            select instrument_id from listing_matches
            union
            select instrument_id
            from {_source(settings, "instruments", "silver")}
            where upper(instrument_id) = upper(?)
               or upper(isin) = upper(?)
        )
        select
            i.instrument_id,
            i.isin,
            i.legal_name,
            i.issuer_name,
            i.sector_name,
            i.industry_name,
            lm.exchange_code,
            lm.symbol,
            lm.bse_scrip_code
        from resolved_ids r
        left join {_source(settings, "instruments", "silver")} i using (instrument_id)
        left join listing_matches lm using (instrument_id)
        limit 1
        """,
        [query, query, query, query, query],
        missing_ok=True,
    )
    if not rows:
        return None
    row = rows[0]
    return {key: _json_value(value) for key, value in row.items()}


def _snapshot(
    settings: Settings,
    instrument_id: str,
    as_of_date: date | None,
) -> dict[str, Any] | None:
    rows = _query_rows(
        settings,
        f"""
        select snapshot_json_path
        from {_source(settings, "stock_snapshots", "gold")}
        where instrument_id = ?
          and (? is null or cast(as_of_date as varchar) = ?)
        order by as_of_date desc
        limit 1
        """,
        [
            instrument_id,
            as_of_date.isoformat() if as_of_date else None,
            as_of_date.isoformat() if as_of_date else None,
        ],
        missing_ok=True,
    )
    if not rows:
        return None
    path = Path(str(rows[0].get("snapshot_json_path") or ""))
    if not path.exists():
        return {"snapshot_json_path": str(path), "missing_file": True}
    return json.loads(path.read_text(encoding="utf-8"))


def _scores(
    settings: Settings,
    instrument_id: str,
    as_of_date: date | None,
) -> list[dict[str, Any]]:
    rows = _query_rows(
        settings,
        f"""
        select *
        from {_source(settings, "score_snapshots", "gold")}
        where instrument_id = ?
          and (? is null or cast(as_of_date as varchar) = ?)
        order by as_of_date desc, horizon
        """,
        [
            instrument_id,
            as_of_date.isoformat() if as_of_date else None,
            as_of_date.isoformat() if as_of_date else None,
        ],
        missing_ok=True,
    )
    return _json_ready(rows)


def _feature_rows(
    settings: Settings,
    instrument_id: str,
    as_of_date: date | None,
) -> list[dict[str, Any]]:
    return _query_rows(
        settings,
        f"""
        select feature_family, feature_name, coverage_flag, value_num
        from {_source(settings, "feature_snapshots", "gold")}
        where instrument_id = ?
          and (? is null or cast(as_of_date as varchar) = ?)
        """,
        [
            instrument_id,
            as_of_date.isoformat() if as_of_date else None,
            as_of_date.isoformat() if as_of_date else None,
        ],
        missing_ok=True,
    )


def _technical_coverage(settings: Settings, instrument_id: str) -> dict[str, Any]:
    return _count_latest(
        settings,
        "price_daily",
        "silver",
        "trade_date",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=21,
    )


def _derivatives_coverage(settings: Settings, instrument_id: str) -> dict[str, Any]:
    return _count_latest(
        settings,
        "derivatives_eod",
        "silver",
        "trade_date",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=1,
    )


def _fundamental_coverage(settings: Settings, instrument_id: str) -> dict[str, Any]:
    facts = _count_latest(
        settings,
        "financial_facts",
        "silver",
        "period_end",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=4,
    )
    shareholding = _count_latest(
        settings,
        "shareholding_pattern",
        "silver",
        "period_end",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=1,
    )
    return {
        "ok": bool(facts["ok"] or shareholding["ok"]),
        "financial_facts": facts,
        "shareholding_pattern": shareholding,
    }


def _governance_coverage(settings: Settings, instrument_id: str) -> dict[str, Any]:
    events = _count_latest(
        settings,
        "governance_events",
        "silver",
        "event_date",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=1,
    )
    pledge = _count_latest(
        settings,
        "pledge_disclosures",
        "silver",
        "period_end",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=1,
    )
    insider = _count_latest(
        settings,
        "insider_trades",
        "silver",
        "transaction_date",
        "instrument_id = ?",
        [instrument_id],
        enough_threshold=1,
    )
    return {
        "ok": bool(events["ok"] or pledge["ok"] or insider["ok"]),
        "governance_events": events,
        "pledge_disclosures": pledge,
        "insider_trades": insider,
    }


def _macro_coverage(settings: Settings) -> dict[str, Any]:
    macro = _count_latest(
        settings,
        "macro_series",
        "silver",
        "observation_date",
        "1=1",
        [],
        enough_threshold=1,
    )
    flows = _count_latest(
        settings,
        "market_flows",
        "silver",
        "trade_date",
        "1=1",
        [],
        enough_threshold=1,
    )
    return {"ok": bool(macro["ok"] or flows["ok"]), "macro_series": macro, "market_flows": flows}


def _event_coverage(settings: Settings, instrument: dict[str, Any]) -> dict[str, Any]:
    instrument_id = instrument.get("instrument_id")
    clause = "instrument_id = ?"
    params: list[object] = [instrument_id]
    event_scope_terms = _event_scope_terms(instrument)
    if event_scope_terms:
        placeholders = ", ".join("?" for _ in event_scope_terms)
        clause = f"(instrument_id = ? or upper(sector_name) in ({placeholders}))"
        params.extend(term.upper() for term in event_scope_terms)
    return _count_latest(
        settings,
        "event_signals",
        "silver",
        "event_date",
        clause,
        params,
        enough_threshold=1,
    )


def _event_scope_terms(instrument: dict[str, Any]) -> list[str]:
    terms = []
    for value in (instrument.get("sector_name"), instrument.get("industry_name")):
        text = _text_or_none(value)
        if text and text not in terms:
            terms.append(text)
    profile_text = " ".join(
        value
        for value in (
            _text_or_none(instrument.get("sector_name")),
            _text_or_none(instrument.get("industry_name")),
            _text_or_none(instrument.get("legal_name")),
            _text_or_none(instrument.get("issuer_name")),
            _text_or_none(instrument.get("symbol")),
        )
        if value
    ).lower()
    aliases = []
    if "bank" in profile_text or "financial service" in profile_text:
        aliases.append("Banks")
    if "nbfc" in profile_text or "financial service" in profile_text:
        aliases.append("NBFC")
    if "insurance" in profile_text:
        aliases.append("Insurance")
    if "capital market" in profile_text or "financial service" in profile_text:
        aliases.append("Capital Markets")
    for alias in aliases:
        if alias not in terms:
            terms.append(alias)
    return terms


def _peer_coverage(families: dict[str, Any]) -> dict[str, Any]:
    peer = families.get("peer", {})
    return {
        "ok": bool(peer.get("covered_features", 0)),
        "covered_features": peer.get("covered_features", 0),
        "total_features": peer.get("total_features", 0),
    }


def _instrument_name(instrument: dict[str, Any], snapshot: dict[str, Any] | None) -> str:
    snapshot_instrument = (snapshot or {}).get("instrument") or {}
    return str(
        snapshot_instrument.get("name")
        or instrument.get("legal_name")
        or instrument.get("issuer_name")
        or instrument.get("symbol")
        or instrument.get("instrument_id")
        or "stock"
    )


def _headline(name: str, horizon: str, classification: str, stance: str) -> str:
    if classification == "abstain":
        return f"{name}: the engine abstains for the {horizon}-term view."
    return f"{name}: {horizon}-term view is {classification} ({stance})."


def _plain_view_sentence(
    name: str,
    horizon: str,
    classification: str,
    stance: str,
    composite: float | None,
    confidence: float | None,
) -> str:
    score_text = "unknown score" if composite is None else f"score {composite}"
    confidence_text = "unknown confidence" if confidence is None else f"confidence {confidence}"
    if classification == "abstain":
        return (
            f"For {name}, the engine abstains on the {horizon}-term view "
            f"with {score_text} and {confidence_text}."
        )
    return (
        f"For {name}, the engine's {horizon}-term view is {classification} "
        f"({stance}) with {score_text} and {confidence_text}."
    )


def _plain_evidence_sentence(
    scored_available: list[str],
    raw_available: list[str],
    missing: list[str],
) -> str:
    if scored_available and missing:
        raw_note = ""
        raw_only = [component for component in raw_available if component not in scored_available]
        if raw_only:
            raw_note = f" Raw {', '.join(raw_only)} data exists but is not yet scoring this view."
        return (
            f"The scored view is mainly based on {', '.join(scored_available)} data; "
            f"{', '.join(missing)} scored evidence is still missing.{raw_note}"
        )
    if scored_available:
        return f"The scored view is based on {', '.join(scored_available)} data."
    if raw_available:
        return (
            f"Raw {', '.join(raw_available)} data exists, but the engine has very limited "
            "scored evidence for this stock/date."
        )
    return "The engine has very limited supporting data for this stock/date."


def _horizon_views(scores: dict[str, Any]) -> dict[str, dict[str, float | str | None]]:
    output = {}
    for horizon in ("short", "medium", "long"):
        score = scores.get(horizon) or {}
        output[horizon] = {
            "classification": score.get("classification"),
            "composite_score": _round_or_none(score.get("composite")),
            "confidence_score": _round_or_none(score.get("confidence")),
        }
    return output


def _component_scores(score: dict[str, Any]) -> dict[str, float | None]:
    return {
        component: _round_or_none(score.get(source_key))
        for component, source_key in (
            ("technical", "technical"),
            ("fundamental", "fundamental"),
            ("governance", "governance"),
            ("macro", "macro"),
            ("event", "events"),
            ("derivatives", "derivatives"),
            ("peer", "peer"),
        )
    }


def _scored_components(components: dict[str, float | None]) -> list[str]:
    return [component for component, value in components.items() if value is not None]


def _raw_available_components(coverage: dict[str, Any]) -> list[str]:
    components = coverage.get("components") or {}
    return [name for name, payload in components.items() if (payload or {}).get("ok") is True]


def _raw_missing_components(coverage: dict[str, Any]) -> list[str]:
    components = coverage.get("components") or {}
    return [name for name, payload in components.items() if (payload or {}).get("ok") is not True]


def _missing_components(coverage: dict[str, Any]) -> list[str]:
    score_missing = coverage.get("score_missing_components") or []
    if score_missing:
        return [str(component) for component in score_missing]
    return _raw_missing_components(coverage)


def _round_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except (TypeError, ValueError):
        return None


def _count_latest(
    settings: Settings,
    table_name: str,
    layer: str,
    date_column: str,
    where_clause: str,
    params: list[object],
    *,
    enough_threshold: int,
) -> dict[str, Any]:
    rows = _query_rows(
        settings,
        f"""
        select count(*) as rows, max({date_column}) as latest
        from {_source(settings, table_name, layer)}
        where {where_clause}
        """,
        params,
        missing_ok=True,
    )
    count = int(rows[0]["rows"]) if rows else 0
    latest = rows[0]["latest"] if rows else None
    return {
        "ok": count >= enough_threshold,
        "rows": count,
        "latest": _json_value(latest),
        "required_rows": enough_threshold,
    }


def _family_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, dict[str, int]] = {}
    for row in rows:
        family = str(row.get("feature_family") or "")
        if not family:
            continue
        grouped.setdefault(family, {"total_features": 0, "covered_features": 0})
        grouped[family]["total_features"] += 1
        if row.get("coverage_flag") is True and row.get("value_num") is not None:
            grouped[family]["covered_features"] += 1
    return grouped


def _latest_as_of(settings: Settings) -> date | None:
    rows = _query_rows(
        settings,
        f"select max(as_of_date) as as_of_date from {_source(settings, 'score_snapshots', 'gold')}",
        [],
        missing_ok=True,
    )
    value = rows[0].get("as_of_date") if rows else None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError:
        return None


def _source(settings: Settings, table_name: str, layer: str) -> str:
    root = settings.gold_root if layer == "gold" else settings.silver_root
    current = root / table_name / "current.parquet"
    if current.exists():
        return _read_parquet_expr(current)
    table_dir = current.parent
    if table_dir.exists() and any(table_dir.glob("*.parquet")):
        return _read_parquet_expr(table_dir / "*.parquet")
    return table_name


def _read_parquet_expr(path: Path) -> str:
    escaped = str(path).replace("'", "''")
    return f"read_parquet('{escaped}')"


def _query_rows(
    settings: Settings,
    query: str,
    params: list[object],
    *,
    missing_ok: bool = False,
) -> list[dict[str, Any]]:
    try:
        if settings.duckdb_path.exists():
            con = duckdb.connect(str(settings.duckdb_path), read_only=True)
        else:
            con = duckdb.connect()
        with con:
            result = con.execute(query, params)
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error as exc:
        if missing_ok:
            return []
        raise exc
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _json_ready(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{key: _json_value(value) for key, value in row.items()} for row in rows]


def _json_value(value: object) -> object:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "as_tuple"):
        return float(value)
    return value


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_list(value: object) -> list[str]:
    if value is None:
        return []
    try:
        payload = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [str(item) for item in payload]
