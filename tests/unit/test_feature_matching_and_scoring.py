from datetime import date
from decimal import Decimal

from india_equity_engine.features.events import compute_event_features
from india_equity_engine.features.peer import compute_peer_features
from india_equity_engine.scoring.rules import component_scores_for_instrument


def test_peer_features_fall_back_to_industry_when_sector_is_missing() -> None:
    instrument_rows = [
        {"instrument_id": "INS_1", "sector_name": None, "industry_name": "Financial Services"},
        {"instrument_id": "INS_2", "sector_name": None, "industry_name": "Financial Services"},
        {"instrument_id": "INS_3", "sector_name": None, "industry_name": "Financial Services"},
    ]
    price_rows = []
    for instrument_id, base in (("INS_1", 100), ("INS_2", 120), ("INS_3", 140)):
        for offset in range(6):
            price_rows.append(
                {
                    "instrument_id": instrument_id,
                    "trade_date": date(2026, 4, 24 + offset),
                    "close_price": base + offset,
                    "traded_value": 100000 + base,
                    "deliverable_pct": 50 + offset,
                    "available_at": "2026-04-30 16:00:00",
                }
            )

    records = compute_peer_features(price_rows, instrument_rows, date(2026, 4, 29))

    rows = [record.row for record in records if record.row["instrument_id"] == "INS_1"]
    return_5d = next(row for row in rows if row["feature_name"] == "peer_return_5d_rank_pct")
    assert return_5d["coverage_flag"] is True
    assert return_5d["value_num"] is not None


def test_event_features_match_financial_services_to_bank_events() -> None:
    event_rows = [
        {
            "event_signal_id": "evt_1",
            "instrument_id": None,
            "sector_name": "Banks",
            "event_date": date(2026, 4, 28),
            "horizon": "medium",
            "impact_direction": "mixed",
            "impact_score": -0.20,
            "confidence": 0.70,
            "available_at": "2026-04-28 12:00:00",
        }
    ]
    instrument_rows = [
        {
            "instrument_id": "INS_BANK",
            "sector_name": None,
            "industry_name": "Financial Services",
        }
    ]

    records = compute_event_features(event_rows, instrument_rows, date(2026, 4, 30))

    rows = [record.row for record in records if record.row["instrument_id"] == "INS_BANK"]
    assert {row["feature_name"] for row in rows} == {
        "event_signal_count_30d",
        "event_net_impact_30d",
        "event_negative_signal_count_30d",
        "event_positive_signal_count_30d",
    }


def test_derivatives_open_interest_and_volume_create_neutral_component_score() -> None:
    scores = component_scores_for_instrument(
        [
            {
                "feature_family": "derivatives",
                "feature_name": "derivatives_open_interest_latest",
                "value_num": 100000,
                "coverage_flag": True,
            },
            {
                "feature_family": "derivatives",
                "feature_name": "derivatives_contract_volume_latest",
                "value_num": 5000,
                "coverage_flag": True,
            },
        ]
    )

    assert scores["derivatives"].score is not None
    assert scores["derivatives"].score == Decimal("0.5")
