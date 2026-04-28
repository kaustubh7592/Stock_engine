from datetime import date, datetime, timezone

from india_equity_engine.connectors.events.governance import (
    GovernanceSourceRow,
    classify_governance_event,
    map_governance_events,
)


def test_governance_classifier_detects_high_risk_events() -> None:
    assert classify_governance_event("Resignation of Statutory Auditor").event_type == (
        "auditor_resignation"
    )
    assert classify_governance_event("Statement on Impact of Audit Qualification").risk_flag
    assert classify_governance_event("SEBI adjudication order and penalty").event_type == (
        "compliance_issue"
    )
    assert classify_governance_event("Preferential issue of warrants").event_type == (
        "dilution_capital_raise"
    )
    assert classify_governance_event("Unaudited financial results") is None


def test_map_governance_events_preserves_lineage() -> None:
    now = datetime(2026, 4, 28, 12, 30, tzinfo=timezone.utc)
    rows = [
        GovernanceSourceRow(
            source_table="pledge_disclosures",
            source_id="pledge_1",
            instrument_id="INS_1",
            event_date=date(2026, 3, 31),
            headline="Promoter pledge disclosure",
            event_text="pledged_pct_total_equity=12.5",
            related_filing_id="filing_1",
            source="nse",
            source_url="https://example.test/xbrl.xml",
            retrieved_at=now,
            available_at=now,
            as_of_date=now.date(),
            document_hash="abc",
            parser_version="test",
            restated_flag=False,
            created_at=now,
            updated_at=now,
        )
    ]

    records = map_governance_events(rows)

    assert len(records) == 1
    row = records[0].row
    assert row["governance_event_id"].startswith("GOV_EVENT_")
    assert row["event_type"] == "promoter_pledge"
    assert row["severity"] == "high"
    assert row["risk_flag"] is True
    assert row["related_filing_id"] == "filing_1"
    assert row["document_hash"] == "abc"


def test_insider_trade_governance_classification() -> None:
    classification = classify_governance_event(
        "Promoter Sell",
        "category=Promoter; type=Sell; value=10000000",
        source_table="insider_trades",
    )

    assert classification.event_type == "insider_activity"
    assert classification.severity == "medium"
    assert classification.risk_flag is True
