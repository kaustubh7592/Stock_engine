from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_equity_engine.connectors.xbrl.facts import (
    XBRLArtifactMetadata,
    parse_financial_facts_from_xml,
)


def test_parse_financial_facts_from_xbrl_fixture() -> None:
    metadata = XBRLArtifactMetadata(
        filing_id="filing_1",
        instrument_id="instrument_1",
        source_url="https://nsearchives.nseindia.com/corporate/xbrl/sample.xml",
        document_hash="abc",
        available_at=datetime(2026, 4, 27, 12, 30, tzinfo=timezone.utc),
        as_of_date=datetime(2026, 4, 27, 12, 30, tzinfo=timezone.utc).date(),
        source="nse",
    )

    records = parse_financial_facts_from_xml(
        Path("tests/fixtures/sample_financial_facts_xbrl.xml").read_bytes(),
        metadata,
        parser_version="test",
    )
    rows = {record.row["concept_name"]: record.row for record in records}

    assert len(records) == 3
    assert rows["revenue_from_operations"]["statement_type"] == "income_statement"
    assert rows["revenue_from_operations"]["period_type"] == "FY"
    assert rows["revenue_from_operations"]["consolidated_flag"] is True
    assert rows["revenue_from_operations"]["unit"] == "INR"
    assert rows["revenue_from_operations"]["value_num"] == Decimal("123456789")
    assert rows["profit_loss"]["value_num"] == Decimal("-1000")
    assert rows["assets"]["statement_type"] == "balance_sheet"
    assert rows["assets"]["period_type"] == "instant"
    assert rows["assets"]["consolidated_flag"] is False
