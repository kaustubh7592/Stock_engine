from datetime import date, datetime, timezone
from decimal import Decimal

from india_equity_engine.connectors.xbrl.shareholding import (
    ShareholdingFact,
    map_shareholding_pattern,
    shareholding_field_for_concept,
)


def test_shareholding_field_mapping_is_conservative() -> None:
    assert (
        shareholding_field_for_concept("promoter_and_promoter_group_shareholding_percentage")
        == "promoter_pct"
    )
    assert shareholding_field_for_concept("public_shareholding_percentage") == "public_pct"
    assert (
        shareholding_field_for_concept("foreign_institutional_investors_shareholding")
        == "fii_pct"
    )
    assert (
        shareholding_field_for_concept("domestic_institutional_investors_shareholding")
        == "dii_pct"
    )
    assert shareholding_field_for_concept("revenue_from_operations") is None


def test_map_shareholding_pattern_groups_facts_by_period() -> None:
    now = datetime(2026, 4, 27, 12, 30, tzinfo=timezone.utc)
    base = {
        "instrument_id": "instrument_1",
        "filing_id": "filing_1",
        "taxonomy_concept": None,
        "period_end": date(2026, 3, 31),
        "consolidated_flag": False,
        "unit": "pure",
        "source": "nse",
        "source_url": "https://example.test/xbrl.xml",
        "retrieved_at": now,
        "available_at": now,
        "as_of_date": now.date(),
        "document_hash": "abc",
        "parser_version": "test",
        "restated_flag": False,
        "created_at": now,
        "updated_at": now,
    }
    facts = [
        ShareholdingFact(
            concept_name="promoter_and_promoter_group_shareholding_percentage",
            value_num=Decimal("51.2"),
            **base,
        ),
        ShareholdingFact(
            concept_name="public_shareholding_percentage",
            value_num=Decimal("48.8"),
            **base,
        ),
        ShareholdingFact(
            concept_name="total_number_of_shares_held",
            value_num=Decimal("1000000"),
            **base,
        ),
    ]

    rows = map_shareholding_pattern(facts)

    assert len(rows) == 1
    row = rows[0].row
    assert row["promoter_pct"] == Decimal("51.2")
    assert row["public_pct"] == Decimal("48.8")
    assert row["share_count"] == 1000000
    assert row["source"] == "nse"
