from datetime import date, datetime, timezone
from decimal import Decimal

from india_equity_engine.connectors.xbrl.pledge import (
    PledgeFact,
    map_pledge_disclosures,
    pledge_field_for_concept,
)


def test_pledge_field_mapping_uses_context() -> None:
    concept = "encumbered_share_under_pledged_as_percentage_of_total_number_of_shares"

    assert (
        pledge_field_for_concept(
            concept,
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
        )
        == "pledged_pct_promoter_holding"
    )
    assert (
        pledge_field_for_concept(
            concept,
            context_text="in-bse-shp:ShareholdingPatternMember",
        )
        == "pledged_pct_total_equity"
    )
    assert (
        pledge_field_for_concept(
            "number_of_shares_encumbered_under_pledged",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
        )
        == "pledged_shares"
    )
    assert (
        pledge_field_for_concept(
            "number_of_shares_under_sub_category_one",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
        )
        is None
    )
    assert (
        pledge_field_for_concept(
            "shareholding_as_apercentage_of_total_number_of_shares",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
        )
        is None
    )
    assert pledge_field_for_concept("revenue_from_operations") is None


def test_map_pledge_disclosures_groups_facts_by_filing_period() -> None:
    now = datetime(2026, 4, 27, 12, 30, tzinfo=timezone.utc)
    base = {
        "instrument_id": "instrument_1",
        "filing_id": "filing_1",
        "taxonomy_concept": None,
        "context_id": "ctx",
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
        PledgeFact(
            concept_name="number_of_shares",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
            unit="shares",
            value_num=Decimal("18469116"),
            **{key: value for key, value in base.items() if key != "unit"},
        ),
        PledgeFact(
            concept_name="number_of_shares_encumbered_under_pledged",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
            unit="shares",
            value_num=Decimal("2900000"),
            **{key: value for key, value in base.items() if key != "unit"},
        ),
        PledgeFact(
            concept_name="encumbered_share_under_pledged_as_percentage_of_total_number_of_shares",
            context_text="in-bse-shp:ShareholdingOfPromoterAndPromoterGroupMember",
            value_num=Decimal("0.1570"),
            **base,
        ),
        PledgeFact(
            concept_name="encumbered_share_under_pledged_as_percentage_of_total_number_of_shares",
            context_text="in-bse-shp:ShareholdingPatternMember",
            value_num=Decimal("0.0876"),
            **base,
        ),
    ]

    rows = map_pledge_disclosures(facts)

    assert len(rows) == 1
    row = rows[0].row
    assert row["pledge_id"].startswith("PLEDGE_")
    assert row["promoter_shares"] == 18469116
    assert row["pledged_shares"] == 2900000
    assert row["pledged_pct_promoter_holding"] == Decimal("15.7000")
    assert row["pledged_pct_total_equity"] == Decimal("8.7600")
    assert row["release_or_creation_flag"] is None
    assert row["source"] == "nse"
