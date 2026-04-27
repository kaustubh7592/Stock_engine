"""Canonical Stage A schema registry."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnDefinition:
    name: str
    dtype: str
    nullable: bool = True
    description: str = ""


@dataclass(frozen=True)
class TableSchema:
    name: str
    grain: str
    primary_key: tuple[str, ...]
    columns: tuple[ColumnDefinition, ...]
    include_lineage: bool = True

    @property
    def column_names(self) -> set[str]:
        return {column.name for column in self.columns} | (
            {column.name for column in LINEAGE_COLUMNS} if self.include_lineage else set()
        )


LINEAGE_COLUMNS = (
    ColumnDefinition("source", "TEXT", description="Logical source name."),
    ColumnDefinition("source_url", "TEXT", description="Original URL or source locator."),
    ColumnDefinition("retrieved_at", "TIMESTAMP", description="Engine retrieval timestamp."),
    ColumnDefinition("available_at", "TIMESTAMP", description="When information became knowable."),
    ColumnDefinition("as_of_date", "DATE", description="Snapshot-valid date."),
    ColumnDefinition("document_hash", "TEXT", description="Raw artifact hash."),
    ColumnDefinition("parser_version", "TEXT", description="Parser version."),
    ColumnDefinition("restated_flag", "BOOLEAN", description="Restatement marker."),
    ColumnDefinition("created_at", "TIMESTAMP", description="Warehouse insertion timestamp."),
    ColumnDefinition("updated_at", "TIMESTAMP", description="Warehouse update timestamp."),
)


CANONICAL_SCHEMAS: dict[str, TableSchema] = {
    "instruments": TableSchema(
        name="instruments",
        grain="One row per economic instrument / issuer security identity.",
        primary_key=("instrument_id",),
        include_lineage=False,
        columns=(
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("isin", "TEXT"),
            ColumnDefinition("legal_name", "TEXT"),
            ColumnDefinition("issuer_name", "TEXT"),
            ColumnDefinition("country_code", "TEXT"),
            ColumnDefinition("industry_code", "TEXT"),
            ColumnDefinition("industry_name", "TEXT"),
            ColumnDefinition("sector_name", "TEXT"),
            ColumnDefinition("fo_eligible_flag", "BOOLEAN"),
            ColumnDefinition("active_flag", "BOOLEAN"),
        ),
    ),
    "listings": TableSchema(
        name="listings",
        grain="One row per exchange-specific listing / symbol / series.",
        primary_key=("listing_id",),
        include_lineage=False,
        columns=(
            ColumnDefinition("listing_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("exchange_code", "TEXT", False),
            ColumnDefinition("symbol", "TEXT"),
            ColumnDefinition("series", "TEXT"),
            ColumnDefinition("bse_scrip_code", "TEXT"),
            ColumnDefinition("listing_date", "DATE"),
            ColumnDefinition("delisting_date", "DATE"),
            ColumnDefinition("face_value", "DECIMAL"),
            ColumnDefinition("market_lot", "INTEGER"),
        ),
    ),
    "universe_memberships": TableSchema(
        name="universe_memberships",
        grain="One row per instrument per universe membership interval.",
        primary_key=("instrument_id", "universe_code", "member_since"),
        columns=(
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("universe_code", "TEXT", False),
            ColumnDefinition("universe_name", "TEXT"),
            ColumnDefinition("member_since", "DATE", False),
            ColumnDefinition("member_until", "DATE"),
            ColumnDefinition("nse_symbol", "TEXT"),
            ColumnDefinition("isin", "TEXT"),
            ColumnDefinition("industry_name", "TEXT"),
            ColumnDefinition("active_flag", "BOOLEAN"),
        ),
    ),
    "price_daily": TableSchema(
        name="price_daily",
        grain="One row per instrument per trade_date.",
        primary_key=("instrument_id", "trade_date"),
        columns=(
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("trade_date", "DATE", False),
            ColumnDefinition("open_price", "DECIMAL"),
            ColumnDefinition("high_price", "DECIMAL"),
            ColumnDefinition("low_price", "DECIMAL"),
            ColumnDefinition("close_price", "DECIMAL"),
            ColumnDefinition("volume", "BIGINT"),
            ColumnDefinition("traded_value", "DECIMAL"),
            ColumnDefinition("deliverable_qty", "BIGINT"),
            ColumnDefinition("deliverable_pct", "DECIMAL"),
            ColumnDefinition("adj_close_price", "DECIMAL"),
            ColumnDefinition("adjustment_factor", "DECIMAL"),
        ),
    ),
    "corporate_announcements": TableSchema(
        name="corporate_announcements",
        grain="One row per exchange announcement.",
        primary_key=("announcement_id",),
        columns=(
            ColumnDefinition("announcement_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT"),
            ColumnDefinition("exchange_code", "TEXT"),
            ColumnDefinition("announced_at", "TIMESTAMP"),
            ColumnDefinition("headline", "TEXT"),
            ColumnDefinition("category", "TEXT"),
            ColumnDefinition("sub_category", "TEXT"),
            ColumnDefinition("summary_text", "TEXT"),
            ColumnDefinition("attachment_url", "TEXT"),
            ColumnDefinition("filing_family", "TEXT"),
        ),
    ),
    "corporate_actions": TableSchema(
        name="corporate_actions",
        grain="One row per action event.",
        primary_key=("corporate_action_id",),
        columns=(
            ColumnDefinition("corporate_action_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT"),
            ColumnDefinition("action_type", "TEXT"),
            ColumnDefinition("ex_date", "DATE"),
            ColumnDefinition("record_date", "DATE"),
            ColumnDefinition("book_closure_start", "DATE"),
            ColumnDefinition("book_closure_end", "DATE"),
            ColumnDefinition("ratio_or_amount", "TEXT"),
            ColumnDefinition("face_value_before", "DECIMAL"),
            ColumnDefinition("face_value_after", "DECIMAL"),
        ),
    ),
    "filings": TableSchema(
        name="filings",
        grain="One row per filing document / attachment.",
        primary_key=("filing_id",),
        columns=(
            ColumnDefinition("filing_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT"),
            ColumnDefinition("exchange_code", "TEXT"),
            ColumnDefinition("filing_family", "TEXT"),
            ColumnDefinition("filing_subtype", "TEXT"),
            ColumnDefinition("period_end", "DATE"),
            ColumnDefinition("filing_date", "DATE"),
            ColumnDefinition("document_type", "TEXT"),
            ColumnDefinition("document_url", "TEXT"),
            ColumnDefinition("xbrl_flag", "BOOLEAN"),
            ColumnDefinition("supersedes_filing_id", "TEXT"),
        ),
    ),
    "filing_artifacts": TableSchema(
        name="filing_artifacts",
        grain="One row per downloaded filing artifact.",
        primary_key=("artifact_id",),
        columns=(
            ColumnDefinition("artifact_id", "TEXT", False),
            ColumnDefinition("filing_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT"),
            ColumnDefinition("exchange_code", "TEXT"),
            ColumnDefinition("document_type", "TEXT"),
            ColumnDefinition("document_url", "TEXT"),
            ColumnDefinition("local_path", "TEXT"),
            ColumnDefinition("metadata_path", "TEXT"),
            ColumnDefinition("content_type", "TEXT"),
            ColumnDefinition("size_bytes", "BIGINT"),
            ColumnDefinition("sha256", "TEXT"),
            ColumnDefinition("download_status", "TEXT"),
            ColumnDefinition("error_text", "TEXT"),
        ),
    ),
    "financial_facts": TableSchema(
        name="financial_facts",
        grain="One row per normalized reported fact.",
        primary_key=("fact_id",),
        columns=(
            ColumnDefinition("fact_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT"),
            ColumnDefinition("filing_id", "TEXT"),
            ColumnDefinition("statement_type", "TEXT"),
            ColumnDefinition("concept_name", "TEXT"),
            ColumnDefinition("taxonomy_concept", "TEXT"),
            ColumnDefinition("period_end", "DATE"),
            ColumnDefinition("period_type", "TEXT"),
            ColumnDefinition("consolidated_flag", "BOOLEAN"),
            ColumnDefinition("unit", "TEXT"),
            ColumnDefinition("value_num", "DECIMAL"),
            ColumnDefinition("scale", "TEXT"),
        ),
    ),
    "shareholding_pattern": TableSchema(
        name="shareholding_pattern",
        grain="One row per instrument per reporting period.",
        primary_key=("instrument_id", "period_end", "consolidated_flag"),
        columns=(
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("filing_id", "TEXT"),
            ColumnDefinition("period_end", "DATE", False),
            ColumnDefinition("consolidated_flag", "BOOLEAN", False),
            ColumnDefinition("promoter_pct", "DECIMAL"),
            ColumnDefinition("public_pct", "DECIMAL"),
            ColumnDefinition("fii_pct", "DECIMAL"),
            ColumnDefinition("dii_pct", "DECIMAL"),
            ColumnDefinition("retail_pct", "DECIMAL"),
            ColumnDefinition("other_pct", "DECIMAL"),
            ColumnDefinition("share_count", "BIGINT"),
        ),
    ),
    "feature_snapshots": TableSchema(
        name="feature_snapshots",
        grain="One row per feature per stock per as_of_date.",
        primary_key=("feature_snapshot_id",),
        columns=(
            ColumnDefinition("feature_snapshot_id", "TEXT", False),
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("as_of_date", "DATE", False),
            ColumnDefinition("horizon", "TEXT"),
            ColumnDefinition("feature_family", "TEXT"),
            ColumnDefinition("feature_name", "TEXT"),
            ColumnDefinition("value_num", "DECIMAL"),
            ColumnDefinition("zscore", "DECIMAL"),
            ColumnDefinition("rank_pct", "DECIMAL"),
            ColumnDefinition("coverage_flag", "BOOLEAN"),
        ),
    ),
    "score_snapshots": TableSchema(
        name="score_snapshots",
        grain="One row per stock per as_of_date per horizon.",
        primary_key=("instrument_id", "as_of_date", "horizon"),
        columns=(
            ColumnDefinition("instrument_id", "TEXT", False),
            ColumnDefinition("as_of_date", "DATE", False),
            ColumnDefinition("horizon", "TEXT", False),
            ColumnDefinition("technical_score", "DECIMAL"),
            ColumnDefinition("fundamental_score", "DECIMAL"),
            ColumnDefinition("governance_score", "DECIMAL"),
            ColumnDefinition("macro_score", "DECIMAL"),
            ColumnDefinition("event_score", "DECIMAL"),
            ColumnDefinition("derivatives_score", "DECIMAL"),
            ColumnDefinition("composite_score", "DECIMAL"),
            ColumnDefinition("confidence_score", "DECIMAL"),
            ColumnDefinition("classification", "TEXT"),
        ),
    ),
}


def get_schema(table_name: str) -> TableSchema:
    """Return a canonical schema by name."""

    return CANONICAL_SCHEMAS[table_name]
