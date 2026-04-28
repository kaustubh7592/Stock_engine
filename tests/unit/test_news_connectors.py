from datetime import datetime, timezone
from pathlib import Path

from india_equity_engine.connectors.news.gdelt import GDELTDocConnector
from india_equity_engine.connectors.news.official_pages import OfficialPageNewsConnector
from india_equity_engine.connectors.news.rss import RBI_RSS_FEEDS, OfficialRSSNewsConnector
from india_equity_engine.core.schemas.contracts import RawArtifact, SourceConfig


def test_official_rss_news_connector_classifies_rbi_rates() -> None:
    connector = OfficialRSSNewsConnector(
        source=_source("S15", "rbi", "https://www.rbi.org.in/Scripts/rss.aspx"),
        feeds=RBI_RSS_FEEDS[:1],
        user_agent="test",
    )
    source_object = connector.discover()[0]
    artifact = RawArtifact(
        source_code="S15",
        source_family="rbi",
        logical_name=source_object.logical_name,
        source_url=str(source_object.url),
        content=Path("tests/fixtures/rbi_news_rss_sample.xml").read_bytes(),
        extension="xml",
        retrieved_at=datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
        content_type="text/xml",
        metadata=source_object.metadata,
    )

    records = connector.normalize(connector.parse(artifact), artifact)

    assert len(records) == 2
    assert records[0].table_name == "news_items"
    assert records[0].row["event_type"] == "macro_policy"
    assert records[0].row["source_class"] == "regulator"


def test_official_page_connector_extracts_budget_links() -> None:
    connector = OfficialPageNewsConnector(
        source=_source("S19", "budget", "https://www.indiabudget.gov.in/"),
        source_name="India Budget",
        source_class="government",
        user_agent="test",
    )
    source_object = connector.discover()[0]
    artifact = RawArtifact(
        source_code="S19",
        source_family="budget",
        logical_name=source_object.logical_name,
        source_url=str(source_object.url),
        content=Path("tests/fixtures/budget_page_sample.html").read_bytes(),
        extension="html",
        retrieved_at=datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
        content_type="text/html",
        metadata=source_object.metadata,
    )

    rows = connector.parse(artifact)
    records = connector.normalize(rows, artifact)

    assert len(records) == 2
    assert records[0].row["event_type"] == "budget_policy"
    assert records[0].row["url"].startswith("https://www.indiabudget.gov.in/")


def test_gdelt_connector_normalizes_article_context() -> None:
    connector = GDELTDocConnector(
        source=_source("S21", "gdelt", "https://www.gdeltproject.org/data.html"),
        user_agent="test",
        max_records=5,
    )
    source_object = connector.discover()[0]
    artifact = RawArtifact(
        source_code="S21",
        source_family="gdelt",
        logical_name=source_object.logical_name,
        source_url=str(source_object.url),
        content=Path("tests/fixtures/gdelt_doc_sample.json").read_bytes(),
        extension="json",
        retrieved_at=datetime(2026, 4, 28, 12, 0, tzinfo=timezone.utc),
        content_type="application/json",
        metadata=source_object.metadata,
    )

    records = connector.normalize(connector.parse(artifact), artifact)

    assert len(records) == 1
    assert records[0].row["source_class"] == "global_news"
    assert records[0].row["event_type"] in {"commodity_energy", "geopolitical"}
    assert records[0].row["sentiment_score"] is not None


def _source(code: str, family: str, url: str) -> SourceConfig:
    return SourceConfig(
        code=code,
        family=family,
        name=family,
        purpose="test",
        url=url,
        fetch_mode="test",
        cost="free_public",
        cadence="hourly",
        enabled=True,
    )
