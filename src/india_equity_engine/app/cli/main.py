"""Command line interface for local engine operations."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Annotated

import typer

from india_equity_engine.core.logging import configure_logging
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_governance_events import build_governance_events
from india_equity_engine.pipelines.download_filings import download_filings
from india_equity_engine.pipelines.ingest_disclosures import ingest_disclosures
from india_equity_engine.pipelines.ingest_filings import ingest_filings
from india_equity_engine.pipelines.ingest_insider_trades import ingest_insider_trades
from india_equity_engine.pipelines.ingest_market_eod import ingest_market_eod
from india_equity_engine.pipelines.parse_financial_facts import parse_financial_facts
from india_equity_engine.pipelines.parse_pledge_disclosures import parse_pledge_disclosures
from india_equity_engine.pipelines.parse_shareholding_pattern import parse_shareholding_pattern
from india_equity_engine.pipelines.refresh_universe import refresh_universe
from india_equity_engine.storage.registry import SourceRegistry

app = typer.Typer(help="India Equity Research Engine")


@app.callback()
def main(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    configure_logging(config_dir)


@app.command("config-check")
def config_check(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Validate core config and source registry."""

    settings = Settings.load(config_dir)
    registry = SourceRegistry(settings.config_dir)
    settings.ensure_runtime_dirs()
    typer.echo(
        json.dumps(
            {
                "status": "ok",
                "config_dir": str(settings.config_dir),
                "data_root": str(settings.data_root),
                "sources": len(registry.sources),
            },
            indent=2,
        )
    )


@app.command("sources")
def sources(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """List enabled configured sources."""

    settings = Settings.load(config_dir)
    registry = SourceRegistry(settings.config_dir)
    rows = [
        {
            "code": source.code,
            "family": source.family,
            "name": source.name,
            "cadence": source.cadence,
            "enabled": source.enabled,
        }
        for source in registry.enabled()
    ]
    typer.echo(json.dumps(rows, indent=2))


@app.command("refresh-universe")
def refresh_universe_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the first security-master pipeline."""

    settings = Settings.load(config_dir)
    result = refresh_universe(settings)
    typer.echo(result.model_dump_json(indent=2))


@app.command("ingest-market-eod")
def ingest_market_eod_command(
    trade_date: Annotated[
        str | None,
        typer.Option(
            help="Trading date to ingest. Defaults to previous weekday.",
        ),
    ] = None,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSE cash-market EOD ingestion pipeline."""

    settings = Settings.load(config_dir)
    parsed_trade_date = _parse_trade_date_option(trade_date)
    result = ingest_market_eod(settings, trade_date=parsed_trade_date)
    typer.echo(result.model_dump_json(indent=2))


@app.command("ingest-disclosures")
def ingest_disclosures_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSE corporate announcements RSS ingestion pipeline."""

    settings = Settings.load(config_dir)
    result = ingest_disclosures(settings)
    typer.echo(result.model_dump_json(indent=2))


@app.command("ingest-filings")
def ingest_filings_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the filing discovery ingestion pipeline."""

    settings = Settings.load(config_dir)
    result = ingest_filings(settings)
    typer.echo(result.model_dump_json(indent=2))


@app.command("ingest-insider-trades")
def ingest_insider_trades_command(
    from_date: Annotated[
        str | None,
        typer.Option(help="Start date in YYYY-MM-DD format. Defaults to lookback window."),
    ] = None,
    to_date: Annotated[
        str | None,
        typer.Option(help="End date in YYYY-MM-DD format. Defaults to today."),
    ] = None,
    lookback_days: Annotated[
        int,
        typer.Option(help="Default lookback when --from-date is omitted."),
    ] = 30,
    index: Annotated[
        str,
        typer.Option(help="NSE PIT index, usually equities or sme."),
    ] = "equities",
    symbol: Annotated[str | None, typer.Option(help="Optional NSE symbol filter.")] = None,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSE PIT insider-trading ingestion pipeline."""

    settings = Settings.load(config_dir)
    if lookback_days < 1:
        raise typer.BadParameter("Lookback days must be at least 1.")
    parsed_from = _parse_trade_date_option(from_date)
    parsed_to = _parse_trade_date_option(to_date)
    if parsed_from and parsed_to and parsed_from > parsed_to:
        raise typer.BadParameter("from-date must be on or before to-date.")
    result = ingest_insider_trades(
        settings,
        from_date=parsed_from,
        to_date=parsed_to,
        lookback_days=lookback_days,
        index=index,
        symbol=symbol,
    )
    typer.echo(result.model_dump_json(indent=2))


@app.command("download-filings")
def download_filings_command(
    document_types: Annotated[
        str,
        typer.Option(
            help="Comma-separated document types to download, in priority order.",
        ),
    ] = "XBRL,XML,ZIP",
    filing_family: Annotated[
        str | None,
        typer.Option(help="Optional filing family filter, for example shareholding."),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum number of filings to download.")] = 25,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Download selected filing artifacts, prioritizing structured filings."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    parsed_types = tuple(part.strip().upper() for part in document_types.split(",") if part.strip())
    result = download_filings(
        settings,
        document_types=parsed_types,
        filing_family=filing_family,
        limit=limit,
    )
    typer.echo(result.model_dump_json(indent=2))


@app.command("parse-financial-facts")
def parse_financial_facts_command(
    limit: Annotated[
        int,
        typer.Option(help="Maximum number of successful artifacts to parse."),
    ] = 25,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Parse downloaded XBRL/XML filing artifacts into financial facts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    result = parse_financial_facts(settings, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


@app.command("parse-shareholding-pattern")
def parse_shareholding_pattern_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate facts to scan.")] = 5000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Parse shareholding pattern rows from financial facts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    result = parse_shareholding_pattern(settings, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


@app.command("parse-pledge-disclosures")
def parse_pledge_disclosures_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate facts to scan.")] = 5000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Parse promoter pledge/encumbrance rows from financial facts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    result = parse_pledge_disclosures(settings, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


@app.command("build-governance-events")
def build_governance_events_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate rows to scan.")] = 10000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build normalized governance events from local disclosure tables."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    result = build_governance_events(settings, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


def _parse_trade_date_option(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Use YYYY-MM-DD format, for example 2026-04-24.") from exc
