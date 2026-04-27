"""Command line interface for local engine operations."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Annotated

import typer

from india_equity_engine.core.logging import configure_logging
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.ingest_disclosures import ingest_disclosures
from india_equity_engine.pipelines.ingest_filings import ingest_filings
from india_equity_engine.pipelines.ingest_market_eod import ingest_market_eod
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


def _parse_trade_date_option(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Use YYYY-MM-DD format, for example 2026-04-24.") from exc
