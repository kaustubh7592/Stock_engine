"""Command line interface for local engine operations."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

import typer

from india_equity_engine.core.logging import configure_logging
from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.observability.job_log import record_job_run, utc_now
from india_equity_engine.pipelines.backup_local_data import backup_local_data
from india_equity_engine.pipelines.build_event_signals import build_event_signals
from india_equity_engine.pipelines.build_governance_events import build_governance_events
from india_equity_engine.pipelines.build_stock_snapshots import build_stock_snapshots
from india_equity_engine.pipelines.compute_features import compute_features
from india_equity_engine.pipelines.download_filings import download_filings
from india_equity_engine.pipelines.explain_snapshots import explain_snapshots
from india_equity_engine.pipelines.ingest_derivatives_eod import ingest_derivatives_eod
from india_equity_engine.pipelines.ingest_disclosures import ingest_disclosures
from india_equity_engine.pipelines.ingest_filings import ingest_filings
from india_equity_engine.pipelines.ingest_insider_trades import ingest_insider_trades
from india_equity_engine.pipelines.ingest_macro_series import ingest_macro_series
from india_equity_engine.pipelines.ingest_market_eod import ingest_market_eod
from india_equity_engine.pipelines.ingest_market_flows import ingest_market_flows
from india_equity_engine.pipelines.ingest_news_items import ingest_news_items
from india_equity_engine.pipelines.parse_financial_facts import parse_financial_facts
from india_equity_engine.pipelines.parse_pledge_disclosures import parse_pledge_disclosures
from india_equity_engine.pipelines.parse_shareholding_pattern import parse_shareholding_pattern
from india_equity_engine.pipelines.rebuild_warehouse import rebuild_duckdb_views
from india_equity_engine.pipelines.refresh_universe import refresh_universe
from india_equity_engine.pipelines.run_data_quality_review import run_data_quality_review
from india_equity_engine.pipelines.run_weekly_maintenance import run_weekly_maintenance
from india_equity_engine.pipelines.score_snapshots import score_snapshots
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
    _run_and_echo(settings, lambda: refresh_universe(settings))


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
    _run_and_echo(settings, lambda: ingest_market_eod(settings, trade_date=parsed_trade_date))


@app.command("ingest-derivatives-eod")
def ingest_derivatives_eod_command(
    trade_date: Annotated[
        str | None,
        typer.Option(
            help="Trading date to ingest. Defaults to previous weekday.",
        ),
    ] = None,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSE derivatives EOD ingestion pipeline."""

    settings = Settings.load(config_dir)
    parsed_trade_date = _parse_trade_date_option(trade_date)
    _run_and_echo(settings, lambda: ingest_derivatives_eod(settings, trade_date=parsed_trade_date))


@app.command("ingest-disclosures")
def ingest_disclosures_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSE corporate announcements RSS ingestion pipeline."""

    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: ingest_disclosures(settings))


@app.command("ingest-filings")
def ingest_filings_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the filing discovery ingestion pipeline."""

    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: ingest_filings(settings))


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
    _run_and_echo(
        settings,
        lambda: ingest_insider_trades(
            settings,
            from_date=parsed_from,
            to_date=parsed_to,
            lookback_days=lookback_days,
            index=index,
            symbol=symbol,
        ),
    )


@app.command("ingest-macro-series")
def ingest_macro_series_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the RBI macro/rates ingestion pipeline."""

    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: ingest_macro_series(settings))


@app.command("ingest-market-flows")
def ingest_market_flows_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSDL FPI market-flow ingestion pipeline."""

    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: ingest_market_flows(settings))


@app.command("ingest-news-items")
def ingest_news_items_command(
    include_gdelt: Annotated[
        bool,
        typer.Option("--include-gdelt/--skip-gdelt", help="Include GDELT DOC API context."),
    ] = True,
    include_official_pages: Annotated[
        bool,
        typer.Option(
            "--include-official-pages/--skip-official-pages",
            help="Include Budget and ECI official-page discovery.",
        ),
    ] = True,
    gdelt_query: Annotated[str, typer.Option(help="GDELT DOC query string.")] = (
        "India (RBI OR rupee OR oil OR crude OR budget OR election OR tariff OR conflict)"
    ),
    gdelt_max_records: Annotated[
        int,
        typer.Option(help="Maximum GDELT article records to request."),
    ] = 50,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the Step 7 news/event ingestion pipeline."""

    if gdelt_max_records < 1:
        raise typer.BadParameter("gdelt-max-records must be at least 1.")
    settings = Settings.load(config_dir)
    _run_and_echo(
        settings,
        lambda: ingest_news_items(
            settings,
            include_gdelt=include_gdelt,
            include_official_pages=include_official_pages,
            gdelt_query=gdelt_query,
            gdelt_max_records=gdelt_max_records,
        ),
    )


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
    _run_and_echo(
        settings,
        lambda: download_filings(
            settings,
            document_types=parsed_types,
            filing_family=filing_family,
            limit=limit,
        ),
    )


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
    _run_and_echo(settings, lambda: parse_financial_facts(settings, limit=limit))


@app.command("parse-shareholding-pattern")
def parse_shareholding_pattern_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate facts to scan.")] = 5000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Parse shareholding pattern rows from financial facts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    _run_and_echo(settings, lambda: parse_shareholding_pattern(settings, limit=limit))


@app.command("parse-pledge-disclosures")
def parse_pledge_disclosures_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate facts to scan.")] = 5000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Parse promoter pledge/encumbrance rows from financial facts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    _run_and_echo(settings, lambda: parse_pledge_disclosures(settings, limit=limit))


@app.command("build-governance-events")
def build_governance_events_command(
    limit: Annotated[int, typer.Option(help="Maximum number of candidate rows to scan.")] = 10000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build normalized governance events from local disclosure tables."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    _run_and_echo(settings, lambda: build_governance_events(settings, limit=limit))


@app.command("build-event-signals")
def build_event_signals_command(
    limit: Annotated[int, typer.Option(help="Maximum number of news rows to scan.")] = 10000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build exposure-aware event signals from local news_items."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    _run_and_echo(settings, lambda: build_event_signals(settings, limit=limit))


@app.command("compute-features")
def compute_features_command(
    as_of_date: Annotated[
        str | None,
        typer.Option(
            help="Snapshot date in YYYY-MM-DD format. Defaults to latest local source date.",
        ),
    ] = None,
    families: Annotated[
        str,
        typer.Option(
            help=(
                "Comma-separated families: "
                "technical,governance,fundamental,macro,derivatives,event,peer."
            ),
        ),
    ] = "technical,governance,fundamental,macro,derivatives,event,peer",
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Compute Stage A deterministic feature snapshots."""

    settings = Settings.load(config_dir)
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    parsed_families = tuple(part.strip() for part in families.split(",") if part.strip())
    _run_and_echo(
        settings,
        lambda: compute_features(
            settings,
            as_of_date=parsed_as_of_date,
            families=parsed_families,
        ),
    )


@app.command("score-snapshots")
def score_snapshots_command(
    as_of_date: Annotated[
        str | None,
        typer.Option(
            help="Score date in YYYY-MM-DD format. Defaults to latest feature snapshot date.",
        ),
    ] = None,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build deterministic score snapshots from local feature snapshots."""

    settings = Settings.load(config_dir)
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    _run_and_echo(settings, lambda: score_snapshots(settings, as_of_date=parsed_as_of_date))


@app.command("build-stock-snapshots")
def build_stock_snapshots_command(
    as_of_date: Annotated[
        str | None,
        typer.Option(
            help="Snapshot date in YYYY-MM-DD format. Defaults to latest score snapshot date.",
        ),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum instruments to snapshot.")] = 5000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build strict stock_snapshot JSON artifacts."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    _run_and_echo(
        settings,
        lambda: build_stock_snapshots(settings, as_of_date=parsed_as_of_date, limit=limit),
    )


@app.command("explain-snapshots")
def explain_snapshots_command(
    as_of_date: Annotated[
        str | None,
        typer.Option(
            help="Explanation date in YYYY-MM-DD format. Defaults to latest stock snapshot date.",
        ),
    ] = None,
    limit: Annotated[int, typer.Option(help="Maximum snapshots to explain.")] = 100,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Generate validated local explanations from stock_snapshot JSON."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    _run_and_echo(
        settings,
        lambda: explain_snapshots(settings, as_of_date=parsed_as_of_date, limit=limit),
    )


@app.command("run-data-quality-review")
def run_data_quality_review_command(
    max_age_days: Annotated[
        int | None,
        typer.Option(
            help="Override table freshness threshold in days for tables with freshness rules.",
        ),
    ] = None,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run local data-quality and observability checks."""

    if max_age_days is not None and max_age_days < 1:
        raise typer.BadParameter("max-age-days must be at least 1.")
    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: run_data_quality_review(settings, max_age_days=max_age_days))


@app.command("rebuild-duckdb")
def rebuild_duckdb_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Rebuild DuckDB views from local Parquet datasets."""

    settings = Settings.load(config_dir)
    _run_and_echo(settings, lambda: rebuild_duckdb_views(settings))


@app.command("backup-local-data")
def backup_local_data_command(
    backup_dir: Annotated[
        str | None,
        typer.Option(help="Backup output directory. Defaults to data/backups."),
    ] = None,
    include_raw: Annotated[
        bool,
        typer.Option("--include-raw/--skip-raw", help="Include immutable raw artifacts."),
    ] = True,
    include_silver: Annotated[
        bool,
        typer.Option("--include-silver/--skip-silver", help="Include normalized silver Parquet."),
    ] = True,
    include_gold: Annotated[
        bool,
        typer.Option("--include-gold/--skip-gold", help="Include gold snapshots and outputs."),
    ] = True,
    include_logs: Annotated[
        bool,
        typer.Option("--include-logs/--skip-logs", help="Include local job logs."),
    ] = True,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--write-archive", help="Plan the backup without writing a zip."),
    ] = False,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Create a local zip backup of configs, warehouse, and selected data layers."""

    settings = Settings.load(config_dir)
    parsed_backup_dir = Path(backup_dir).resolve() if backup_dir else None
    _run_and_echo(
        settings,
        lambda: backup_local_data(
            settings,
            backup_dir=parsed_backup_dir,
            include_raw=include_raw,
            include_silver=include_silver,
            include_gold=include_gold,
            include_logs=include_logs,
            dry_run=dry_run,
        ),
    )


@app.command("run-weekly-maintenance")
def run_weekly_maintenance_command(
    include_backup: Annotated[
        bool,
        typer.Option("--include-backup/--skip-backup", help="Create a local backup archive."),
    ] = True,
    backup_dir: Annotated[
        str | None,
        typer.Option(help="Backup output directory. Defaults to data/backups."),
    ] = None,
    dry_run_backup: Annotated[
        bool,
        typer.Option("--dry-run-backup/--write-backup", help="Plan backup without writing a zip."),
    ] = False,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run weekend maintenance: rebuild views, quality review, and optional backup."""

    settings = Settings.load(config_dir)
    parsed_backup_dir = Path(backup_dir).resolve() if backup_dir else None
    _run_and_echo(
        settings,
        lambda: run_weekly_maintenance(
            settings,
            backup_dir=parsed_backup_dir,
            include_backup=include_backup,
            dry_run_backup=dry_run_backup,
        ),
    )


@app.command("run-daily")
def run_daily_command(
    include_live_ingest: Annotated[
        bool,
        typer.Option(
            "--include-live-ingest/--skip-live-ingest",
            help="Run network-dependent source ingestion before local rebuilds.",
        ),
    ] = False,
    include_filing_processing: Annotated[
        bool,
        typer.Option(
            "--include-filing-processing/--skip-filing-processing",
            help="Download and parse filing artifacts before governance/features.",
        ),
    ] = False,
    as_of_date: Annotated[
        str | None,
        typer.Option(help="Date in YYYY-MM-DD format for EOD/features/scoring/snapshots."),
    ] = None,
    snapshot_limit: Annotated[
        int,
        typer.Option(help="Maximum instruments to build as stock snapshots."),
    ] = 5000,
    explanation_limit: Annotated[
        int,
        typer.Option(help="Maximum stock snapshots to explain."),
    ] = 100,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the local daily Stage A flow as one convenience command."""

    if snapshot_limit < 1:
        raise typer.BadParameter("snapshot-limit must be at least 1.")
    if explanation_limit < 1:
        raise typer.BadParameter("explanation-limit must be at least 1.")
    settings = Settings.load(config_dir)
    command_started_at = utc_now()
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    results: list[JobRunResult] = []

    if include_live_ingest:
        results.extend(
            [
                _run_step(settings, "refresh_universe", lambda: refresh_universe(settings)),
                _run_step(
                    settings,
                    "ingest_market_eod",
                    lambda: ingest_market_eod(settings, trade_date=parsed_as_of_date),
                ),
                _run_step(
                    settings,
                    "ingest_derivatives_eod",
                    lambda: ingest_derivatives_eod(settings, trade_date=parsed_as_of_date),
                ),
                _run_step(settings, "ingest_disclosures", lambda: ingest_disclosures(settings)),
                _run_step(settings, "ingest_filings", lambda: ingest_filings(settings)),
                _run_step(
                    settings,
                    "ingest_insider_trades",
                    lambda: ingest_insider_trades(settings),
                ),
                _run_step(settings, "ingest_macro_series", lambda: ingest_macro_series(settings)),
                _run_step(settings, "ingest_market_flows", lambda: ingest_market_flows(settings)),
            ]
        )

    if include_filing_processing:
        results.extend(
            [
                _run_step(settings, "download_filings", lambda: download_filings(settings)),
                _run_step(
                    settings,
                    "parse_financial_facts",
                    lambda: parse_financial_facts(settings),
                ),
                _run_step(
                    settings,
                    "parse_shareholding_pattern",
                    lambda: parse_shareholding_pattern(settings),
                ),
                _run_step(
                    settings,
                    "parse_pledge_disclosures",
                    lambda: parse_pledge_disclosures(settings),
                ),
            ]
        )

    results.extend(
        [
            _run_step(
                settings,
                "build_governance_events",
                lambda: build_governance_events(settings),
            ),
            _run_step(
                settings,
                "compute_features",
                lambda: compute_features(settings, as_of_date=parsed_as_of_date),
            ),
            _run_step(
                settings,
                "score_snapshots",
                lambda: score_snapshots(settings, as_of_date=parsed_as_of_date),
            ),
            _run_step(
                settings,
                "build_stock_snapshots",
                lambda: build_stock_snapshots(
                    settings,
                    as_of_date=parsed_as_of_date,
                    limit=snapshot_limit,
                ),
            ),
            _run_step(
                settings,
                "explain_snapshots",
                lambda: explain_snapshots(
                    settings,
                    as_of_date=parsed_as_of_date,
                    limit=explanation_limit,
                ),
            ),
        ]
    )
    combined = _combined_result("run_daily", results)
    logged = record_job_run(
        settings,
        combined,
        started_at=command_started_at,
        finished_at=utc_now(),
    )
    typer.echo(logged.model_dump_json(indent=2))


@app.command("run-hourly-events")
def run_hourly_events_command(
    include_gdelt: Annotated[
        bool,
        typer.Option("--include-gdelt/--skip-gdelt", help="Include GDELT DOC API context."),
    ] = True,
    include_official_pages: Annotated[
        bool,
        typer.Option(
            "--include-official-pages/--skip-official-pages",
            help="Include Budget and ECI official-page discovery.",
        ),
    ] = True,
    gdelt_max_records: Annotated[
        int,
        typer.Option(help="Maximum GDELT article records to request."),
    ] = 50,
    event_signal_limit: Annotated[
        int,
        typer.Option(help="Maximum news rows to scan into event signals."),
    ] = 10000,
    refresh_snapshots: Annotated[
        bool,
        typer.Option(
            "--refresh-snapshots/--skip-snapshots",
            help="Refresh features, scores, snapshots, and explanations after events.",
        ),
    ] = False,
    snapshot_limit: Annotated[
        int,
        typer.Option(help="Maximum instruments to build when refreshing snapshots."),
    ] = 5000,
    explanation_limit: Annotated[
        int,
        typer.Option(help="Maximum explanations to build when refreshing snapshots."),
    ] = 100,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the hourly event/news refresh flow."""

    if gdelt_max_records < 1:
        raise typer.BadParameter("gdelt-max-records must be at least 1.")
    if event_signal_limit < 1:
        raise typer.BadParameter("event-signal-limit must be at least 1.")
    if snapshot_limit < 1:
        raise typer.BadParameter("snapshot-limit must be at least 1.")
    if explanation_limit < 1:
        raise typer.BadParameter("explanation-limit must be at least 1.")
    settings = Settings.load(config_dir)
    command_started_at = utc_now()
    results = [
        _run_step(
            settings,
            "ingest_news_items",
            lambda: ingest_news_items(
                settings,
                include_gdelt=include_gdelt,
                include_official_pages=include_official_pages,
                gdelt_max_records=gdelt_max_records,
            ),
        ),
        _run_step(
            settings,
            "build_event_signals",
            lambda: build_event_signals(settings, limit=event_signal_limit),
        ),
    ]

    if refresh_snapshots:
        results.extend(
            [
                _run_step(settings, "compute_features", lambda: compute_features(settings)),
                _run_step(settings, "score_snapshots", lambda: score_snapshots(settings)),
                _run_step(
                    settings,
                    "build_stock_snapshots",
                    lambda: build_stock_snapshots(settings, limit=snapshot_limit),
                ),
                _run_step(
                    settings,
                    "explain_snapshots",
                    lambda: explain_snapshots(settings, limit=explanation_limit),
                ),
            ]
        )
    combined = _combined_result("run_hourly_events", results)
    logged = record_job_run(
        settings,
        combined,
        started_at=command_started_at,
        finished_at=utc_now(),
    )
    typer.echo(logged.model_dump_json(indent=2))


def _parse_trade_date_option(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Use YYYY-MM-DD format, for example 2026-04-24.") from exc


def _run_and_echo(settings: Settings, func: Callable[[], JobRunResult]) -> None:
    started_at = utc_now()
    result = func()
    logged = record_job_run(settings, result, started_at=started_at, finished_at=utc_now())
    typer.echo(logged.model_dump_json(indent=2))


def _run_step(
    settings: Settings,
    job_name: str,
    func: Callable[[], JobRunResult],
) -> JobRunResult:
    started_at = utc_now()
    try:
        result = func()
    except Exception as exc:  # noqa: BLE001 - orchestration should report all step failures.
        result = JobRunResult(job_name=job_name, status="failed", warnings=[str(exc)])
    return record_job_run(settings, result, started_at=started_at, finished_at=utc_now())


def _combined_result(job_name: str, results: list[JobRunResult]) -> JobRunResult:
    status_counts = {result.status for result in results}
    if status_counts == {"success"}:
        status = "success"
    elif "success" in status_counts:
        status = "partial_success"
    else:
        status = "failed"
    warnings = [
        f"{result.job_name}: {warning}"
        for result in results
        for warning in result.warnings
    ]
    return JobRunResult(
        job_name=job_name,
        status=status,
        records_in=sum(result.records_in for result in results),
        records_out=sum(result.records_out for result in results),
        warnings=warnings,
        outputs={"steps": [result.model_dump(mode="json") for result in results]},
    )
