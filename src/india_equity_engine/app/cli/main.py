"""Command line interface for local engine operations."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime
from typing import Annotated

import typer

from india_equity_engine.core.logging import configure_logging
from india_equity_engine.core.schemas.contracts import JobRunResult
from india_equity_engine.core.settings import Settings
from india_equity_engine.pipelines.build_event_signals import build_event_signals
from india_equity_engine.pipelines.build_governance_events import build_governance_events
from india_equity_engine.pipelines.build_stock_snapshots import build_stock_snapshots
from india_equity_engine.pipelines.compute_features import compute_features
from india_equity_engine.pipelines.download_filings import download_filings
from india_equity_engine.pipelines.explain_snapshots import explain_snapshots
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
from india_equity_engine.pipelines.refresh_universe import refresh_universe
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


@app.command("ingest-macro-series")
def ingest_macro_series_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the RBI macro/rates ingestion pipeline."""

    settings = Settings.load(config_dir)
    result = ingest_macro_series(settings)
    typer.echo(result.model_dump_json(indent=2))


@app.command("ingest-market-flows")
def ingest_market_flows_command(
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Run the NSDL FPI market-flow ingestion pipeline."""

    settings = Settings.load(config_dir)
    result = ingest_market_flows(settings)
    typer.echo(result.model_dump_json(indent=2))


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
    result = ingest_news_items(
        settings,
        include_gdelt=include_gdelt,
        include_official_pages=include_official_pages,
        gdelt_query=gdelt_query,
        gdelt_max_records=gdelt_max_records,
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


@app.command("build-event-signals")
def build_event_signals_command(
    limit: Annotated[int, typer.Option(help="Maximum number of news rows to scan.")] = 10000,
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Build exposure-aware event signals from local news_items."""

    settings = Settings.load(config_dir)
    if limit < 1:
        raise typer.BadParameter("Limit must be at least 1.")
    result = build_event_signals(settings, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


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
            help="Comma-separated families: technical,governance,fundamental.",
        ),
    ] = "technical,governance,fundamental",
    config_dir: Annotated[str, typer.Option(help="Configuration directory.")] = "configs",
) -> None:
    """Compute Stage A deterministic feature snapshots."""

    settings = Settings.load(config_dir)
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    parsed_families = tuple(part.strip() for part in families.split(",") if part.strip())
    result = compute_features(
        settings,
        as_of_date=parsed_as_of_date,
        families=parsed_families,
    )
    typer.echo(result.model_dump_json(indent=2))


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
    result = score_snapshots(settings, as_of_date=parsed_as_of_date)
    typer.echo(result.model_dump_json(indent=2))


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
    result = build_stock_snapshots(settings, as_of_date=parsed_as_of_date, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


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
    result = explain_snapshots(settings, as_of_date=parsed_as_of_date, limit=limit)
    typer.echo(result.model_dump_json(indent=2))


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
    parsed_as_of_date = _parse_trade_date_option(as_of_date)
    results: list[JobRunResult] = []

    if include_live_ingest:
        results.extend(
            [
                _run_step("refresh_universe", lambda: refresh_universe(settings)),
                _run_step(
                    "ingest_market_eod",
                    lambda: ingest_market_eod(settings, trade_date=parsed_as_of_date),
                ),
                _run_step("ingest_disclosures", lambda: ingest_disclosures(settings)),
                _run_step("ingest_filings", lambda: ingest_filings(settings)),
                _run_step("ingest_insider_trades", lambda: ingest_insider_trades(settings)),
                _run_step("ingest_macro_series", lambda: ingest_macro_series(settings)),
                _run_step("ingest_market_flows", lambda: ingest_market_flows(settings)),
            ]
        )

    if include_filing_processing:
        results.extend(
            [
                _run_step("download_filings", lambda: download_filings(settings)),
                _run_step("parse_financial_facts", lambda: parse_financial_facts(settings)),
                _run_step(
                    "parse_shareholding_pattern",
                    lambda: parse_shareholding_pattern(settings),
                ),
                _run_step(
                    "parse_pledge_disclosures",
                    lambda: parse_pledge_disclosures(settings),
                ),
            ]
        )

    results.extend(
        [
            _run_step("build_governance_events", lambda: build_governance_events(settings)),
            _run_step(
                "compute_features",
                lambda: compute_features(settings, as_of_date=parsed_as_of_date),
            ),
            _run_step(
                "score_snapshots",
                lambda: score_snapshots(settings, as_of_date=parsed_as_of_date),
            ),
            _run_step(
                "build_stock_snapshots",
                lambda: build_stock_snapshots(
                    settings,
                    as_of_date=parsed_as_of_date,
                    limit=snapshot_limit,
                ),
            ),
            _run_step(
                "explain_snapshots",
                lambda: explain_snapshots(
                    settings,
                    as_of_date=parsed_as_of_date,
                    limit=explanation_limit,
                ),
            ),
        ]
    )
    typer.echo(_combined_result("run_daily", results).model_dump_json(indent=2))


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
    results = [
        _run_step(
            "ingest_news_items",
            lambda: ingest_news_items(
                settings,
                include_gdelt=include_gdelt,
                include_official_pages=include_official_pages,
                gdelt_max_records=gdelt_max_records,
            ),
        ),
        _run_step(
            "build_event_signals",
            lambda: build_event_signals(settings, limit=event_signal_limit),
        ),
    ]

    if refresh_snapshots:
        results.extend(
            [
                _run_step("compute_features", lambda: compute_features(settings)),
                _run_step("score_snapshots", lambda: score_snapshots(settings)),
                _run_step(
                    "build_stock_snapshots",
                    lambda: build_stock_snapshots(settings, limit=snapshot_limit),
                ),
                _run_step(
                    "explain_snapshots",
                    lambda: explain_snapshots(settings, limit=explanation_limit),
                ),
            ]
        )
    typer.echo(_combined_result("run_hourly_events", results).model_dump_json(indent=2))


def _parse_trade_date_option(value: str | None) -> date | None:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter("Use YYYY-MM-DD format, for example 2026-04-24.") from exc


def _run_step(job_name: str, func: Callable[[], JobRunResult]) -> JobRunResult:
    try:
        return func()
    except Exception as exc:  # noqa: BLE001 - orchestration should report all step failures.
        return JobRunResult(job_name=job_name, status="failed", warnings=[str(exc)])


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
