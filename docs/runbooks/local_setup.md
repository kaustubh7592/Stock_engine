# Local Setup

Install the project:

```powershell
python -m pip install -e .[dev]
```

Check config:

```powershell
iee config-check
```

Run tests:

```powershell
python -m pytest
```

Refresh the first security master:

```powershell
iee refresh-universe
```

The refresh downloads NSE `EQUITY_L.csv`, Nifty 500 constituents, and BSE aliases from `Scrip_BSE.zip` /
`SCRIP.zip` when the BSE download host is reachable. It stores raw artifacts immutably, normalizes
instruments/listings/universe memberships, and writes current Parquet outputs under `data/silver/`.

If BSE returns a TLS handshake failure from a corporate or local network, the job reports `partial_success` and
still writes the verified NSE and Nifty 500 outputs.

Ingest NSE EOD prices:

```powershell
iee ingest-market-eod
```

The command defaults to the previous weekday. For a specific trading date:

```powershell
iee ingest-market-eod --trade-date 2026-04-24
```

The pipeline stores the raw NSE price zip and delivery CSV under `data/raw/`, writes `price_daily` Parquet under
`data/silver/`, and refreshes the DuckDB `price_daily` view.

Ingest NSE corporate announcements and corporate actions:

```powershell
iee ingest-disclosures
```

The command stores the raw NSE RSS XML files under `data/raw/`, writes
`corporate_announcements/current.parquet` and `corporate_actions/current.parquet`, and refreshes the related
DuckDB views.

Discover NSE filing metadata:

```powershell
iee ingest-filings
```

The command stores the raw discovery feeds under `data/raw/`, writes `filings/current.parquet`, and refreshes the
DuckDB `filings` view. It includes NSE announcements RSS plus official NSE shareholding-pattern filing APIs.
XBRL/XML documents are flagged for later structured parsing.

Ingest NSE PIT insider trades:

```powershell
iee ingest-insider-trades --from-date 2026-04-24 --to-date 2026-04-28
```

The command reads the official NSE PIT corporate-filings table, stores the raw JSON response, writes
`insider_trades/current.parquet`, and refreshes the DuckDB view. Omit `--from-date` and `--to-date` to use the
default recent lookback window, or pass `--symbol 360ONE` for a focused run.

Download selected filing artifacts:

```powershell
iee download-filings --document-types XBRL,XML,ZIP --limit 25
```

For shareholding filings specifically:

```powershell
iee download-filings --filing-family shareholding --document-types XBRL --limit 25
```

The command reads the local `filings` view, downloads prioritized structured documents, stores immutable raw
artifacts and metadata JSON files under `data/raw/`, writes `filing_artifacts/current.parquet`, and refreshes the
DuckDB `filing_artifacts` view. Use a small limit for first live tests because exchange-hosted files can be slow or
temporarily unavailable.

Parse downloaded financial facts:

```powershell
iee parse-financial-facts --limit 25
```

The command reads successful local `filing_artifacts`, parses XML/XBRL numeric facts, writes
`financial_facts/current.parquet`, and refreshes the DuckDB `financial_facts` view. If the current network produced
only failed download attempts, run `download-filings` again from a machine that can reach NSE archives before
parsing.

Parse shareholding pattern rows:

```powershell
iee parse-shareholding-pattern --limit 5000
```

The command reads `financial_facts`, maps ownership-related XBRL concepts into `shareholding_pattern`, writes
`shareholding_pattern/current.parquet`, and refreshes the DuckDB view. It may return no rows if the downloaded
filings are board-meeting or dividend XBRLs rather than shareholding filings.

Parse pledge/encumbrance disclosure rows:

```powershell
iee parse-pledge-disclosures --limit 5000
```

The command reads `financial_facts`, maps promoter pledge and encumbrance XBRL concepts into
`pledge_disclosures`, writes `pledge_disclosures/current.parquet`, and refreshes the DuckDB view. It uses XBRL
context labels so promoter-holding percentages and total-equity percentages are not mixed.

Build governance events:

```powershell
iee build-governance-events --limit 10000
```

The command reads local `corporate_announcements`, `filings`, `pledge_disclosures`, and `insider_trades`, applies
conservative rules for auditor, board, compliance, dilution, pledge, and insider-activity events, writes
`governance_events/current.parquet`, and refreshes the DuckDB view.

Ingest RBI macro/rates:

```powershell
iee ingest-macro-series
```

The command reads RBI's official current-rates page, stores the raw HTML, writes `macro_series/current.parquet`,
and refreshes the DuckDB view. The first slice keeps only single numeric values, such as policy rates, reserve
ratios, and exchange-rate observations; range values are skipped instead of being silently averaged.

Ingest NSDL FPI market flows:

```powershell
iee ingest-market-flows
```

The command reads NSDL's daily FPI/FII investment trends page, stores the raw HTML, writes
`market_flows/current.parquet`, and refreshes the DuckDB view. It separates equity, debt, hybrid, and route slices
by encoding the investment route in `flow_type`.

Ingest event/news items and build event signals:

```powershell
iee ingest-news-items
iee build-event-signals --limit 10000
```

For a low-network test, skip GDELT and official HTML pages while keeping RBI/PIB RSS:

```powershell
iee ingest-news-items --skip-gdelt --skip-official-pages
```

Build features, deterministic scores, stock snapshots, and explanations:

```powershell
iee compute-features
iee score-snapshots
iee build-stock-snapshots --limit 5000
iee explain-snapshots --limit 100
```

The feature step writes `feature_snapshots/current.parquet`; scoring writes
`score_snapshots/current.parquet`; stock snapshot building writes strict JSON artifacts under
`data/gold/stock_snapshots/json/`; explanations write schema-validated local explanation outputs under
`data/gold/llm_outputs/`.

## Convenience operating flows

Run the local after-close rebuild from existing warehouse data:

```powershell
iee run-daily --skip-live-ingest --snapshot-limit 5000 --explanation-limit 100
```

Run the full daily flow only from a machine/network that can reach the official source hosts:

```powershell
iee run-daily --include-live-ingest --snapshot-limit 5000 --explanation-limit 100
```

Add filing artifact download and XBRL/shareholding/pledge parsing when you want the filing-processing slice in the
same daily command:

```powershell
iee run-daily --include-live-ingest --include-filing-processing
```

Refresh event/news data during the day:

```powershell
iee run-hourly-events --include-gdelt --skip-snapshots
```

If you want hourly event updates to flow immediately into refreshed features, scores, snapshots, and explanations:

```powershell
iee run-hourly-events --include-gdelt --refresh-snapshots --snapshot-limit 5000 --explanation-limit 100
```

Each convenience command prints a single JSON job result with nested step results. A failed network source is
reported in that JSON instead of hiding which step failed.

Run the data-quality and observability review:

```powershell
iee run-data-quality-review
```

This writes `job_runs/current.parquet` and `data_quality_issues/current.parquet` under `data/gold/`, refreshes the
matching DuckDB views where the local DuckDB lock allows it, and records issues such as missing current tables,
duplicate primary keys, missing required fields, stale dates, and job warnings. For append-style tables such as
dated `price_daily` Parquet files, the review checks the latest available Parquet file when no `current.parquet`
exists.

For Windows Task Scheduler, call the thin wrappers:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-daily.ps1
powershell -ExecutionPolicy Bypass -File scripts/run-hourly-events.ps1 -SkipGdelt
powershell -ExecutionPolicy Bypass -File scripts/run-data-quality-review.ps1
```

## Local API

Start the local API:

```powershell
uvicorn india_equity_engine.app.api.main:app --reload
```

Then query:

```powershell
curl.exe http://127.0.0.1:8000/health
curl.exe "http://127.0.0.1:8000/snapshots?limit=10"
curl.exe "http://127.0.0.1:8000/snapshots/INS_1"
curl.exe "http://127.0.0.1:8000/scores/INS_1"
curl.exe "http://127.0.0.1:8000/job-runs"
curl.exe "http://127.0.0.1:8000/quality-issues"
```

The API reads current Parquet outputs first and falls back to DuckDB views when needed. It can also trigger a small
set of local jobs synchronously, for example:

```powershell
curl.exe -X POST "http://127.0.0.1:8000/jobs/build-stock-snapshots/run?limit=25"
curl.exe -X POST "http://127.0.0.1:8000/jobs/explain-snapshots/run?limit=5"
curl.exe -X POST "http://127.0.0.1:8000/jobs/run-data-quality-review/run"
```

## HTTP proxy troubleshooting

If a direct NSE archive XML URL opens in the browser but the CLI reports connection refused, inspect proxy
environment variables:

```powershell
Get-ChildItem Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY,Env:NO_PROXY -ErrorAction SilentlyContinue
```

Values such as `http://127.0.0.1:9` point Python HTTP clients at a closed local port. The local config defaults to
`http.trust_env: false` so the engine ignores those environment proxy variables. Set it to `true` only when the
environment variables intentionally point to a working proxy.
