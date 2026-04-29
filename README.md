# India Equity Research Engine

Local-first Stage A implementation for an India-only equity research engine.

The project follows the supplied Stage A design document:

- Indian listed equities only.
- ISIN is the canonical security identifier.
- Raw source artifacts are immutable and lineage is preserved.
- Normalized data is written as local Parquet and queried with DuckDB.
- Deterministic scoring comes before any local LLM explanation.

## Current Build Slice

This repository has been bootstrapped with the Stage A foundation:

- Configuration and source registry under `configs/`.
- Canonical schema definitions under `src/india_equity_engine/models/`.
- Connector contracts under `src/india_equity_engine/connectors/`.
- Raw artifact storage under `src/india_equity_engine/storage/`.
- Parquet and DuckDB storage interfaces.
- A first NSE security-master connector for `EQUITY_L.csv`.
- Nifty 500 universe membership ingestion.
- BSE scrip-master ZIP alias ingestion when `www.bseindia.com` is reachable from the local network.
- A Typer CLI entrypoint named `iee`.

## Local Setup

```powershell
python -m pip install -e .[dev]
```

Validate the configuration:

```powershell
iee config-check
```

List configured sources:

```powershell
iee sources
```

Run the first security-master pipeline:

```powershell
iee refresh-universe
```

Run the first NSE EOD market pipeline:

```powershell
iee ingest-market-eod
```

Or specify a trading date:

```powershell
iee ingest-market-eod --trade-date 2026-04-24
```

Run the NSE corporate announcements pipeline:

```powershell
iee ingest-disclosures
```

Run the filing-discovery pipeline:

```powershell
iee ingest-filings
```

Run the NSE PIT insider-trading pipeline:

```powershell
iee ingest-insider-trades --from-date 2026-04-24 --to-date 2026-04-28
```

Run the Step 6 macro and flow pipelines:

```powershell
iee ingest-macro-series
iee ingest-market-flows
```

Run the Step 7 event/news layer:

```powershell
iee ingest-news-items
iee build-event-signals --limit 10000
```

Run the Step 8 feature engine:

```powershell
iee compute-features
```

Run the Step 9 deterministic scoring engine:

```powershell
iee score-snapshots
```

Run the Step 10 stock snapshot and explanation layer:

```powershell
iee build-stock-snapshots
iee explain-snapshots --limit 100
```

Run the Step 11 convenience flows:

```powershell
iee run-daily --skip-live-ingest --snapshot-limit 5000 --explanation-limit 100
iee run-hourly-events --include-gdelt --skip-snapshots
```

Windows Task Scheduler can call the thin wrappers in `scripts/run-daily.ps1` and
`scripts/run-hourly-events.ps1`.

Use `--include-live-ingest` on `run-daily` when the machine can reach NSE/BSE/RBI/NSDL sources. The default
`--skip-live-ingest` mode rebuilds governance events, features, scores, stock snapshots, and explanations from
the local warehouse, which is safer for repeat testing on a blocked network.

Run the local API:

```powershell
uvicorn india_equity_engine.app.api.main:app --reload
```

Useful local endpoints:

- `GET /health`
- `GET /snapshots?as_of_date=2026-04-28&limit=50`
- `GET /snapshots/{instrument_id}`
- `GET /scores/{instrument_id}?as_of_date=2026-04-28`
- `POST /jobs/{job_name}/run`, where `job_name` is one of `ingest-news-items`, `build-event-signals`,
  `compute-features`, `score-snapshots`, `build-stock-snapshots`, or `explain-snapshots`

Download structured filing artifacts for later XBRL parsing:

```powershell
iee download-filings --document-types XBRL,XML,ZIP --limit 25
```

For a focused ownership run:

```powershell
iee download-filings --filing-family shareholding --document-types XBRL --limit 25
```

Parse downloaded XBRL/XML artifacts into financial facts:

```powershell
iee parse-financial-facts --limit 25
```

Build shareholding pattern rows from parsed facts:

```powershell
iee parse-shareholding-pattern --limit 5000
```

Build pledge/encumbrance disclosure rows from parsed facts:

```powershell
iee parse-pledge-disclosures --limit 5000
```

Build normalized governance events from local disclosure tables:

```powershell
iee build-governance-events --limit 10000
```

The refresh currently builds:

- `instruments`
- `listings`
- `universe_memberships`
- `price_daily`
- `corporate_announcements`
- `corporate_actions`
- `filings`
- `filing_artifacts`
- `financial_facts`
- `shareholding_pattern`
- `pledge_disclosures`
- `insider_trades`
- `governance_events`
- `macro_series`
- `market_flows`
- `news_items`
- `event_signals`
- `feature_snapshots`
- `score_snapshots`
- `stock_snapshots`
- `llm_explanations`

`price_daily` uses NSE's current CM UDiFF bhavcopy as the primary price source and enriches matched rows with
NSE security-wise delivery quantity and delivery percentage when available.

`filings` is metadata-first: it discovers exchange filing/document URLs and flags XBRL/XML documents for later
structured parsing.

`filing_artifacts` records local raw downloads, hashes, metadata sidecars, and failed download attempts without
parsing the financial facts yet.

The filing discovery step includes NSE corporate announcements RSS plus the official NSE shareholding-pattern
filing API, so `filings` can contain shareholding XBRL links before artifact download.

`financial_facts` is XBRL/XML-first and extracts numeric reported facts with context period, unit, consolidation
flag, source URL, hash, and parser lineage.

`shareholding_pattern` maps ownership-related XBRL facts into promoter, public, FII, DII, retail, other, and total
share-count fields when matching shareholding concepts are available.

`pledge_disclosures` maps promoter pledge/encumbrance XBRL facts into pledged share counts and pledge percentages,
using the XBRL context to separate promoter-holding percentages from total-equity percentages.

`insider_trades` ingests the official NSE PIT table into person/category, transaction type, quantity, value, derived
price, and post-holding fields with row-level XBRL/source lineage.

`governance_events` applies conservative rules over announcements, filings, pledge disclosures, and PIT rows to
surface auditor, board, compliance, dilution, pledge, and insider-activity events for later governance features.

`macro_series` currently ingests RBI's official current-rates panel into policy rates, reserve ratios, exchange
rates, and market-rate series.

`market_flows` currently ingests NSDL's official daily FPI/FII investment trends into equity, debt, hybrid, and route
slices.

`news_items` ingests official RBI RSS, PIB RSS, India Budget, ECI, and GDELT context surfaces into classified
event/news rows with source class, geography, novelty, optional source tone, and extracted exposure themes.

`event_signals` maps classified news into conservative sector-level exposure signals for oil/fuel, FX, rates,
government capex/budget, rural/agriculture, commodities, regulation, election, and geopolitical channels. Stock-level
instrument mapping can be layered once sector and exposure metadata are richer.

`feature_snapshots` is written to the gold layer by `compute-features`. The first feature families are technical,
governance, and fundamental/shareholding features, with explicit `coverage_flag` values instead of fake zeros when
history or structured facts are missing.

`score_snapshots` is written to the gold layer by `score-snapshots`. Scores are deterministic: component scores stay
separate, composite scores use `configs/weights/score_weights.yaml`, and confidence/abstain behavior uses
`configs/weights/confidence_rules.yaml`.

`stock_snapshots` indexes strict JSON artifacts produced by `build-stock-snapshots`. Each full snapshot is validated
with the local `StockSnapshot` schema before being stored under `data/gold/stock_snapshots/json/`.

`llm_explanations` is produced by `explain-snapshots` from validated stock snapshot JSON only. The current local
backend is a schema-validated structured renderer, so it does not calculate indicators, read raw filings, or invent
decisions. A local model adapter can replace the renderer later without changing the snapshot contract.

By default, local data is written under `data/`, which is intentionally ignored by Git.

If direct NSE archive links open in your browser but `iee` downloads fail with connection refused, check whether
`HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY` are set to a dead local proxy. The default config sets
`http.trust_env: false` so Python HTTP clients ignore those environment proxy variables.
