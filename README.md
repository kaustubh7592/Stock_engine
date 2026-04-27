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

Download structured filing artifacts for later XBRL parsing:

```powershell
iee download-filings --document-types XBRL,XML,ZIP --limit 25
```

Parse downloaded XBRL/XML artifacts into financial facts:

```powershell
iee parse-financial-facts --limit 25
```

Build shareholding pattern rows from parsed facts:

```powershell
iee parse-shareholding-pattern --limit 5000
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

`price_daily` uses NSE's current CM UDiFF bhavcopy as the primary price source and enriches matched rows with
NSE security-wise delivery quantity and delivery percentage when available.

`filings` is metadata-first: it discovers exchange filing/document URLs and flags XBRL/XML documents for later
structured parsing.

`filing_artifacts` records local raw downloads, hashes, metadata sidecars, and failed download attempts without
parsing the financial facts yet.

`financial_facts` is XBRL/XML-first and extracts numeric reported facts with context period, unit, consolidation
flag, source URL, hash, and parser lineage.

`shareholding_pattern` maps ownership-related XBRL facts into promoter, public, FII, DII, retail, other, and total
share-count fields when matching shareholding concepts are available.

By default, local data is written under `data/`, which is intentionally ignored by Git.

If direct NSE archive links open in your browser but `iee` downloads fail with connection refused, check whether
`HTTP_PROXY`, `HTTPS_PROXY`, or `ALL_PROXY` are set to a dead local proxy. The default config sets
`http.trust_env: false` so Python HTTP clients ignore those environment proxy variables.
