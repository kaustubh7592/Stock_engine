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

The refresh currently builds:

- `instruments`
- `listings`
- `universe_memberships`
- `price_daily`
- `corporate_announcements`
- `corporate_actions`
- `filings`

`price_daily` uses NSE's current CM UDiFF bhavcopy as the primary price source and enriches matched rows with
NSE security-wise delivery quantity and delivery percentage when available.

`filings` is metadata-first: it discovers exchange filing/document URLs and flags XBRL/XML documents for later
structured parsing.

By default, local data is written under `data/`, which is intentionally ignored by Git.
