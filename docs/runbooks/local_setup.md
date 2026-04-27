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

The command stores the raw discovery feed under `data/raw/`, writes `filings/current.parquet`, and refreshes the
DuckDB `filings` view. XBRL/XML documents are flagged for later structured parsing.

Download selected filing artifacts:

```powershell
iee download-filings --document-types XBRL,XML,ZIP --limit 25
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

## HTTP proxy troubleshooting

If a direct NSE archive XML URL opens in the browser but the CLI reports connection refused, inspect proxy
environment variables:

```powershell
Get-ChildItem Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY,Env:NO_PROXY -ErrorAction SilentlyContinue
```

Values such as `http://127.0.0.1:9` point Python HTTP clients at a closed local port. The local config defaults to
`http.trust_env: false` so the engine ignores those environment proxy variables. Set it to `true` only when the
environment variables intentionally point to a working proxy.
