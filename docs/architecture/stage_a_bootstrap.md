# Stage A Bootstrap

This repo starts with the first implementation step from the design document:

1. Configuration system.
2. Source registry.
3. Immutable raw artifact store.
4. Parquet and DuckDB storage abstractions.
5. Canonical schemas with point-in-time lineage fields.
6. A security-master pipeline for NSE `EQUITY_L.csv`, Nifty 500 membership, and BSE aliases.
7. An NSE cash-market EOD pipeline for `price_daily`.
8. NSE announcements and corporate-actions RSS pipelines for `corporate_announcements` and
   `corporate_actions`.

The code uses a namespaced package, `india_equity_engine`, while retaining the blueprint's module boundaries
inside the package. That avoids collisions with generic package names such as `core` or `models`.

Data under `data/` is local runtime state and should not be committed.

## Stage A Step 2

The security-master pipeline now follows the document's second implementation step:

- NSE `EQUITY_L.csv` is the primary local security master.
- Nifty 500 constituents seed the first live research universe.
- BSE `Scrip_BSE.zip` / `SCRIP.zip` data is normalized into BSE listing aliases when the BSE download host is
  reachable.

The canonical outputs are written to `current.parquet` files so DuckDB views remain idempotent across repeated
runs. Raw source artifacts remain immutable and timestamped.

## Stage A Step 3

The first EOD market slice ingests NSE cash-market bhavcopy data into `price_daily`.

- The connector tries NSE's current `CM-UDiFF Common Bhavcopy Final (zip)` filename first.
- It falls back to the older `CM - Bhavcopy (PR.zip)` filename.
- The parser supports both current UDiFF fields and older bhavcopy headers.
- It enriches matched rows with NSE `sec_bhavdata_full_DDMMYYYY.csv` delivery quantity and delivery percentage.
- Basic OHLCV quality checks run before Parquet writes.
- One `trade_date_YYYYMMDD.parquet` file is written per trading date, making repeat runs idempotent.

## Stage A Step 4

The announcement/disclosure foundation ingests NSE corporate announcements RSS into
`corporate_announcements` and NSE corporate-actions RSS into `corporate_actions`.

- RSS XML is stored immutably under `data/raw/`.
- Announcement and corporate-action IDs are stable and deterministic.
- NSE symbols/company names are resolved through the local listings and instruments tables when possible.
- `announced_at`, `available_at`, `retrieved_at`, source URL, document hash, and parser version are preserved.
- Corporate actions capture action type, ex-date, record date, book-closure dates, ratio/amount text, and face value.
- Attachments are referenced by URL; full attachment download/enrichment is a later slice.
