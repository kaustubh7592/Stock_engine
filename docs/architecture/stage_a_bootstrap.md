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
9. NSE filing-discovery metadata pipeline for `filings`.
10. Structured filing artifact downloader for `filing_artifacts`.
11. XBRL/XML numeric fact parser for `financial_facts`.
12. Shareholding pattern mapper from parsed XBRL facts.
13. Pledge/encumbrance disclosure mapper from parsed XBRL facts.
14. NSE PIT insider-trading ingestion for `insider_trades`.

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

## Stage A Step 5

The first filing/XBRL slice builds a metadata inventory in `filings`.

- Filing/document URLs are discovered from official NSE exchange disclosure metadata.
- NSE shareholding-pattern API records are included so `shareholding_pattern` has source XBRLs to consume.
- Document type is detected from URLs and XBRL/XML documents are flagged with `xbrl_flag`.
- Filing family is classified conservatively from the exchange subject text.
- PDFs are recorded as documents, not parsed as the primary source.
- Raw feed artifacts are stored immutably and point-in-time lineage fields are preserved.
- Full XBRL parsing into `financial_facts`, `shareholding_pattern`, and governance tables is layered after this
  discovery foundation.

The second Step 5 slice downloads selected structured documents into immutable raw storage and writes a
`filing_artifacts` manifest.

- XBRL/XML/ZIP documents are prioritized by default.
- Raw artifact hashes and metadata sidecar paths are retained.
- Failed downloads are recorded with error text so live-source instability is visible rather than hidden.
- The parser for `financial_facts`, `shareholding_pattern`, and governance tables consumes these local raw
  artifacts in later Step 5 slices.

The third Step 5 slice parses successful XBRL/XML artifacts into `financial_facts`.

- Only structured XML/XBRL artifacts are parsed; PDFs remain a fallback for later work.
- Numeric facts are retained with taxonomy concept, normalized concept name, context period, unit, and
  consolidated/standalone signal when the context makes it visible.
- The first parser is deliberately conservative and source-backed. Deeper taxonomy mapping can be layered over the
  canonical fact table without replacing the raw fact inventory.

The fourth Step 5 slice maps ownership-related XBRL facts into `shareholding_pattern`.

- It consumes `financial_facts` rather than raw PDFs.
- Concept matching is conservative and produces no rows until matching shareholding concepts are present.
- Promoter, public, FII, DII, retail, other, and total share count fields are retained with the original fact
  lineage.

The fifth Step 5 slice maps promoter pledge/encumbrance facts into `pledge_disclosures`.

- It consumes `financial_facts` from structured shareholding XBRL filings.
- Context-aware mapping separates pledge percentage of promoter holding from pledge percentage of total equity.
- Pledged share counts, promoter share counts, and point-in-time lineage are preserved for later governance
  features.

The sixth Step 5 slice ingests official NSE PIT insider-trading disclosures into `insider_trades`.

- It uses the NSE corporate-filings PIT table API rather than PDF scraping.
- Person, category, transaction type, transaction date, quantity, value, derived price, and post-holding fields are
  normalized into the canonical table.
- The source XBRL URL is retained in lineage so row-level evidence remains traceable.
