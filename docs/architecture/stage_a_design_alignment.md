# Stage A Design Alignment

This note tracks the current implementation against the Stage A design document, especially implementation item
21: local API, CLI convenience commands, and operating runbooks.

## Implemented

- Local-first runtime: source artifacts, Parquet outputs, DuckDB views, and generated snapshot/explanation files
  remain under local `data/`.
- Official/public source posture: NSE, BSE fallback aliasing, RBI, NSDL, PIB, ECI, India Budget, and GDELT surfaces
  are represented through connectors and the source registry.
- Point-in-time storage discipline: normalized rows carry source, URL/hash, parser version, retrieval/availability
  fields where the source supports them.
- Gold snapshot layer: `feature_snapshots`, `score_snapshots`, strict `stock_snapshots` JSON, and
  `llm_explanations` exist as deterministic outputs.
- LLM safety boundary: the current explanation layer consumes validated stock snapshot JSON and does not compute
  indicators from raw data.
- CLI operations: individual job commands exist, plus `run-daily` and `run-hourly-events` for the operating model.
- Local API: FastAPI exposes health checks, stock snapshot lookup, score lookup, and synchronous local job triggers.
- Runbooks: setup, source troubleshooting, daily/hourly flows, and API usage are documented.
- Observability: orchestrated jobs are logged into `job_runs`, warnings are emitted into `data_quality_issues`, and
  `run-data-quality-review` checks duplicate keys, missing fields, missing current tables, and stale table dates.
- Maintenance: `rebuild-duckdb`, `backup-local-data`, and `run-weekly-maintenance` cover the design's local
  rebuild, backup, and weekend operating flow without requiring AWS or another cloud dependency.
- Derivatives EOD: `ingest-derivatives-eod` normalizes NSE F&O bhavcopy rows into `derivatives_eod`, and the feature
  plus scoring layers now use conservative OI/volume-derived derivatives signals.
- MoSPI macro releases: `ingest-macro-series` now includes MoSPI latest releases alongside RBI current rates.
- Feature families: `compute-features` now covers technical, governance, fundamental, macro/FPI-flow, derivatives,
  event/news carryover, and peer-relative feature families with explicit missing-coverage behavior.
- Decision layer: `score-snapshots` now emits peer score, conflicts, abstain reasons, missing components, and
  deterministic driver/risk JSON; snapshots and explanations consume those fields directly.
- Operating polish: `iee engine-status` and `/engine-status` summarize local table health and generated outputs.
- Usability layer: range backfill commands plus symbol-based stock and coverage lookup make the local engine easier
  to feed and inspect.

## Intentional Current Limits

- Event signals are conservative sector/theme signals. Stock-level event linking can improve once richer sector and
  exposure metadata are added.
- The LLM backend is a schema-validated local renderer. An Ollama or llama.cpp adapter can replace it later without
  changing the `stock_snapshot` contract.
- Observability is intentionally local and simple. Prometheus/Grafana-style metrics and automatic issue resolution
  workflows are later hardening, not Stage A blockers.

## Operating Fit

The current Step 11 implementation matches the document's local operations model:

- Daily after-close work can be driven by `iee run-daily`.
- Hourly event refresh can be driven by `iee run-hourly-events`.
- Windows Task Scheduler can call those commands directly.
- The local API can query snapshots and trigger bounded jobs without requiring a cloud service.
- Weekly data-quality review can be driven by `iee run-data-quality-review`.
- Weekly rebuild/backup maintenance can be driven by `iee run-weekly-maintenance`.
- Data and generated artifacts remain outside Git, which keeps the repository small and reproducible.
