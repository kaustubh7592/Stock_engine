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

## Intentional Current Limits

- Derivatives EOD ingestion is not implemented yet, so derivatives scores stay absent/neutral in Stage A outputs.
- MoSPI macro ingestion is still a future addition; the current macro slice uses RBI current rates and NSDL FPI
  flows.
- Event signals are conservative sector/theme signals. Stock-level event linking can improve once richer sector and
  exposure metadata are added.
- The LLM backend is a schema-validated local renderer. An Ollama or llama.cpp adapter can replace it later without
  changing the `stock_snapshot` contract.
- Durable observability tables such as `job_runs` and `data_quality_issues` are not yet implemented. For now, each
  command returns JSON job summaries and tests cover core parsers/pipelines.

## Operating Fit

The current Step 11 implementation matches the document's local operations model:

- Daily after-close work can be driven by `iee run-daily`.
- Hourly event refresh can be driven by `iee run-hourly-events`.
- Windows Task Scheduler can call those commands directly.
- The local API can query snapshots and trigger bounded jobs without requiring a cloud service.
- Data and generated artifacts remain outside Git, which keeps the repository small and reproducible.
