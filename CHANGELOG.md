# Changelog

---

## [0.2.0] — 2026-04-01

### Added

**Data**
- FF5 + momentum factors ingested (26,070 rows, 1926–Jan 2026, Kenneth French library)
- CBOE vol series added: VIX3M, GVZ (Gold VIX), OVX (Oil VIX) via FRED
- dim_symbol expanded from 40 → 531 symbols (all equity symbols from canonical data)
- dim_calendar corrected: 8,086 rows/2035 → 6,260 rows/2028 (covers LEAPS, not unnecessary future)
- Option EOD ingested: 207M rows across 7 symbols (2008–2026) from ThetaData vrp_validate + chain parquets
- VRP gap-fill completed: 513 symbols updated to Mar 30 2026 (2.24M total rows)

**Models**
- `stg_ff_factors` — new staging model for FF factors
- `int_macro_series` updated — now merges FRED + 7 FF factors + 3 CBOE vol indices (21 series total)
- `mart_regime_panel` expanded — 10 → 33 columns including VIX3M, vix_term_structure, gold_vix, oil_vix, all FF factors
- `mart_optimization_inputs` updated — adds FF factors + excess_return_1d (return minus RF)

**Scripts**
- `ingest_ff_factors.py` — new, handles Kenneth French CSV format
- `ingest_theta_option_eod.py` — rewrote: memory-safe (one file at a time), handles all ThetaData column variants
- `ingest_theta_vrp_features.py` — rewrote: memory-safe, symbol-by-symbol with gc.collect()
- `ingest_existing_equity.py` — rewrote: memory-safe
- `ingest_theta_contracts.py` — fixed column mapping for chain parquet format (right→option_type, expiration→expiry)
- `ingest_stooq_daily_bars.py` — updated: exits cleanly when Stooq blocks requests, clear error message

**Documentation**
- `STATUS.md` — new: current data state, test results, what works/doesn't/in-progress
- `ROADMAP.md` — new: full prioritised plan for data additions, free sources, integration patterns
- `docs/data_sources.md` — new: comprehensive source reference (current, planned, evaluated)
- `README.md` — rewritten: status table, source table, links to all docs
- `dbt/models/schema.yml` — added column-level not_null tests for all 15 models
- `CONTRIBUTING.md`, `docs/architecture.md`, `docs/data_dictionary.md`, `docs/ingestion_guide.md`, `docs/query_guide.md` — all present from v0.1.0

**Operations**
- Watcher script (`/tmp/market_lake_post_gapfill.sh`) — auto-ingest + dbt rebuild on gap-fill completion
- Manifest dedup — 15 duplicate rows cleaned to 8 canonical records

### Fixed
- `parquet.py` — removed `use_legacy_dataset=True` (removed in PyArrow 23)
- `mart_backtest_equity_panel.sql` — refactored nested window functions into 3 CTEs
- `int_underlying_bars_daily.sql` — changed to read all sources (not just source='yahoo')
- `stg_theta_option_eod.sql` — pass through expiry/strike/option_type columns
- `int_option_eod.sql` — nullable-safe join; unmatched rows kept not silently dropped
- `stg_theta_contracts.sql` / `stg_theta_option_eod.sql` — placeholder parquets prevent glob errors

### Tests
- 36 / 36 pytest unit tests passing
- 28 / 28 dbt schema tests passing
- 15 / 15 dbt models building clean

---

## [0.1.0] — 2026-03-31

### Added — Core foundation

- Python package (`src/market_lake/`): settings, io, ids, validation
- 7 ingest scripts: equity, VRP features, options EOD, Yahoo, Stooq, FRED, FF factors
- 3 build scripts: bootstrap_duckdb, dim_symbol, dim_calendar
- 14 dbt models: 6 staging, 3 intermediate, 5 marts
- DuckDB init SQL: extensions, schemas, views
- 4 docs: architecture, data dictionary, ingestion guide, query guide
- dbt schema.yml with column-level tests
- 13 pytest tests (now 36 after v0.2.0 additions)

### Initial data load
- 2,579,090 equity bar rows (531 symbols, 2005–2026, alphaquant cache)
- 1,122,018 VRP feature rows (513 symbols, 2017–2026, ThetaData vrp_clean)
- 85,951 macro rows (11 FRED series)
- 8,086 calendar rows (2005–2035 weekdays)
- 40 symbols in dim_symbol
