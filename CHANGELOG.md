# Changelog

All notable changes to market-lake are documented here.
Follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) format.

---

## [0.1.0] — 2026-03-31

### Added — Core foundation

**Python package (`src/market_lake/`)**
- `settings.py` — `Settings.load()` reads `MARKET_LAKE_ROOT` from `.env`, exposes all canonical paths
- `io/duckdb_conn.py` — `connect()`, `open_db()` context manager, `run_sql_file()`
- `io/parquet.py` — `write_parquet()` with content-hash filenames and PyArrow Hive partitioning
- `io/manifests.py` — `ManifestRecord`, `write_manifest()`, `load_manifests()`, `build_batch_id()`
- `ids/contract_id.py` — `make_contract_id()` producing `UNDERLYING|EXPIRY|STRIKE|TYPE` format
- `ids/symbol_map.py` — `stable_symbol_id()` (deterministic SHA-256 int), `build_dim_symbol()`
- `validation/prices.py` — OHLCV integrity checks (high/low, NaN, negative volume)
- `validation/options.py` — contract and EOD validation (duplicates, bid > ask, IV sanity)
- `validation/macros.py` — macro series duplicate and null checks

**DuckDB bootstrap**
- `duckdb/init/001_extensions.sql` — parquet, httpfs, json
- `duckdb/init/002_schemas.sql` — raw, canonical, features, marts, metadata
- `duckdb/init/003_views.sql` — template with `{root}` substitution for absolute paths
- `scripts/build/bootstrap_duckdb.py` — renders view template, gracefully skips missing data

**Dimension builders**
- `scripts/build/build_dim_symbol.py` — builds `dim_symbol` from `config/symbols.yaml`
- `scripts/build/build_dim_calendar.py` — generates weekday calendar 2005–2035

**Ingest scripts**
- `scripts/ingest/ingest_existing_equity.py` — absorbs `*_daily_*.parquet` from any cache directory
- `scripts/ingest/ingest_theta_vrp_features.py` — absorbs ThetaData `*_vrp_*.parquet` files
- `scripts/ingest/ingest_theta_contracts.py` — normalizes ThetaData contract listings
- `scripts/ingest/ingest_theta_option_eod.py` — normalizes ThetaData option EOD files
- `scripts/ingest/ingest_yahoo_daily_bars.py` — downloads via yfinance with retry
- `scripts/ingest/ingest_stooq_daily_bars.py` — downloads from Stooq free CSV endpoint
- `scripts/ingest/ingest_fred_macro.py` — fetches 12 curated FRED series

**dbt transform layer (14 models)**
- 6 staging models: yahoo, stooq, theta contracts, theta EOD, theta VRP, FRED macro
- 3 intermediate models: unified bars (all sources), option EOD + DTE, macro forward-fill
- 5 mart tables: equity backtest panel, option backtest panel, regime panel,
  optimization inputs, screening panel

**Configuration**
- `config/symbols.yaml` — 40-symbol universe with asset types and source aliases
- `config/macros.yaml` — 12 curated FRED series (rates, spreads, VIX, inflation, FX)
- `config/datasets.yaml` — table registry with grain, partition, and builder documentation
- `config/sources.yaml` — source registry with URLs and licensing notes

**Tests (13 passing)**
- `test_contract_id.py` — 6 tests
- `test_symbol_map.py` — 7 tests
- `test_validation_prices.py` — 7 tests
- `test_validation_options.py` — 7 tests
- `test_manifests.py` — 6 tests
- `test_parquet_io.py` — 4 tests

### Initial data load

- **2,579,090** equity bar rows (531 symbols, 2005–2026, alphaquant cache)
- **1,122,018** VRP feature rows (513 symbols, 2017–2026, ThetaData vrp_clean)
- **85,951** macro observation rows (11 FRED series, 1947–2026)
- **8,086** calendar dimension rows (2005–2035 weekdays)
- **40** symbols in dim_symbol

### Bug fixes (found during verification)
- `parquet.py`: Removed `use_legacy_dataset=True` (removed in PyArrow 23)
- `mart_backtest_equity_panel.sql`: Refactored nested window functions into 3 CTEs
- `int_underlying_bars_daily.sql`: Changed to read all sources (not just `source='yahoo'`)
- `stg_theta_contracts.sql` / `stg_theta_option_eod.sql`: Added placeholder parquets to
  prevent glob errors when option data not yet ingested
