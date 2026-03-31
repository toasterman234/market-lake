# Ingestion Guide

This document covers all ingestion scripts, their inputs, outputs, and how to run them.

---

## General Principles

- Every ingest script validates data before writing
- Every write appends a manifest record to `canonical/metadata/fact_dataset_manifest/`
- `symbol_id` is always set via `stable_symbol_id()` — deterministic across all runs
- Partitioned outputs use PyArrow Hive partitioning (e.g. `year=2024/part-xxx.parquet`)
- Re-running a script with the same inputs produces the same `ingest_batch_id` (idempotent)

---

## Setup

All scripts require `MARKET_LAKE_ROOT` in your environment or `.env` file:

```bash
cd /path/to/market-lake
source .venv/bin/activate
export MARKET_LAKE_ROOT="$(pwd)"
```

---

## Dimension Builders

### build_dim_symbol.py

Builds `dim_symbol` from `config/symbols.yaml`.

```bash
python scripts/build/build_dim_symbol.py

# Or override with CLI args:
python scripts/build/build_dim_symbol.py \
    --symbols SPY QQQ IWM AAPL MSFT \
    --asset-type etf etf etf stock stock
```

**Output:** `canonical/dimensions/dim_symbol/dim_symbol.parquet`

---

### build_dim_calendar.py

Generates every weekday between `--start` and `--end`.

```bash
python scripts/build/build_dim_calendar.py --start 2005-01-01 --end 2035-12-31
```

**Output:** `canonical/dimensions/dim_calendar/dim_calendar.parquet`

---

## Equity Daily Bars

### ingest_existing_equity.py

Absorbs pre-existing `*_daily_*.parquet` files from any cache directory.
Designed to import the alphaquant / options-research equity cache without re-downloading.

```bash
python scripts/ingest/ingest_existing_equity.py \
    --input-dir /path/to/cache/history \
    --source-label "alphaquant_cache"
```

**Output:** `canonical/facts/fact_underlying_bar_daily/` partitioned by `year`

---

### ingest_yahoo_daily_bars.py

Downloads via yfinance with automatic retry (3 attempts, exponential backoff).

```bash
python scripts/ingest/ingest_yahoo_daily_bars.py \
    --symbols SPY QQQ IWM TLT GLD AAPL MSFT NVDA \
    --start 2005-01-01 \
    --end 2026-03-31
```

**Output:** `canonical/facts/fact_underlying_bar_daily/` partitioned by `year`

---

### ingest_stooq_daily_bars.py

Downloads from Stooq's free historical CSV endpoint. Includes polite delay between requests.

```bash
python scripts/ingest/ingest_stooq_daily_bars.py \
    --symbols SPY QQQ AAPL MSFT \
    --start 2005-01-01 \
    --end 2026-03-31 \
    --delay 1.5
```

**Output:** `canonical/facts/fact_underlying_bar_daily/` partitioned by `year`

**Note:** Stooq data is adjusted by default. Symbol format: `spy.us`, `aapl.us`. The script handles conversion automatically.

---

## ThetaData Options

### ingest_theta_contracts.py

Normalizes raw ThetaData contract listing CSV/Parquet files.

```bash
python scripts/ingest/ingest_theta_contracts.py \
    --input-dir raw/thetadata/contracts \
    --output-dir canonical/dimensions/dim_option_contract
```

**Required columns in source files:** `underlying_symbol`, `expiry`, `strike`, `option_type`

**Output:** `canonical/dimensions/dim_option_contract/dim_option_contract.parquet`

---

### ingest_theta_option_eod.py

Normalizes ThetaData EOD option files (bid/ask/last/volume/OI/Greeks).

```bash
python scripts/ingest/ingest_theta_option_eod.py \
    --input-dir raw/thetadata/options_eod \
    --output-dir canonical/facts/fact_option_eod
```

**Output:** `canonical/facts/fact_option_eod/` partitioned by `underlying_symbol`, `year`, `month`

---

### ingest_theta_vrp_features.py

Absorbs per-symbol VRP/IV feature parquets (one file per symbol).
Designed to import existing `*_vrp_*.parquet` files from ThetaData-derived pipelines.

```bash
# Import all symbols from a vrp_clean directory:
python scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir /path/to/vrp_clean \
    --output-dir canonical/features/fact_option_feature_daily

# Or filter to specific symbols:
python scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir /path/to/vrp_clean \
    --output-dir canonical/features/fact_option_feature_daily \
    --symbols SPY QQQ IWM TLT GLD
```

**Expected filename format:** `SYMBOL_vrp_STARTDATE_ENDDATE.parquet`

**Output:** `canonical/features/fact_option_feature_daily/` partitioned by `year`, `month`

---

## Macro Series

### ingest_fred_macro.py

Fetches FRED series using the free CSV endpoint (no API key needed).
Set `FRED_API_KEY` in `.env` for higher rate limits.

```bash
# Use series from config/macros.yaml:
python scripts/ingest/ingest_fred_macro.py

# Override with specific series:
python scripts/ingest/ingest_fred_macro.py --series FEDFUNDS DGS10 VIXCLS T10Y2Y
```

**Output:** `canonical/facts/fact_macro_series/` partitioned by `series_id`

**Note:** FRED data is for research use only. Do not redistribute raw series files.

---

## Checking Ingest Status

```python
import duckdb
ROOT = "/path/to/market-lake"
db = duckdb.connect(f"{ROOT}/duckdb/market.duckdb")
print(db.execute("""
    SELECT dataset_name, source, row_count, min_date, max_date, ingested_at, status
    FROM metadata.vw_dataset_manifest
    ORDER BY ingested_at DESC
""").df())
```
