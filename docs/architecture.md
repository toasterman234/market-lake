# Architecture

## Overview

market-lake is a layered local-first data lakehouse. It follows the medallion architecture pattern:

```
raw (bronze) → canonical (silver) → marts (gold)
```

All storage is Parquet. All queries go through DuckDB. All transforms are dbt SQL models.

---

## Layer Definitions

### raw/
Immutable vendor drops. Files land here exactly as received.
- `raw/thetadata/contracts/` — option contract listings
- `raw/thetadata/options_eod/` — daily option EOD quotes
- `raw/thetadata/vrp/` — raw VRP source files
- `raw/yahoo/` — (not used; yfinance writes directly to canonical)
- `raw/stooq/` — (not used; Stooq writes directly to canonical)
- `raw/fred/` — (not used; FRED writes directly to canonical)

**Rule:** Never modify anything in `raw/`. If a source has bad data, fix it in the ingest script, not in the raw file.

---

### canonical/
Normalized, validated Parquet tables. The source of truth for all downstream work.

#### Dimensions (slowly changing)
| Table | Key | Description |
|---|---|---|
| `dim_symbol` | `symbol_id` | Universe of ticker symbols with asset types |
| `dim_option_contract` | `contract_id` | All option contracts ever seen |
| `dim_calendar` | `date` | Every weekday 2005–2035 with calendar metadata |

#### Facts (time-series)
| Table | Key | Partition | Description |
|---|---|---|---|
| `fact_underlying_bar_daily` | `symbol, date` | `year` | Daily OHLCV for all equities/ETFs |
| `fact_option_eod` | `contract_id, date` | `underlying_symbol, year, month` | Daily option quotes + Greeks |
| `fact_macro_series` | `series_id, date` | `series_id` | FRED macro series |

#### Features (derived)
| Table | Key | Partition | Description |
|---|---|---|---|
| `fact_option_feature_daily` | `symbol, date` | `year, month` | VRP/IV/HV/skew features from ThetaData |

#### Metadata
| Table | Key | Description |
|---|---|---|
| `fact_dataset_manifest` | `ingest_batch_id` | Audit trail: source, row count, schema hash, date range |

---

### dbt Models

```
staging/            Clean casts + null filters on canonical parquet
intermediate/       Joins, deduplication, forward-filling
marts/              Research-facing tables (materialized as DuckDB tables)
```

Dependency graph:
```
stg_yahoo_daily_bars    ──┐
stg_stooq_daily_bars    ──┤→ int_underlying_bars_daily ──→ mart_backtest_equity_panel ──┐
                           │                              → mart_regime_panel           │
stg_fred_macro ─────────→ int_macro_series ──────────────→ mart_optimization_inputs  ──┤
                                                          → mart_screening_panel       │
stg_theta_vrp_features ────────────────────────────────────────────────────────────── ┤
                                                                                        │
stg_theta_contracts ──┐                                                                │
stg_theta_option_eod──┤→ int_option_eod ──────────────→ mart_backtest_option_panel ──┘
```

---

### DuckDB Query Layer

`duckdb/market.duckdb` contains:
- Schema `canonical` — views onto dimension and fact parquets
- Schema `features` — views onto VRP feature parquets
- Schema `marts` — materialized mart tables (from dbt)
- Schema `metadata` — manifest view

The DuckDB file is **not committed to git**. It is rebuilt from scratch by running:
```bash
python scripts/build/bootstrap_duckdb.py
cd dbt && dbt run
```

---

## Stable IDs

**`symbol_id`** — Deterministic integer from SHA-256 of the uppercase symbol string, capped to int32 range. The same symbol always gets the same ID on every machine in every run:
```python
int(hashlib.sha256("SPY".encode()).hexdigest()[:8], 16) % (2**31 - 1)
```

**`contract_id`** — Pipe-delimited string: `UNDERLYING|EXPIRY|STRIKE|TYPE`
Example: `AAPL|2026-06-19|150|C`

Strike is formatted to remove trailing zeros: `400.0` → `400`, `99.5` → `99.5`.

---

## Manifest Tracking

Every ingest script writes a `ManifestRecord` to `canonical/metadata/fact_dataset_manifest/`.
The record contains:
- `ingest_batch_id` — deterministic hash of inputs (stable across reruns of same data)
- `dataset_name` — which canonical table was written
- `source` — data source label
- `row_count`, `schema_hash` — data fingerprint
- `min_date`, `max_date` — date coverage
- `status` — success / error
- `ingested_at` — UTC timestamp

Query the manifest via DuckDB:
```sql
SELECT * FROM metadata.vw_dataset_manifest ORDER BY ingested_at DESC;
```
