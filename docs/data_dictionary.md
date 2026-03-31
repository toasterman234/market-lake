# Data Dictionary

This document describes all canonical tables, their columns, types, and business meaning.

---

## dim_symbol

One row per canonical ticker symbol.

| Column | Type | Description |
|---|---|---|
| `symbol_id` | integer | Stable deterministic ID (SHA-256 hash of symbol) |
| `symbol` | string | Canonical ticker (uppercase, e.g. `SPY`, `AAPL`) |
| `asset_type` | string | `etf`, `stock`, `index`, `unknown` |
| `yahoo_symbol` | string | Symbol as used in Yahoo Finance API |
| `stooq_symbol` | string | Symbol as used in Stooq (e.g. `spy.us`) |

---

## dim_option_contract

One row per unique option contract.

| Column | Type | Description |
|---|---|---|
| `contract_id` | string | `UNDERLYING\|EXPIRY\|STRIKE\|TYPE` (e.g. `AAPL\|2026-06-19\|150\|C`) |
| `symbol_id` | integer | FK → dim_symbol |
| `underlying_symbol` | string | Underlying ticker |
| `occ_symbol` | string | OCC-format symbol if available |
| `expiry` | date | Expiration date |
| `strike` | double | Strike price |
| `option_type` | string | `C` (call) or `P` (put) |
| `multiplier` | integer | Contract multiplier (typically 100) |
| `first_seen` | date | First date this contract appeared in data |
| `last_seen` | date | Last date this contract appeared in data |

---

## dim_calendar

One row per weekday from 2005-01-03 to 2035-12-31.

| Column | Type | Description |
|---|---|---|
| `date` | date | Calendar date |
| `year` | integer | Calendar year |
| `month` | integer | Month (1–12) |
| `quarter` | integer | Quarter (1–4) |
| `day_of_week` | string | Monday–Friday |
| `week_of_year` | integer | ISO week number |
| `is_month_end` | boolean | True if last weekday of month |
| `is_quarter_end` | boolean | True if last weekday of quarter |
| `is_year_end` | boolean | True if last weekday of year |

---

## fact_underlying_bar_daily

One row per symbol × date × source. Partitioned by `year`.

| Column | Type | Description |
|---|---|---|
| `symbol_id` | integer | FK → dim_symbol |
| `symbol` | string | Ticker symbol |
| `date` | date | Trading date |
| `open` | double | Opening price |
| `high` | double | Intraday high |
| `low` | double | Intraday low |
| `close` | double | Unadjusted closing price |
| `adj_close` | double | Dividend/split adjusted close |
| `volume` | bigint | Share volume |
| `source` | string | `yahoo`, `stooq`, `alphaquant_cache`, etc. |
| `year` | integer | Partition key |

---

## fact_option_eod

One row per contract × date. Partitioned by `underlying_symbol`, `year`, `month`.

| Column | Type | Description |
|---|---|---|
| `contract_id` | string | FK → dim_option_contract |
| `symbol_id` | integer | FK → dim_symbol (underlying) |
| `underlying_symbol` | string | Underlying ticker |
| `date` | date | Quote date |
| `bid` | double | Best bid price |
| `ask` | double | Best ask price |
| `last` | double | Last trade price |
| `volume` | bigint | Option volume |
| `open_interest` | bigint | Open interest |
| `iv` | double | Implied volatility (annualized, decimal: 0.25 = 25%) |
| `delta` | double | Delta (–1 to 1) |
| `gamma` | double | Gamma |
| `theta` | double | Theta (daily dollar decay) |
| `vega` | double | Vega |
| `source` | string | Data source |
| `year`, `month` | integer | Partition keys |

---

## fact_option_feature_daily

One row per symbol × date. VRP/IV derived features. Partitioned by `year`, `month`.

| Column | Type | Description |
|---|---|---|
| `symbol_id` | integer | FK → dim_symbol |
| `symbol` | string | Ticker |
| `date` | date | As-of date |
| `spot_price` | double | Underlying spot price |
| `iv_7d` – `iv_180d` | double | Implied vol at 7, 14, 21, 30, 45, 60, 90, 180 day tenor |
| `ts_slope_30_60` | double | Term structure slope (IV_60 / IV_30) |
| `put_iv_10d` – `put_iv_50d` | double | Put IV at 10, 25, 50 delta |
| `call_iv_10d` – `call_iv_50d` | double | Call IV at 10, 25, 50 delta |
| `put_skew_25d` | double | 25-delta put skew (put_iv_25d / atm_iv) |
| `hv5` – `hv90` | double | Historical vol at 5, 10, 20, 30, 60, 90 day window |
| `vrp_30d` | double | Variance risk premium 30d (IV_30 – HV_30) |
| `ivr_252d` | double | IV rank over trailing 252 days (0–1) |
| `ivp_252d` | double | IV percentile over trailing 252 days (0–1) |
| `pc_volume_ratio` | double | Put/call volume ratio |
| `source` | string | Data source (`thetadata`) |
| `year`, `month` | integer | Partition keys |

---

## fact_macro_series

One row per series × date. Partitioned by `series_id`.

| Column | Type | Description |
|---|---|---|
| `series_id` | string | FRED series ID (e.g. `FEDFUNDS`, `DGS10`) |
| `label` | string | Human-readable description |
| `date` | date | Observation date |
| `value` | double | Observed value (may be null for missing vintages) |
| `source` | string | `fred` |
| `year` | integer | Partition key |

### Curated FRED Series

| series_id | Description | Frequency |
|---|---|---|
| `FEDFUNDS` | Federal Funds Rate | Monthly |
| `DGS10` | 10-Year Treasury Rate | Daily |
| `DGS2` | 2-Year Treasury Rate | Daily |
| `DGS1MO` | 1-Month T-Bill Rate | Daily |
| `T10Y2Y` | 10Y–2Y Treasury Spread | Daily |
| `T10Y3M` | 10Y–3M Treasury Spread | Daily |
| `UNRATE` | Unemployment Rate | Monthly |
| `CPIAUCSL` | Consumer Price Index | Monthly |
| `VIXCLS` | CBOE VIX | Daily |
| `BAMLH0A0HYM2` | US High Yield OAS | Daily |
| `DTWEXBGS` | US Dollar Index | Daily |

---

## fact_dataset_manifest

Audit trail. One row per ingest batch.

| Column | Type | Description |
|---|---|---|
| `ingest_batch_id` | string | Deterministic 16-char hash of input parameters |
| `dataset_name` | string | Canonical table written |
| `source` | string | Data source |
| `file_path` | string | Input path or source description |
| `row_count` | integer | Rows written |
| `schema_hash` | string | 16-char hash of DataFrame dtypes |
| `min_date` | string | Earliest date in the batch |
| `max_date` | string | Latest date in the batch |
| `ingested_at` | string | UTC ISO timestamp |
| `status` | string | `success` or `error` |
| `notes` | string | Optional notes |
