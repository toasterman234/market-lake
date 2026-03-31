# Query Guide

Common DuckDB queries and patterns for research and backtesting.

---

## Connecting

### Python

```python
from market_lake.io.duckdb_conn import open_db
from market_lake.settings import Settings

with open_db(Settings.load().duckdb_path) as con:
    df = con.execute("SELECT ...").df()
```

### DuckDB CLI

```bash
cd /path/to/market-lake
duckdb duckdb/market.duckdb
```

---

## Prices

```sql
-- Latest close for all symbols
SELECT symbol, date, close, adj_close, volume, source
FROM canonical.vw_prices_daily
WHERE date = (SELECT MAX(date) FROM canonical.vw_prices_daily)
ORDER BY symbol;

-- SPY full history
SELECT date, close, adj_close, volume
FROM canonical.vw_prices_daily
WHERE symbol = 'SPY'
ORDER BY date;

-- Coverage per source
SELECT source, COUNT(DISTINCT symbol) AS symbols,
       COUNT(*) AS rows, MIN(date) AS first, MAX(date) AS last
FROM canonical.vw_prices_daily
GROUP BY source ORDER BY rows DESC;
```

---

## VRP / IV Features

```sql
-- Latest VRP snapshot ranked by IVR
SELECT symbol, iv_30d, ivr_252d, ivp_252d, vrp_30d, put_skew_25d, ts_slope_30_60
FROM features.vw_option_features_daily
WHERE date = (SELECT MAX(date) FROM features.vw_option_features_daily)
  AND ivr_252d IS NOT NULL
ORDER BY ivr_252d DESC LIMIT 20;

-- SPY IV term structure over time
SELECT date, iv_7d, iv_30d, iv_60d, iv_90d, ts_slope_30_60, vrp_30d
FROM features.vw_option_features_daily
WHERE symbol = 'SPY'
  AND date >= '2023-01-01'
ORDER BY date;

-- Symbols with elevated IV rank AND positive VRP
SELECT symbol, date, ivr_252d, ivp_252d, vrp_30d, put_skew_25d
FROM features.vw_option_features_daily
WHERE date = (SELECT MAX(date) FROM features.vw_option_features_daily)
  AND ivr_252d > 0.7
  AND vrp_30d > 0.02
ORDER BY vrp_30d DESC;
```

---

## Equity Backtest Panel

```sql
-- SPY 1-year returns history
SELECT symbol, date, return_1d, return_21d, realized_vol_20d, mom_12m
FROM main_marts.mart_backtest_equity_panel
WHERE symbol = 'SPY'
  AND date >= '2023-01-01'
ORDER BY date;

-- Momentum ranking across universe on a date
SELECT symbol, mom_12m, realized_vol_20d,
       mom_12m / NULLIF(realized_vol_20d, 0) AS sharpe_signal
FROM main_marts.mart_backtest_equity_panel
WHERE date = '2026-03-27'
  AND realized_vol_20d > 0
ORDER BY sharpe_signal DESC NULLS LAST LIMIT 20;
```

---

## Regime Panel

```sql
-- Recent regime state
SELECT date, spy_close, vix, vol_regime, trend_regime, yield_curve_regime,
       fed_funds_rate, rate_10y, yield_curve_10y2y
FROM main_marts.mart_regime_panel
ORDER BY date DESC LIMIT 20;

-- Count days by vol regime
SELECT vol_regime, COUNT(*) AS days,
       AVG(spy_return_1d) * 252 AS ann_avg_return,
       STDDEV(spy_return_1d) * SQRT(252) AS ann_vol
FROM main_marts.mart_regime_panel
WHERE spy_return_1d IS NOT NULL
GROUP BY vol_regime ORDER BY days DESC;
```

---

## Scanner (cross-sectional)

```sql
-- Today's top 20 candidates by combined VRP + momentum rank
SELECT symbol, date, ivr_252d, vrp_30d, mom_12m, ivr_rank, vrp_rank, mom_rank,
       (ivr_rank + vrp_rank) / 2 AS combined_score
FROM main_marts.mart_screening_panel
WHERE date = (SELECT MAX(date) FROM main_marts.mart_screening_panel)
ORDER BY combined_score DESC LIMIT 20;
```

---

## Optimization Inputs

```sql
-- Optimizer inputs for a given date — all symbols with valid signals
SELECT symbol, ann_return_est, ann_vol_est, sharpe_like_signal, ivr_252d, vrp_30d, vol_regime
FROM main_marts.mart_optimization_inputs
WHERE date = '2026-03-27'
  AND ann_vol_est > 0
  AND ann_vol_est < 2.0  -- exclude extreme vol
ORDER BY sharpe_like_signal DESC NULLS LAST;
```

---

## Macro

```sql
-- Latest macro readings
SELECT series_id, label, date, value
FROM canonical.vw_macro_series
WHERE (series_id, date) IN (
    SELECT series_id, MAX(date) FROM canonical.vw_macro_series GROUP BY series_id
)
ORDER BY series_id;

-- Yield curve history
SELECT date,
    MAX(CASE WHEN series_id='DGS10'  THEN value END) AS rate_10y,
    MAX(CASE WHEN series_id='DGS2'   THEN value END) AS rate_2y,
    MAX(CASE WHEN series_id='T10Y2Y' THEN value END) AS spread_10y2y
FROM canonical.vw_macro_series
WHERE series_id IN ('DGS10','DGS2','T10Y2Y')
GROUP BY date ORDER BY date DESC LIMIT 30;
```

---

## Manifest / Audit

```sql
-- Full ingest history
SELECT dataset_name, source, row_count, min_date, max_date, status, ingested_at
FROM metadata.vw_dataset_manifest
ORDER BY ingested_at DESC;

-- Check for any failed ingests
SELECT * FROM metadata.vw_dataset_manifest WHERE status != 'success';
```
