# STATUS.md
# market-lake — Current Status

> Last updated: 2026-04-01

---

## Test Suite

| Suite | Result | Count |
|---|---|---|
| `pytest` unit tests | ✅ PASSING | 36 / 36 |
| `dbt test` schema tests | ✅ PASSING | 28 / 28 |
| `dbt run` model builds | ✅ PASSING | 15 / 15 |

---

## Data State

### Canonical Datasets

| Dataset | Rows | Symbols | Date Range | Status |
|---|---|---|---|---|
| `fact_underlying_bar_daily` | 2,579,090 | 531 | 2005-01-03 → 2026-03-27 | ✅ Current |
| `fact_option_eod` | 207,460,675 | 7 | 2008-01-02 → 2026-03-20 | ⚠️ 7 symbols only; ends Mar 20 |
| `fact_option_feature_daily` (VRP) | 2,244,891 | 513 | 2017-01-03 → 2026-03-30 | ✅ Current through Mar 30 |
| `fact_macro_series` | 100,309 | 14 series | 1947-01-01 → 2026-03-31 | ✅ Current |
| `fact_ff_factors_daily` | 26,070 | 7 factors | 1926-11-03 → 2026-01-30 | ⚠️ French library lag (~2 months) |
| `dim_symbol` | 531 | — | — | ✅ |
| `dim_calendar` | 6,260 | — | 2005-01-03 → 2028-12-29 | ✅ |
| `dim_option_contract` | 559,866 | 5 | 2008 → 2028 | ⚠️ Only 5 of 531 symbols |

### Marts

| Mart | Rows | Latest Date | Status |
|---|---|---|---|
| `mart_backtest_option_panel` | 325,102,391 | 2026-03-20 | ⚠️ 7 symbols only |
| `mart_backtest_equity_panel` | 2,578,559 | 2026-03-27 | ✅ |
| `mart_optimization_inputs` | 6,013,578 | 2026-03-27 | ✅ |
| `mart_screening_panel` | 6,013,578 | 2026-03-27 | ✅ |
| `mart_regime_panel` | 7,662 | 2026-03-27 | ✅ |

### Macro / Factor Series (21 total in `int_macro_series`)

All forward-filled to daily equity date spine.

| Series ID | Description | Latest | Status |
|---|---|---|---|
| VIXCLS | CBOE VIX | 2026-03-27 | ✅ |
| VIX3M | CBOE 3-Month VIX | 2026-03-27 | ✅ |
| GVZ | Gold Volatility Index | 2026-03-27 | ✅ |
| OVX | Oil Volatility Index | 2026-03-27 | ✅ |
| DGS10 | 10-Year Treasury | 2026-03-27 | ✅ |
| DGS2 | 2-Year Treasury | 2026-03-27 | ✅ |
| DGS1MO | 1-Month T-Bill | 2026-03-27 | ✅ |
| T10Y2Y | 10Y-2Y Spread | 2026-03-27 | ✅ |
| T10Y3M | 10Y-3M Spread | 2026-03-27 | ✅ |
| FEDFUNDS | Fed Funds Rate | 2026-03-27 | ✅ |
| CPIAUCSL | CPI | 2026-03-27 | ✅ |
| UNRATE | Unemployment Rate | 2026-03-27 | ✅ |
| BAMLH0A0HYM2 | HY Credit Spread | 2026-03-27 | ✅ |
| DTWEXBGS | US Dollar Index | 2026-03-27 | ✅ |
| FF_MKT_RF | FF Market Excess Return | 2026-03-27 | ⚠️ Raw data ends Jan 30 2026; forward-filled |
| FF_SMB | FF Small Minus Big | 2026-03-27 | ⚠️ Same |
| FF_HML | FF High Minus Low | 2026-03-27 | ⚠️ Same |
| FF_RMW | FF Profitability | 2026-03-27 | ⚠️ Same |
| FF_CMA | FF Investment | 2026-03-27 | ⚠️ Same |
| FF_MOM | FF Momentum | 2026-03-27 | ⚠️ Same |
| FF_RF | FF Risk-Free Rate | 2026-03-27 | ⚠️ Same |

---

## What Works ✅

- **VRP scanner**: 513-symbol universe, IVR/IVP/VRP/skew/term structure, current through Mar 30
- **Equity backtest panel**: 531 symbols, 20+ years of daily returns/vol/momentum
- **Regime panel**: 33-column daily macro+factor regime surface, VIX term structure
- **Option backtest panel**: 325M rows, 7 symbols, real bid/ask/Greeks/DTE back to 2008
- **Optimization mart**: Sharpe signal, excess return, FF factors, regime context per symbol/date
- **Screening mart**: Cross-sectional IVR/VRP/momentum ranks for scanner
- **All ingest scripts**: Yahoo ✅, ThetaData VRP ✅, ThetaData EOD ✅, FRED ✅, FF factors ✅
- **Auto gap-fill → ingest → dbt pipeline**: Watcher script tested end-to-end

---

## What Doesn't Work ❌

### `ingest_stooq_daily_bars.py` — Blocked by Stooq
**Problem:** Stooq returns HTTP 200 with an empty body for bulk historical requests. This is a deliberate anti-scraping measure, not a script bug. Script exits cleanly with a message pointing to Yahoo.
**Impact:** None — Yahoo Finance is the primary equity source and works without restrictions.
**Fix:** Use Yahoo. Monitor Stooq periodically; the script will work again if they re-enable the endpoint.

### VVIX / CBOE SKEW Index — FRED 404
**Problem:** `VVIXCLS` and `SKEWCLS` return 404 on FRED's CSV endpoint. CBOE removed these series from FRED.
**Impact:** Minor — VIX + VIX3M + GVZ + OVX cover the vol surface adequately.
**Fix:** Pull directly from CBOE CDN. See Roadmap.

---

## Work In Progress ⚙️

### Option Chain Backfill — 506 Symbols Missing
**Status:** Not started. Requires ThetaTerminal running + `chain_backfill.py` from options-research.
**What's needed:** Full historical option EOD (bid/ask/Greeks/volume/OI) for all 513 symbols back to 2017.
**Estimated time:** ~21 hours at 4 workers.
**Impact:** `mart_backtest_option_panel` currently only covers 7 symbols.

### Option EOD Freshness — Ends Mar 20
**Status:** The 7-symbol EOD data ends Mar 20. Incremental pull needed from Dec 2025 → today.
**Script:** `chain_backfill.py --ticker SPY QQQ AAPL NVDA META MSFT TSLA --start 2025-12-13`

### Corporate Actions Table
**Status:** Not built. `fact_corporate_action` doesn't exist yet.
**Impact:** `adj_close` is pre-adjusted from source, so pricing is correct. Explicit split/dividend log is missing.
**Script needed:** `ingest_corporate_actions.py` using `yfinance` with `actions=True`.

### Daily Automation
**Status:** Gap-fill watcher script exists and has been tested end-to-end. Not yet wired into launchd for automatic daily runs.
**What's needed:** launchd plist to run VRP gap-fill + ingest + dbt every morning at 6:45am.

---

## Known Limitations

| Limitation | Detail |
|---|---|
| No intraday data | All data is daily EOD only |
| No live/streaming prices | market-lake is historical research only |
| Option EOD 7 symbols only | Full backfill is a multi-day compute job |
| FF factors ~2 month lag | French library publishes with delay; forward-filled in the interim |
| dim_option_contract sparse | Only 5 symbols from chain parquets; grows automatically as backfill runs |
| No earnings/fundamental data | Not planned for current phase |
| No alternative data | Sentiment, positioning, etc. not yet planned |
