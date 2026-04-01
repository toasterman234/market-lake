# STATUS.md
# market-lake — Current Status

> Last updated: 2026-04-01 (overnight session)

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

| Dataset | Rows | Symbols / Series | Latest Date | Status |
|---|---|---|---|---|
| `fact_underlying_bar_daily` | 2,579,170 | 531 | 2026-03-30 | ✅ Current |
| `fact_option_eod` | 207,460,675 | 7 | 2026-03-20 | ⚠️ 7 symbols; chain backfill running |
| `fact_option_feature_daily` (VRP) | 2,244,891 | 513 | 2026-03-30 | ✅ Current |
| `fact_macro_series` | 165,149 | **24 series** | 2026-03-31 | ✅ Current |
| `fact_ff_factors_daily` | 26,070 | 7 factors | 2026-01-30 | ⚠️ French lib lag ~2 months |
| `fact_corporate_action` | **57,422** | 503 | 2026-03-30 | ✅ NEW — dividends + splits |
| `fact_financial_statements` | **2,375** | 502 equity symbols | 2026-01-31 | ✅ NEW — annual GAAP statements |
| `fact_fundamentals_annual` | **2,375** | 502 equity symbols | 2026-01-31 | ✅ NEW — computed ratios |
| `dim_symbol` | 531 | — | — | ✅ Now includes sector + industry |
| `dim_option_contract` | **1,451,764** | — | — | ✅ Growing as chain backfill runs |
| `dim_calendar` | 6,260 | — | 2028-12-29 | ✅ |

### Marts

| Mart | Rows | Latest Date | Status |
|---|---|---|---|
| `mart_backtest_option_panel` | 207,460,675 | 2026-03-20 | ⚠️ 7 symbols only |
| `mart_backtest_equity_panel` | 2,578,559 | 2026-03-27 | ✅ |
| `mart_optimization_inputs` | 6,013,578 | 2026-03-27 | ✅ |
| `mart_screening_panel` | 6,013,578 | 2026-03-27 | ✅ |
| `mart_regime_panel` | 7,662 | 2026-03-27 | ✅ |

> Note: Marts will be rebuilt tomorrow after overnight chain backfill ingestion.

### Macro / Factor Series (24 total in `int_macro_series`)

| Series ID | Description | Latest | Status |
|---|---|---|---|
| VIXCLS | CBOE VIX | 2026-03-27 | ✅ |
| VIX3M | CBOE 3-Month VIX | 2026-03-27 | ✅ |
| VVIX | CBOE VIX of VIX | 2026-03-31 | ✅ **NEW** |
| SKEW | CBOE SKEW Index | 2026-03-31 | ✅ **NEW** |
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
| M2SL | M2 Money Supply | 2026-03-27 | ✅ **NEW** |
| T5YIE | 5-Year Breakeven Inflation | 2026-03-27 | ✅ **NEW** |
| DFII10 | 10-Year Real Rate (TIPS) | 2026-03-27 | ✅ **NEW** |
| USEPUINDXD | Economic Policy Uncertainty | 2026-03-27 | ✅ **NEW** |
| DCOILWTICO | WTI Crude Oil Price | 2026-03-27 | ✅ **NEW** |
| DCOILBRENTEU | Brent Crude Oil Price | 2026-03-27 | ✅ **NEW** |
| KCFSI | KC Financial Stress Index | 2026-03-27 | ✅ **NEW** |
| STLFSI4 | St. Louis Financial Stress Index | 2026-03-27 | ✅ **NEW** |
| FF_MKT_RF–FF_MOM | Fama-French 7 factors | 2026-03-27 | ⚠️ Raw ends Jan 30; forward-filled |

---

## What's Running Overnight

| Process | Detail | ETA |
|---|---|---|
| **Option chain backfill** | Full history 2017→present for ~316 missing symbols (ThetaData) | Morning |
| **Corporate actions** | 503 symbols — 57K rows complete | ✅ Done |
| **Fundamentals ingest** | 502 equity symbols — 2,375 rows complete | ✅ Done |

---

## What Works ✅

- **VRP scanner**: 513-symbol universe, IVR/IVP/VRP/skew/term structure, current through Mar 30
- **Equity backtest panel**: 531 symbols, 20+ years of returns/vol/momentum
- **Regime panel**: 33-column daily surface — VIX/VIX3M/VVIX/SKEW/FF factors/macro
- **Option backtest panel**: 207M rows, 7 symbols, real bid/ask/Greeks/DTE back to 2008
- **Fundamentals screening**: Piotroski Score + Altman Z-Score + 10 ratios for 502 equity symbols
- **Corporate actions**: Dividends + splits for 503 symbols back to IPO
- **dim_symbol**: 503 stocks + 27 ETFs, with sector + industry classifications
- **All ingest scripts**: Yahoo ✅, ThetaData VRP ✅, ThetaData EOD ✅, FRED ✅, FF factors ✅, Fundamentals ✅, Corporate actions ✅

---

## What Doesn't Work ❌

### `ingest_stooq_daily_bars.py` — Blocked by Stooq
**Problem:** Stooq returns HTTP 200 empty body for bulk historical requests.
**Impact:** None — Yahoo Finance is primary and works.
**Fix:** Script exits cleanly with clear message. Monitor for re-enablement.

---

## Work In Progress ⚙️

### Option Chain Full Backfill — Running Overnight
**Status:** ✅ Running. Step 1 (incremental Mar 13→30 for 195 existing symbols) complete.
Step 2 (full 2017→present for ~316 missing symbols) running overnight.
**Impact:** `mart_backtest_option_panel` currently 7 symbols. Will expand to 500+ after backfill.
**Next:** Ingest chain parquets into market-lake tomorrow morning, rebuild dbt.

### Daily Automation — Not Yet Built
**Status:** Watcher script exists and tested end-to-end. Not yet wired into launchd.
**What's needed:** `scripts/ops/daily_refresh.sh` + launchd plist.

---

## Known Limitations

| Limitation | Detail |
|---|---|
| Option EOD 7 symbols | Full backfill running overnight |
| Fundamentals 4-year history only | yfinance free tier; upgrade to FMP for 30yr |
| Fundamentals annual only | Quarterly needs FMP paid |
| FF factors ~2 month lag | French library publication delay; forward-filled |
| dim_option_contract grows with backfill | Currently 1.45M contracts; will expand overnight |
| No point-in-time fundamentals | yfinance gives as-reported; look-ahead bias in backtests |
| No earnings calendar | Phase 2 roadmap item |
| No short interest | Phase 2 roadmap item |
