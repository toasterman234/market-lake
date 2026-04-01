# STATUS.md
# market-lake — Current Status

> Last updated: 2026-04-01 (post-overnight session — all jobs complete)

---

## Test Suite

| Suite | Result | Count |
|---|---|---|
| `pytest` unit tests | ✅ PASSING | 36 / 36 |
| `dbt test` schema tests | ✅ PASSING | 28 / 28 |
| `dbt run` model builds | ✅ PASSING | 17 / 17 |

---

## Data State

### Canonical Datasets

| Dataset | Rows | Symbols / Series | Latest Date | Status |
|---|---|---|---|---|
| `fact_underlying_bar_daily` | 2,579,170 | 531 | 2026-03-30 | ✅ Current |
| `fact_option_eod` | 207,460,675+ | 7→500+ | 2026-03-20+ | 🔄 Chain ingest running |
| `fact_option_feature_daily` (VRP) | 2,244,891 | 513 | 2026-03-30 | ✅ Current |
| `fact_macro_series` | 165,149+ | **27 series** | 2026-03-31 | ✅ Current |
| `fact_ff_factors_daily` | 26,070 | 7 factors | 2026-01-30 | ⚠️ French lib lag |
| `fact_corporate_action` | 79,648 | 473 | 2026-03-30 | ✅ |
| `fact_financial_statements` | 2,375 | 500 | 2026-01-31 | ✅ |
| `fact_fundamentals_annual` | 2,375 | 500 | 2026-01-31 | ✅ |
| `fact_earnings_calendar` | 12,256 | 499 | 2026-07-16 | ✅ Incl. forward dates |
| `fact_short_interest` | **15,397,564** | ~11K symbols/day | 2026-03-31 | ✅ NEW — 2020-present |
| `dim_symbol` | 531 | — | — | ✅ sector + industry |
| `dim_option_contract` | 1,451,764+ | — | — | 🔄 Growing |
| `dim_calendar` | 6,260 | — | 2028-12-29 | ✅ |

### Marts (post last full dbt run — 17/17 clean)

| Mart | Status |
|---|---|
| `mart_backtest_equity_panel` | ✅ 2.58M rows |
| `mart_backtest_option_panel` | ✅ 207M+ rows — underlying_symbol null fix applied |
| `mart_optimization_inputs` | ✅ 6M rows |
| `mart_screening_panel` | ✅ 6M rows |
| `mart_regime_panel` | ✅ 7,662 rows — now includes VVIX, SKEW, M2, WTI, EPU |
| `mart_fundamental_screen` | ✅ 521 symbols — composite VRP + Piotroski + Altman Z |
| `stg_fundamentals_annual` | ✅ |

### Macro Series (27 total in `fact_macro_series`)

Rates: DGS10, DGS2, DGS1MO, T10Y2Y, T10Y3M, FEDFUNDS
Vol: VIXCLS, VIX3M, VVIX, SKEW, GVZ, OVX
Inflation/Money: CPIAUCSL, T5YIE, DFII10, M2SL
Commodities: DCOILWTICO, DCOILBRENTEU
Other macro: UNRATE, BAMLH0A0HYM2, DTWEXBGS, USEPUINDXD
Stress: KCFSI, STLFSI4
P/C Ratios: CBOE_EQUITY_PC, CBOE_TOTAL_PC, CBOE_INDEX_PC (2003-2019)
FF Factors: 7 factors in `fact_ff_factors_daily`

---

## In Progress

### Chain Ingest → dbt Rebuild
Step 1/4 running: Ingesting 517 chain parquet files into `fact_option_eod`.
Subsequent steps: contracts ingest → bootstrap DuckDB → dbt run.
Expected result: `mart_backtest_option_panel` grows from 7 → 500+ symbols.

---

## Bugs Fixed (Deep Audit 2026-04-01)

| Bug | Severity | Fix |
|---|---|---|
| 50.6M option EOD rows with null `underlying_symbol` | High | `stg_theta_option_eod`: COALESCE with SPLIT_PART(contract_id, '\|', 1) |
| `mart_regime_panel` missing VVIX, SKEW, M2, WTI, EPU | Medium | Added 6 new series to macro CTE + SELECT |
| `daily_refresh.sh` relative VRP output path | Low | Made absolute with `$MARKET_LAKE/canonical/...` |
| `daily_refresh.sh` macOS-only date syntax | Low | Added Linux fallback (`date -d '5 days ago'`) |
| 1 duplicate in `fact_earnings_calendar` (SMCI) | Low | Deduplicated parquet |

---

## Known Limitations

| Limitation | Detail |
|---|---|
| FF factors ~2 month lag | French library publication delay; forward-filled |
| CBOE P/C ratios 2019-present | Moved to paid DataShop; free CDN ends 2019-10-04 |
| Fundamentals 4-year annual only | yfinance free tier; FMP for 30yr quarterly |
| No point-in-time fundamentals | Look-ahead bias in backtests; fine for live screening |
| BF.B / BRK.B no equity bars | Dot-ticker breaks yfinance lookup |
| 40 equity bar dups (2026-03-27) | Same price, two sources; handled by int dedup |

---

## Daily Automation

launchd installed: `~/Library/LaunchAgents/com.market-lake.daily-refresh.plist`
Runs: M–F at 6:45am
Pipeline: ThetaTerminal → VRP gap-fill → ingest VRP → equity bars → FRED → bootstrap → dbt run
Log: `/tmp/market_lake_daily_YYYY-MM-DD.log`
