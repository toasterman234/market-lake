# STATUS.md
# market-lake — Current Status

> Last updated: 2026-04-02 (post deep-audit + guardrails + regime panel fix session)

---

## Test Suite

| Suite | Result | Count |
|---|---|---|
| `pytest` unit + integration tests | ✅ PASSING | 70 / 70 |
| `dbt test` schema + uniqueness tests | ✅ PASSING | 39 / 39 |
| `dbt run` model builds | ✅ PASSING | 17 / 17 |

---

## Data State

### Canonical Datasets

| Dataset | Rows | Symbols / Series | Latest Date | Status |
|---|---|---|---|---|
| `fact_underlying_bar_daily` | 2,579,130 | 531 | 2026-03-30 | ✅ Current |
| `fact_option_eod` | 767,396,301 | 510 | 2026-03-30 | ✅ Full backfill complete |
| `fact_option_feature_daily` (VRP) | 1,122,873 | 513 | 2026-03-30 | ✅ Current — deduped |
| `fact_macro_series` | 204,174 | 33 series | 2026-03-31 | ✅ Current |
| `fact_ff_factors_daily` | 26,070 | 7 factors | 2026-01-30 | ⚠️ French lib lag (~2mo) |
| `fact_corporate_action` | 50,823 | 473 | 2026-03-30 | ✅ Deduped |
| `fact_financial_statements` | 2,375 | 500 | 2026-01-31 | ✅ |
| `fact_fundamentals_annual` | 2,375 | 500 | 2026-01-31 | ✅ |
| `fact_earnings_calendar` | 12,256 | 499 | 2026-07-16 | ✅ Incl. forward dates |
| `fact_short_interest` | 15,392,602 | ~11K symbols/day | 2026-03-31 | ✅ Deduped |
| `dim_symbol` | 531 | — | — | ✅ sector + industry |
| `dim_option_contract` | 9,917,418 | 513 | — | ✅ Full backfill complete |
| `dim_calendar` | 6,260 | — | 2028-12-29 | ✅ |

### Marts

| Mart | Rows | Status |
|---|---|---|
| `mart_backtest_equity_panel` | 2,578,599 | ✅ |
| `mart_backtest_option_panel` | 325,102,391 | ✅ Full universe — 325M rows |
| `mart_regime_panel_base` | 5,342 | ✅ 0 duplicate dates |
| `mart_regime_panel` | 5,342 | ✅ 0 duplicate dates (was 7,663 with 2,321 dups) |
| `mart_optimization_inputs` | 6,013,618 | ✅ |
| `mart_screening_panel` | 2,578,599 | ✅ 0 duplicate (symbol,date) keys |
| `mart_fundamental_screen` | 531 | ✅ All symbols (was broken at 40) |

### Macro Series (33 total in `fact_macro_series`)

Rates: DGS10, DGS2, DGS1MO, T10Y2Y, T10Y3M, FEDFUNDS
Vol: VIXCLS, VIX3M, VVIX, SKEW, GVZ, OVX, VIX9D, VXD, VXN, VXEEM, VXAPL, VXO
Inflation/Money: CPIAUCSL, T5YIE, DFII10, M2SL
Commodities: DCOILWTICO, DCOILBRENTEU
Other macro: UNRATE, BAMLH0A0HYM2, DTWEXBGS, USEPUINDXD
Stress: KCFSI, STLFSI4
P/C Ratios: CBOE_EQUITY_PC, CBOE_TOTAL_PC, CBOE_INDEX_PC (2003-2019)
FF Factors: 7 factors in `fact_ff_factors_daily`

---

## Guardrails (implemented 2026-04-01)

Eight automated guardrails protecting data integrity across every ingest and dbt run:

| # | Guardrail | Location | What it catches |
|---|---|---|---|
| 1 | Schema type enforcement | `src/market_lake/io/parquet.py` | Wrong types at write time (TIMESTAMP vs DATE, float vs int) |
| 2 | Post-write dedup verification | `src/market_lake/io/parquet.py` | Duplicate natural-key rows immediately after writing |
| 3 | Schema registry | `config/schemas.yaml` | 13 tables, canonical types + natural keys |
| 4 | Row count anomaly monitor | `scripts/ops/row_count_check.py` | >50% growth (duplication) or >5% shrinkage |
| 5 | Health dashboard | `scripts/ops/health_check.py` | Freshness, min rows, dedup — per table |
| 6 | Idempotent writes | `src/market_lake/io/parquet.py` | `delete_matching` prevents file accumulation on re-ingest |
| 7 | Pre-commit hook | `.git/hooks/pre-commit` | 30 unit tests run before every commit |
| 8 | dbt uniqueness tests | `dbt/models/schema.yml` | Natural-key uniqueness on 7 key models |

---

## Daily Automation

launchd installed: `~/Library/LaunchAgents/com.market-lake.daily-refresh.plist`
Runs: M–F at 6:45am
Pipeline:
1. ThetaTerminal health check / start
2. VRP gap-fill (options-research)
3. Ingest VRP → market-lake
4. Equity bars top-up (38 symbols)
5. FRED macro refresh
6. Bootstrap DuckDB
7. `dbt run`
8. `dbt test`
9. `pytest`
10. Row count anomaly check
11. Health dashboard snapshot

Log: `/tmp/market_lake_daily_YYYY-MM-DD.log`
