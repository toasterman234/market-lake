## [0.4.0] — 2026-04-01 (overnight + audit session)

### Added — New canonical tables

**`fact_short_interest`** — 15,397,564 rows, daily 2020-present
- FINRA daily short sale volume for ~11,000 US-listed securities
- Script: `scripts/ingest/ingest_short_interest.py`
- URL: `cdn.finra.org/equity/regsho/daily/CNMSshvol{YYYYMMDD}.txt`
- Schema: symbol, settle_date, short_shares, avg_daily_volume, days_to_cover
- Covers ~1,630 trading days from 2020-01-02 to 2026-03-31

**`fact_earnings_calendar`** — 12,256 rows, 499 symbols
- Historical + forward earnings dates via yfinance
- Covers 2007-2026, including 496 upcoming earnings events
- Script: `scripts/ingest/ingest_earnings_calendar.py`
- Required: lxml (`pip install lxml` in .venv)

**CBOE Put/Call Ratios** added to `fact_macro_series` (3 new series)
- CBOE_EQUITY_PC, CBOE_TOTAL_PC, CBOE_INDEX_PC (2003-2019)
- Source: `cdn.cboe.com/resources/options/volume_and_call_put_ratios/`
- Note: 2019-present unavailable from free CDN (moved to paid DataShop)

### Added — New dbt models (17 total)
- `stg_fundamentals_annual`: type-safe staging for fact_fundamentals_annual
- `mart_fundamental_screen`: composite VRP + Piotroski + Altman Z scanner
  * 18 FULL SIZE, 127 HALF SIZE, 293 QUARTER SIZE, 83 SKIP (521 total)
  * Top candidates: AOS (Piotr=8, Z=3.91), APP (Piotr=9), LRCX (Piotr=8)

### Fixed — Bugs (deep audit)

**CRITICAL**: `stg_theta_option_eod` — 50.6M rows had null `underlying_symbol`
  - Root cause: `thetadata_vrp_validate` source didn't populate the field
  - Fix: `COALESCE(nullif(underlying_symbol,'NAN'), SPLIT_PART(contract_id,'|',1))`
  - All 207M option EOD rows now have valid underlying_symbol

**MEDIUM**: `mart_regime_panel` was missing VVIX, SKEW, M2, WTI, EPU, KC stress
  - All 6 new macro series were in `int_macro_series` but not exposed
  - Regime panel now covers 33 columns total

**LOW**: `daily_refresh.sh` — relative path + macOS-only date syntax both fixed
**LOW**: `fact_earnings_calendar` — 1 duplicate (SMCI 2025-02-25) removed

### Infrastructure
- launchd automation installed: M-F 6:45am daily refresh
- `scripts/ops/daily_refresh.sh` + `com.market-lake.daily-refresh.plist`

### Overnight processes completed
- Option chain full backfill: 517 files, all 513 symbols × 2017→2026 ✅
- Short interest: 15.4M rows (2020-present) ✅
- dbt run: 17/17 models, 0 errors ✅
- pytest: 36/36 passing ✅

---

# Changelog

---

## [0.3.0] — 2026-04-01 (overnight session)

### Added — New canonical tables

**`fact_corporate_action`** — 57,422 rows, 503 symbols
- Dividends and stock splits back to IPO for all 531 symbols
- Script: `scripts/ingest/ingest_corporate_actions.py`
- Source: yfinance `ticker.actions`
- Schema: `symbol, symbol_id, action_date, action_type (dividend|split), value, split_ratio, year`

**`fact_financial_statements`** — 2,375 rows, 502 equity symbols
- Annual GAAP income statement, balance sheet, cash flow — 4 years back
- Script: `scripts/ingest/ingest_fundamentals.py`
- Source: yfinance `ticker.income_stmt`, `ticker.balance_sheet`, `ticker.cashflow`
- Key columns: revenue, gross_profit, ebit, net_income, total_assets, total_debt, total_equity,
  current_assets, current_liabilities, operating_cash_flow, capex, free_cash_flow

**`fact_fundamentals_annual`** — 2,375 rows, 502 equity symbols
- Computed financial ratios (FinanceToolkit formulas) per symbol per fiscal year
- Script: `scripts/ingest/ingest_fundamentals.py`
- Key ratios: gross_margin, ebit_margin, net_margin, roe, roa, current_ratio,
  debt_to_equity, debt_to_assets, interest_coverage, fcf_margin, earnings_quality,
  revenue_growth_yoy, earnings_growth_yoy, **piotroski_score**, **altman_z_score**

### Added — New ingest scripts

- `scripts/ingest/ingest_fundamentals.py` — annual financial statements + ratio computation
- `scripts/ingest/ingest_corporate_actions.py` — dividends + splits via yfinance
- `scripts/build/enrich_dim_symbol.py` — adds asset_type, sector, industry to dim_symbol via yfinance

### Updated — Macro series (24 total, was 21)

New series added via CBOE CDN and FRED:
- `VVIX` — CBOE VIX of VIX (4,989 rows, back to 2006) — was 404 on FRED, fixed via CBOE CDN
- `SKEW` — CBOE SKEW Index (9,112 rows, back to 1990) — same fix
- `M2SL` — M2 Money Supply
- `T5YIE` — 5-Year Breakeven Inflation Rate
- `DFII10` — 10-Year Real Rate (TIPS)
- `USEPUINDXD` — Economic Policy Uncertainty Index
- `DCOILWTICO` — WTI Crude Oil Price
- `DCOILBRENTEU` — Brent Crude Oil Price
- `KCFSI` — Kansas City Financial Stress Index
- `STLFSI4` — St. Louis Financial Stress Index

### Updated — dim_symbol
- Now includes `sector`, `industry` columns populated via yfinance
- `asset_type` corrected: 503 stocks, 27 ETFs, 1 unknown (was 487 unknown)
- Script: `scripts/build/enrich_dim_symbol.py`

### Updated — dim_option_contract
- Grew from 559K → **1,451,764 contracts** as chain backfill progresses

### Updated — fact_macro_series
- 100,309 → **165,149 rows** after 10 new series added

### Running overnight
- Option chain full backfill: ~316 symbols, 2017→present (ThetaData, ~21hr total)
- Tomorrow: ingest chain output, rebuild dbt, mart_backtest_option_panel will expand from 7 → 500+ symbols

---

## [0.2.0] — 2026-04-01

### Added
- FF5 + momentum factors (26,070 rows, 1926–Jan 2026)
- CBOE vol series: VIX3M, GVZ, OVX
- dim_symbol expanded 40 → 531 symbols
- dim_calendar corrected: 8,086/2035 → 6,260/2028
- Option EOD: 207M rows, 7 symbols, 2008–2026
- VRP gap-fill: 513 symbols → Mar 30 2026 (2.24M rows)
- stg_ff_factors dbt model
- int_macro_series: 21 series (FRED + FF + CBOE vol)
- mart_regime_panel expanded to 33 columns
- mart_optimization_inputs: FF factors + excess_return_1d

### Fixed
- Memory-safe ingestion (one file at a time, gc.collect)
- Option EOD column mapping (right→option_type, expiration→expiry)
- int_option_eod nullable-safe join
- Manifest dedup: 15 rows → 8 clean records

---

## [0.1.0] — 2026-03-31

### Added — Core foundation
- Python package: settings, io, ids, validation
- 7 ingest scripts, 3 build scripts, 14 dbt models
- DuckDB init SQL, 4 docs
- 36 pytest + 28 dbt tests
- Initial load: 2.58M equity rows, 1.12M VRP rows, 86K macro rows
