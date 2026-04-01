# Data Sources

Complete reference for all data sources — current, planned, and evaluated.

---

## Currently Loaded

### ThetaData (Paid — OPTION.STANDARD)
**What we have:**
- VRP/IV features — 513 symbols, 2017–2026, daily (2.2M rows)
- Option EOD chains — 7 symbols (AAPL/META/MSFT/NVDA/QQQ/SPY/TSLA), 2008–2026 (207M rows)

**Access:** ThetaTerminal running locally at `http://127.0.0.1:25503`
**Auth:** `creds.txt` in ThetaTerminal directory
**Ingest scripts:**
- `scripts/ingest/ingest_theta_vrp_features.py` — VRP parquet files
- `scripts/ingest/ingest_theta_option_eod.py` — EOD chain parquets
- `scripts/ingest/ingest_theta_contracts.py` — Contract dimension
- `options-research/scripts/daily_gapfill.py` — Daily VRP refresh
- `options-research/scripts/chain_backfill.py` — Full chain backfill
**Known issues:** Column naming varies across export formats (handled in normalization)
**Rate limits:** 4 concurrent requests (OPTION.STANDARD plan)

---

### Yahoo Finance (Free — yfinance)
**What we have:** Used as primary equity bar source for any new pulls.
**Pre-existing data:** `alphaquant_cache` covers 531 symbols 2005–Mar 27 2026.
**Ingest script:** `scripts/ingest/ingest_yahoo_daily_bars.py`
**Status:** ✅ Tested and working
**Limitations:** No split-adjusted options data; adj_close is split+dividend adjusted
**Rate limits:** Unofficial API; no published limits. Add `--delay 0.5` for large pulls.

---

### FRED (Free — Federal Reserve Economic Data)
**What we have:** 14 series loaded (rates, spreads, VIX, CPI, unemployment, FX dollar)
**Ingest script:** `scripts/ingest/ingest_fred_macro.py`
**Config:** `config/macros.yaml` — add series IDs here to include in next run
**Status:** ✅ Working
**API:** Free CSV endpoint (no key needed). Optional: set `FRED_API_KEY` in `.env` for higher rate limits.
**URL pattern:** `https://fred.stlouisfed.org/graph/fredgraph.csv?id={SERIES_ID}`

---

### Kenneth French Data Library (Free)
**What we have:** FF5 factors + momentum, 1926–Jan 2026
**Ingest script:** `scripts/ingest/ingest_ff_factors.py`
**Status:** ✅ Working. Run `--download` flag to pull fresh from French library.
**Lag:** ~2 months publication delay. Re-run monthly.
**URL:** `https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html`

---

### CBOE (Free — Partial)
**What we have:** VIX3M, GVZ (Gold VIX), OVX (Oil VIX) via FRED
**What's missing:** VVIX, SKEW (FRED 404s — use CBOE CDN directly)
**Status:** ⚠️ Partial — VVIX and SKEW not yet loaded
**CBOE CDN URLs (free, no auth):**
- `https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv`
- `https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv`
- `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/equitypc.csv`
- `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/totalpc.csv`

---

### Stooq (Free — Currently Blocked)
**Status:** ❌ Historical endpoint blocked (HTTP 200, empty body) as of 2026.
**Ingest script:** `scripts/ingest/ingest_stooq_daily_bars.py`
**Notes:** Script exits cleanly. Use Yahoo Finance instead. Monitor for re-enablement.
**What still works:** Last quote endpoint (`/q/l/`) — not useful for backtesting.

---

## Planned — Phase 2 (Free Sources)

### CBOE Options Flow — Put/Call Ratios
**URL:** `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/equitypc.csv`
**What:** Daily equity and index put/call ratios. Sentiment + contrarian regime signal.
**Schema:** `date, total_pc_ratio, equity_pc_ratio, index_pc_ratio`
**Destination:** `fact_macro_series` (new series IDs: `CBOE_EQUITY_PC`, `CBOE_TOTAL_PC`)
**Script to write:** `scripts/ingest/ingest_cboe_options_flow.py`
**Effort:** 2–3 hours

---

### VIX Futures Term Structure
**URL:** CBOE CFE historical data or yfinance futures tickers (`^VX1.CF` etc.)
**What:** Daily VIX futures settlements for front 8 contracts. Full futures curve.
**Schema:** `date, contract_month, settlement, dte_to_expiry`
**Destination:** New table `fact_vix_futures`
**Script to write:** `scripts/ingest/ingest_vix_futures.py`
**dbt models needed:** `stg_vix_futures`, `mart_vix_term_structure`
**Effort:** 4–6 hours

---

### FRED Additional Series
**What:** High-value macro series not yet loaded.

| Series ID | Description |
|---|---|
| `M2SL` | M2 Money Supply (monthly) |
| `T5YIE` | 5-Year Breakeven Inflation Rate |
| `DFII10` | 10-Year Real Interest Rate (TIPS) |
| `USEPUINDXD` | Economic Policy Uncertainty Index (daily) |
| `DCOILWTICO` | WTI Crude Oil Price (daily) |
| `GOLDAMGBD228NLBM` | Gold Price London PM Fix (daily) |
| `KCFSI` | Kansas City Financial Stress Index |
| `STLFSI4` | St. Louis Financial Stress Index |

**How to add:** Put series IDs in `config/macros.yaml`, re-run `ingest_fred_macro.py`.
**Effort:** 30 minutes

---

### Earnings Calendar
**What:** Expected earnings dates per symbol. Critical for binary event risk management.
**Source options:**
- yfinance `ticker.calendar` — next earnings only (not historical)
- `earningswhispers.com` — scrape for historical + forward calendar
- SEC EDGAR — XBRL filings for exact earnings dates (authoritative, complex)
**Schema:** `symbol, earnings_date, period_end, eps_estimate, eps_actual, is_confirmed`
**Destination:** New table `fact_earnings_calendar`
**Script to write:** `scripts/ingest/ingest_earnings_calendar.py`
**dbt models needed:** `stg_earnings_calendar`, join into `mart_backtest_option_panel` for `days_to_earnings`
**Effort:** 4–6 hours

---

### Short Interest (FINRA)
**What:** Bi-monthly short interest per symbol.
**Source:** FINRA free data portal
**URL:** `https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data`
**Schema:** `symbol, settle_date, short_shares, avg_daily_volume, days_to_cover`
**Destination:** New table `fact_short_interest`
**Script to write:** `scripts/ingest/ingest_short_interest.py`
**Effort:** 3–4 hours

---

### Sector / Industry Classifications
**What:** GICS sector, industry group, industry for each symbol.
**Source:** yfinance `ticker.info['sector']` + `ticker.info['industry']`
**How:** Extend `dim_symbol` — add `sector`, `industry`, `market_cap_bucket` columns
**Script to write:** `scripts/build/enrich_dim_symbol.py`
**Effort:** 2–3 hours

---

### Implied Move from Earnings (Computed — No New Source)
**What:** ATM straddle price / spot = market's implied move into earnings.
Computed from existing `fact_option_eod` + `fact_earnings_calendar`.
**Destination:** `mart_earnings_implied_move` (new dbt mart)
**Effort:** 2–3 hours (pure dbt)

---

## Evaluated — Not Planned

### Alpha Vantage (Free tier)
**Reason not using:** 25 API calls/day limit on free tier makes bulk historical pulls impractical.
Requires premium (~$50/month) for useful volume.

### Quandl / Nasdaq Data Link (Free tier)
**Reason not using:** Most valuable datasets (futures, options) are paid.
Free tier too limited for systematic use.

### SEC EDGAR (Free)
**Potential use:** 13F filings (institutional positioning), XBRL fundamentals.
**Status:** Not planned for current phase — would require significant parsing work.
**Revisit:** If fundamental factor research becomes a priority.

### Open Options (Free)
**What:** Community-maintained options data project.
**Status:** Data quality and completeness not sufficient for systematic research.

### Polygon.io (Paid)
**Why not now:** We have ThetaData for options. Polygon adds value for real-time equity bars and news.
**Revisit:** If we need intraday data or news sentiment.

---

## Phase 3 — FinanceToolkit + yfinance Fundamentals

### FinanceToolkit (Open-Source, MIT License)
**Repo:** https://github.com/JerBouma/FinanceToolkit
**What we use it for:** Formula library only — 150+ transparent financial ratio implementations.
We do NOT use its data fetching layer.
**Key formulas we implement:** Piotroski F-Score, Altman Z-Score, debt/equity, current ratio,
FCF yield, FCF margin, ROE, ROA, gross margin, earnings quality, revenue growth.
**Install:** `pip install financetoolkit`
**License:** MIT — free for all use

---

### yfinance — Financial Statements (Free)
**What:** GAAP income statement, balance sheet, cash flow for US-listed equities.
**Access:**
```python
import yfinance as yf
t = yf.Ticker("AAPL")
income = t.income_stmt      # 4 years annual
balance = t.balance_sheet   # 4 years annual
cashflow = t.cashflow       # 4 years annual
```
**Coverage:** 4 years annual, all US-listed equities (ETFs excluded)
**Script to write:** `scripts/ingest/ingest_fundamentals.py`
**Tables:**
- `fact_financial_statements` — raw GAAP line items
- `fact_fundamentals_annual` — computed ratios (Piotroski, Altman Z, etc.)
**dbt models:**
- `stg_fundamentals_annual` — staging
- `mart_fundamental_screen` — VRP + fundamental composite scanner
**Status:** 🗓 Planned (Phase 3)
**Limitations:** 4-year annual only; no point-in-time; ETFs excluded; 2-6 week reporting lag
**Upgrade path:** Financial Modeling Prep (FMP) ~$14-40/month for quarterly + 30-year history
