# ROADMAP.md
# market-lake — Roadmap

> Priority order reflects research value, implementation complexity, and data availability.
> All free sources are flagged. Paid sources require existing subscriptions noted.

---

## Phase 1 — Operational Completeness (Current Sprint)

These are gaps in data we already have infrastructure for.

### 1.1 Option Chain Full Backfill 🔄 RUNNING OVERNIGHT
**Gap:** `mart_backtest_option_panel` only covers 7 symbols (SPY/QQQ/AAPL/NVDA/META/MSFT/TSLA).
**Source:** ThetaData (OPTION.STANDARD subscription — already active)
**Script:** `options-research/scripts/chain_backfill.py`
**Steps:**
```bash
# Start ThetaTerminal
cd "/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/ThetaTerminal"
java -jar ThetaTerminalv3.jar --creds-file creds.txt &

# Incremental fill: 7 existing symbols Dec 2025 → today
cd "/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/options-research"
.venv/bin/python scripts/chain_backfill.py \
    --ticker SPY QQQ AAPL NVDA META MSFT TSLA \
    --start 2025-12-13 --workers 4

# Full universe backfill (run overnight, multiple nights)
.venv/bin/python scripts/chain_backfill.py --workers 4 --resume
```
**Then ingest:**
```bash
cd market-lake
.venv/bin/python scripts/ingest/ingest_theta_option_eod.py \
    --input-dir "/path/to/options-research/cache/chain" \
    --output-dir canonical/facts/fact_option_eod
```
**Estimated time:** ~21 hours for full 513-symbol universe at 4 workers
**Expected output:** ~500M additional rows in `fact_option_eod`

### 1.2 Option Contract Dimension — All 513 Symbols [DEPENDS ON 1.1]
**Gap:** `dim_option_contract` only has 560K contracts from 5 symbols.
**Fix:** After chain backfill completes, run:
```bash
.venv/bin/python scripts/ingest/ingest_theta_contracts.py \
    --input-dir "/path/to/options-research/cache/chain" \
    --output-dir canonical/dimensions/dim_option_contract
```

### 1.3 Daily Automation via launchd [HIGH PRIORITY]
**Gap:** VRP gap-fill, ingest, and dbt rebuild run manually.
**Fix:** Create launchd plist for daily 6:45am run (after market open, before scan).
**Steps:**
1. Write `scripts/ops/daily_refresh.sh` (ThetaTerminal start → gap-fill → ingest → dbt)
2. Write `com.market-lake.daily-refresh.plist`
3. `launchctl load ~/Library/LaunchAgents/com.market-lake.daily-refresh.plist`

### 1.4 Corporate Actions Table ✅ DONE
**Gap:** No `fact_corporate_action` — splits and dividends not explicitly logged.
**Source:** yfinance (free, no API key)
**Script to write:** `scripts/ingest/ingest_corporate_actions.py`
```python
import yfinance as yf
ticker = yf.Ticker("AAPL")
actions = ticker.actions  # DataFrame with dividends + splits
```
**Schema:** `symbol, date, action_type (split|dividend), value, split_ratio`
**dbt model:** `stg_corporate_actions` → `fact_corporate_action`

### 1.5 CBOE VVIX and SKEW Index ✅ DONE
**Gap:** FRED 404s for `VVIXCLS` / `SKEWCLS`. CBOE publishes these directly.
**Source:** CBOE CDN (free, no API key)
**Fix:** Add to `ingest_fred_macro.py` or create `ingest_cboe_indices.py`:
```python
CBOE_URLS = {
    "VVIX": "https://cdn.cboe.com/api/global/us_indices/daily_prices/VVIX_History.csv",
    "SKEW": "https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv",
}
```

### 1.6 Fama-French Factors Monthly Refresh [LOW]
**Gap:** FF factors end Jan 30 2026 due to French library publication lag.
**Fix:** Add to monthly cron. Re-run `ingest_ff_factors.py --download` monthly.
**When available:** ~2 months after period end (March data → available ~May 2026)

---

## Phase 2 — Free Data Source Expansion

All sources below are free. Listed in order of research value for options premium selling.

### 2.1 CBOE Options Volume and Put/Call Ratio [HIGH]
**What:** Daily aggregate equity and index put/call ratios; total options volume.
Useful as a market sentiment / contrarian regime signal.
**Source:** CBOE Data Shop (free daily CSV)
- Equity P/C ratio: `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/equitypc.csv`
- Total P/C ratio: `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/totalpc.csv`
- Index P/C ratio: `https://www.cboe.com/publish/scheduledtask/mktdata/datahouse/indexpc.csv`
**Implementation:**
- Script: `scripts/ingest/ingest_cboe_options_flow.py`
- Table: `fact_macro_series` (add as additional series IDs: `CBOE_EQUITY_PC`, `CBOE_TOTAL_PC`)
- dbt: Flows automatically into `int_macro_series` → `mart_regime_panel`
**Effort:** 2-3 hours

### 2.2 VIX Futures Term Structure / VIX Curve [HIGH]
**What:** Daily VIX futures settlement prices (VX1–VX8). Gives the full VIX futures curve,
which is the most direct measure of vol term structure and contango/backwardation.
**Source:** CBOE Futures Exchange (free, requires registration)
- Historical: `https://cfe.cboe.com/cfe-products/vx-cboe-volatility-index-vix-futures/vx-historical-data`
- Or via yfinance: `yf.Ticker('^VIX').history()` covers VIX index (not futures)
- Quandl/Nasdaq free tier: `VIX/VIX_Futures`
**Implementation:**
- Script: `scripts/ingest/ingest_vix_futures.py`
- Table: `fact_vix_futures` — schema: `date, contract_month, settlement, dte`
- dbt: New mart column `vix_futures_slope`, `vix_contango_ratio`
**Effort:** 4-6 hours

### 2.3 FRED Additional Macro Series ✅ DONE (8 series added)
**What:** Several high-value series not yet in market-lake.
**Source:** FRED (free, no API key needed)

| Series ID | Description | Why useful |
|---|---|---|
| `M2SL` | M2 Money Supply | Liquidity signal |
| `T5YIE` | 5-Year Breakeven Inflation | Real rate context |
| `DFII10` | 10-Year Real Rate (TIPS) | Real cost of capital |
| `USEPUINDXD` | Economic Policy Uncertainty Index | Regime uncertainty |
| `DCOILWTICO` | WTI Crude Oil Price | Commodity regime |
| `GOLDAMGBD228NLBM` | Gold Price (London Fix) | Risk-off signal |
| `KCFSI` | KC Financial Stress Index | Credit stress |
| `STLFSI4` | St. Louis Financial Stress Index | Alternative stress |

**Implementation:** Add series IDs to `config/macros.yaml` and re-run `ingest_fred_macro.py`.
**Effort:** 30 minutes

### 2.4 Earnings Calendar [MEDIUM]
**What:** Expected earnings dates per symbol. Critical for avoiding binary event risk
in options premium selling — never sell premium into earnings without knowing.
**Source:** Multiple free options:
- `yfinance`: `ticker.calendar` (next earnings date)
- `nasdaq.com` earnings calendar (web scrape)
- `earningswhispers.com` (web scrape, more complete)
**Implementation:**
- Script: `scripts/ingest/ingest_earnings_calendar.py`
- Table: `fact_earnings_calendar` — schema: `symbol, earnings_date, period, eps_estimate, is_confirmed`
- dbt: Join to option panel — add `days_to_earnings` column to `mart_backtest_option_panel`
- Use case: Filter out any option position within 5 days of earnings
**Effort:** 4-6 hours (scraping complexity)

### 2.5 Short Interest [MEDIUM]
**What:** Bi-monthly short interest data per symbol (short shares, days to cover).
Useful for understanding positioning and squeeze risk.
**Source:** FINRA (free, bi-monthly)
- URL: `https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files`
**Implementation:**
- Script: `scripts/ingest/ingest_short_interest.py`
- Table: `fact_short_interest` — schema: `symbol, date, short_shares, float_shares, short_pct_float, days_to_cover`
**Effort:** 3-4 hours

### 2.6 Options Implied Move (Earnings Straddle Price) [MEDIUM]
**What:** Implied move into earnings = (ATM straddle price) / spot price.
Computable from existing option EOD data — no new source needed.
**Implementation:** dbt model `mart_earnings_implied_move.sql`
```sql
-- ATM straddle around earnings dates
-- = (nearest call + nearest put) / spot
-- Requires joining fact_option_eod to fact_earnings_calendar
```
**Effort:** 2-3 hours (pure dbt, no new ingestion)

### 2.7 Sector / Industry Classifications ✅ DONE
**What:** GICS sector and industry for each symbol. Useful for sector-relative screening.
**Source:** Multiple free options:
- yfinance: `ticker.info['sector']`, `ticker.info['industry']`
- Wikipedia GICS tables (scrape)
- `config/symbols.yaml` extension (manual for 531 symbols)
**Implementation:**
- Extend `dim_symbol` with `sector`, `industry`, `market_cap_bucket` columns
- Script: `scripts/build/enrich_dim_symbol.py`
**Effort:** 2-3 hours

---

## Phase 3 — Fundamentals & Financial Analysis (FinanceToolkit)

> **Decision:** Use FinanceToolkit's computation library (open-source, MIT) on top of
> raw financial statements from yfinance (free). Do NOT use FinanceToolkit's data
> fetching layer — yfinance is the data source, FinanceToolkit provides the formulas.
>
> **Why this matters for premium selling:** Piotroski Score + Altman Z-Score filter out
> financially distressed companies. Combined with high IVR, this is the difference between
> a systematic edge and selling puts into bankruptcies.

### 3.1 Raw Financial Statements — yfinance ✅ DONE (2,375 rows, 502 symbols)
**What:** Income statement, balance sheet, cash flow for all 531 symbols.
**Source:** yfinance (`ticker.income_stmt`, `ticker.balance_sheet`, `ticker.cashflow`)
**Coverage:** 4 years annual, all US-listed equities (ETFs excluded from ratio calc)
**Script to write:** `scripts/ingest/ingest_fundamentals.py`

**Processing:** One symbol at a time, `gc.collect()` after each, polite delay.
**Output table:** `fact_financial_statements` — raw GAAP line items per symbol per fiscal year.

**Key columns ingested:**

| Column | Source Statement | Description |
|---|---|---|
| `symbol`, `symbol_id`, `fiscal_year_end`, `period_type` | derived | Identity |
| `revenue`, `gross_profit`, `ebit`, `net_income`, `eps_diluted` | Income | P&L |
| `total_assets`, `total_debt`, `total_equity`, `cash_and_equivalents` | Balance | Position |
| `current_assets`, `current_liabilities` | Balance | Liquidity inputs |
| `operating_cash_flow`, `capex`, `free_cash_flow` | Cash Flow | Cash generation |

**Effort:** 4-6 hours

---

### 3.2 Compute Financial Ratios — FinanceToolkit Formulas ✅ DONE
**What:** Implement FinanceToolkit's transparent formulas directly to compute key ratios.
**Install:** `pip install financetoolkit` (use as formula reference, not data fetcher)

**Key ratios computed:**

| Ratio | Why it matters for premium selling |
|---|---|
| `piotroski_score` (0-9) | Hard quality filter — avoid selling puts on deteriorating companies |
| `altman_z_score` | Bankruptcy predictor — never sell puts when Z < 1.81 |
| `debt_to_equity` | Leverage filter — high debt = higher tail risk |
| `current_ratio` | Liquidity — distressed companies fail here first |
| `fcf_yield` | Value signal — positive FCF means self-funding |
| `fcf_margin` | Cash generation quality |
| `roe`, `roa` | Return quality signals |
| `gross_margin` | Business quality proxy |
| `earnings_quality` | CFO/net_income — catches accruals manipulation |
| `revenue_growth_yoy` | Momentum quality |

**Output table:** `fact_fundamentals_annual` — one row per symbol per fiscal year with all ratios.
**Effort:** 2-3 hours

---

### 3.3 dbt Models — Fundamental Screening Layer [FREE]
**New models:**

```
staging/
  stg_fundamentals_annual.sql     ← reads canonical/facts/fact_fundamentals_annual

marts/
  mart_fundamental_screen.sql     ← joins fundamentals to screening panel
```

**`mart_fundamental_screen.sql` — position sizing tier logic:**
```sql
-- Maps to existing FULL SIZE / HALF SIZE / QUARTER SIZE / SKIP convention
CASE
    WHEN piotroski_score >= 7 AND altman_z_score > 2.99 THEN 'FULL SIZE'
    WHEN piotroski_score >= 5 AND altman_z_score > 1.81 THEN 'HALF SIZE'
    WHEN piotroski_score >= 3                            THEN 'QUARTER SIZE'
    ELSE                                                      'SKIP'
END AS fundamental_tier
```

**Effort:** 3-4 hours

---

### 3.4 Composite Scanner — VRP + Fundamentals [FREE]
**The finished daily query:**

```sql
SELECT
    f.symbol, f.ivr_252d, f.vrp_rank,
    f.piotroski_score, f.altman_z_score, f.debt_to_equity,
    f.fcf_yield, f.fundamental_tier,
    v.iv_30d, v.put_skew_25d, v.ts_slope_30_60
FROM mart_fundamental_screen f
JOIN mart_screening_panel_latest v USING (symbol)
WHERE f.ivr_252d > 0.60            -- elevated IV rank
  AND f.altman_z_score > 2.99      -- safe zone (not distressed)
  AND f.piotroski_score >= 5        -- financially healthy
  AND f.debt_to_equity  < 2.0      -- manageable leverage
ORDER BY f.ivr_252d DESC
LIMIT 25
```

**Effort:** 1-2 hours (pure SQL)

---

### 3.5 Known Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| **4-year history only (free)** | No long-term fundamental backtests | Fine for live screening |
| **Annual data only** | Miss quarterly deterioration | Add FMP quarterly if needed (see 3.6) |
| **ETFs have no fundamentals** | ~200 of 531 symbols excluded | Filter by `asset_type = 'stock'` |
| **Reporting lag 2-6 weeks** | Statements not instant | Use `fiscal_year_end + 45d` as earliest valid date |
| **No point-in-time data** | Look-ahead bias possible in backtests | Document; accept for live screening |
| **Restatements not tracked** | Historical ratios may shift | Use current as-reported values only |

### 3.6 Upgrade Path — Financial Modeling Prep (FMP)
When you want quarterly data or 30-year history:
- **FMP Starter ~$14/month** — quarterly statements, 5 years, 300 req/min
- **FMP Professional ~$40/month** — 30 years, all endpoints, 750 req/min
- Design `ingest_fundamentals.py` with FMP as a drop-in alternative to yfinance

**Total Phase 3 effort: ~12-15 hours, entirely free (yfinance + FinanceToolkit open-source)**

---

## Phase 4 — Paid / Subscription Data Expansion

### 4.1 ThetaData — Full Historical Backfill [HIGHEST IMPACT, PAID]
Already have OPTION.STANDARD subscription. See Phase 1.1 above.

### 4.2 ThetaData — ORATS IV Surface [PAID, FUTURE]
**What:** More complete implied vol surface with model-calibrated greeks.
**Cost:** Additional ThetaData subscription tier.
**When:** After Phase 1 and 2 complete.

### 4.3 Refinitiv / LSEG Eikon [PAID, FUTURE]
**What:** Fundamentals, earnings revisions, analyst ratings.
**Cost:** Enterprise subscription.
**When:** If fundamental factor research becomes a priority.

---

## Sustainability Principles

Every new data source added to market-lake follows these rules:

1. **One script per source.** `scripts/ingest/ingest_<source>.py`. Never add source logic to dbt.
2. **Memory-safe ingestion.** Process one file/symbol at a time. `gc.collect()` after each.
3. **Manifest every write.** All writes append to `fact_dataset_manifest` with row count, schema hash, date range.
4. **Idempotent.** Running the same ingest twice produces the same `ingest_batch_id`. No duplicate data.
5. **Partition correctly.** Facts partitioned by `year` or `year+month`. Dimensions flat.
6. **Validate before writing.** All data passes through `validation/` before touching canonical.
7. **dbt models for all transforms.** No Python transforms in ingest scripts beyond normalization.
8. **Document the source.** Add to `config/sources.yaml` and `docs/data_sources.md`.

---

## Integration Pattern for New Sources

```
1. Add source to config/sources.yaml
2. Write scripts/ingest/ingest_<source>.py
   - read_raw() → normalize() → validate() → write_parquet() → write_manifest()
   - Process one file/batch at a time (memory-safe)
3. Write dbt/models/staging/stg_<source>.sql
   - read_parquet() with union_by_name=true
   - Placeholder parquet in canonical/ prevents glob errors when empty
4. Wire into existing intermediate/mart models (or create new ones)
5. Add to config/datasets.yaml
6. Update docs/data_dictionary.md with new columns
7. Add tests in tests/
8. Update STATUS.md and CHANGELOG.md
```
