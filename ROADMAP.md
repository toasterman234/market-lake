# ROADMAP.md
# market-lake — Roadmap

> Priority order reflects research value, implementation complexity, and data availability.
> All free sources are flagged. Paid sources require existing subscriptions noted.

---

## Phase 1 — Operational Completeness (Current Sprint)

These are gaps in data we already have infrastructure for.

### 1.1 Option Chain Full Backfill [HIGH PRIORITY]
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

### 1.4 Corporate Actions Table [MEDIUM]
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

### 1.5 CBOE VVIX and SKEW Index [LOW]
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

### 2.3 FRED Additional Macro Series [MEDIUM]
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

### 2.7 Sector / Industry Classifications [LOW]
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

## Phase 3 — Paid / Subscription Data Expansion

### 3.1 ThetaData — Full Historical Backfill [HIGHEST IMPACT, PAID]
Already have OPTION.STANDARD subscription. See Phase 1.1 above.

### 3.2 ThetaData — ORATS IV Surface [PAID, FUTURE]
**What:** More complete implied vol surface with model-calibrated greeks.
**Cost:** Additional ThetaData subscription tier.
**When:** After Phase 1 and 2 complete.

### 3.3 Refinitiv / LSEG Eikon [PAID, FUTURE]
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
