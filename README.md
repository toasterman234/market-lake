# market-lake

> A local-first historical market data lakehouse for quant research, backtesting, screening, and portfolio optimization.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![dbt](https://img.shields.io/badge/dbt-duckdb-orange.svg)](https://docs.getdbt.com/)

---

## Current Status

| Metric | Value |
|---|---|
| pytest | ✅ 36 / 36 passing |
| dbt test | ✅ 28 / 28 passing |
| dbt run | ✅ 15 / 15 models clean |
| Equity bars | 2.58M rows — 531 symbols — 2005 → Mar 2026 |
| VRP features | 2.24M rows — 513 symbols — 2017 → Mar 2026 |
| Option EOD | 207M rows — 7 symbols — 2008 → Mar 2026 |
| Macro series | 165K rows — **24 series** — 1947 → Mar 2026 |
| FF factors | 26K rows — 7 factors — 1926 → Jan 2026 |
| **Fundamentals** | **2,375 rows — 502 equity symbols — annual ratios** |
| **Corporate actions** | **57,422 rows — 503 symbols — dividends + splits** |
| **dim_symbol** | **531 symbols — with sector + industry** |

**→ See [STATUS.md](STATUS.md) for full data inventory, known issues, and work-in-progress.**
**→ See [ROADMAP.md](ROADMAP.md) for planned additions and new data sources.**

---

## Stack

| Layer | Technology |
|---|---|
| Storage | Apache Parquet (Hive-partitioned) |
| Query engine | DuckDB |
| Transform layer | dbt-duckdb |
| Ingestion | Python 3.11+ |

---

## Architecture

```
raw/                        ← immutable vendor drops
canonical/
  dimensions/               ← dim_symbol (w/ sector/industry), dim_option_contract, dim_calendar
  facts/                    ← fact_underlying_bar_daily, fact_option_eod,
  │                            fact_macro_series, fact_ff_factors_daily,
  │                            fact_corporate_action, fact_financial_statements
  features/                 ← fact_option_feature_daily (VRP/IV/HV/skew)
  metadata/                 ← fact_dataset_manifest (audit trail)
duckdb/market.duckdb        ← DuckDB views onto canonical parquet
dbt/                        ← 15 models: staging → intermediate → marts
src/market_lake/            ← Python package: ingest, io, ids, validation
scripts/                    ← runnable ingest + build scripts
config/                     ← symbols.yaml, macros.yaml, sources.yaml, datasets.yaml
```

---

## Quick Start

```bash
git clone https://github.com/toasterman234/market-lake.git
cd market-lake
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env           # set MARKET_LAKE_ROOT

python scripts/build/bootstrap_duckdb.py
python scripts/build/build_dim_symbol.py
python scripts/build/enrich_dim_symbol.py   # adds sector + industry via yfinance
python scripts/build/build_dim_calendar.py
python scripts/ingest/ingest_fred_macro.py
python scripts/ingest/ingest_yahoo_daily_bars.py --symbols SPY QQQ --start 2020-01-01 --end 2026-03-31
python scripts/ingest/ingest_fundamentals.py   # annual financial statements + ratios

cd dbt && dbt run
python examples/query_examples.py
```

---

## Data Sources

| Source | Data | Cost | Status |
|---|---|---|---|
| ThetaData | Options EOD, VRP features | Paid (OPTION.STANDARD) | ✅ Active |
| Yahoo Finance | Equity OHLCV + financial statements + corporate actions | Free | ✅ Working |
| FRED | 24 macro series (rates, vol, inflation, oil, stress) | Free | ✅ Working |
| Kenneth French | FF5 + momentum factors | Free | ✅ Working |
| CBOE CDN | VVIX, SKEW (direct download) | Free | ✅ Working |
| FinanceToolkit | Formula library for ratio computation | Free (MIT) | ✅ Used internally |
| Stooq | Equity OHLCV (cross-validation) | Free | ❌ Blocked |

**→ See [docs/data_sources.md](docs/data_sources.md) for full source reference and planned additions.**

---

## Ingest Scripts

| Script | Source | Output |
|---|---|---|
| `ingest_yahoo_daily_bars.py` | Yahoo Finance | `fact_underlying_bar_daily` |
| `ingest_existing_equity.py` | Cached parquets | `fact_underlying_bar_daily` |
| `ingest_theta_vrp_features.py` | ThetaData | `fact_option_feature_daily` |
| `ingest_theta_option_eod.py` | ThetaData | `fact_option_eod` |
| `ingest_theta_contracts.py` | ThetaData | `dim_option_contract` |
| `ingest_fred_macro.py` | FRED | `fact_macro_series` |
| `ingest_ff_factors.py` | French Library | `fact_ff_factors_daily` |
| `ingest_fundamentals.py` | yfinance | `fact_financial_statements`, `fact_fundamentals_annual` |
| `ingest_corporate_actions.py` | yfinance | `fact_corporate_action` |
| `ingest_stooq_daily_bars.py` | Stooq | `fact_underlying_bar_daily` ⚠️ blocked |
| `build_dim_symbol.py` | config | `dim_symbol` |
| `enrich_dim_symbol.py` | yfinance | `dim_symbol` (sector + industry) |
| `build_dim_calendar.py` | generated | `dim_calendar` |

---

## dbt Models (15 total)

**Staging (7):** `stg_yahoo_daily_bars`, `stg_stooq_daily_bars`, `stg_theta_contracts`,
`stg_theta_option_eod`, `stg_theta_vrp_features`, `stg_fred_macro`, `stg_ff_factors`

**Intermediate (3):** `int_underlying_bars_daily`, `int_option_eod`, `int_macro_series`

**Marts (5):** `mart_backtest_equity_panel`, `mart_backtest_option_panel`,
`mart_regime_panel`, `mart_optimization_inputs`, `mart_screening_panel`

---

## Documentation

| Doc | Description |
|---|---|
| [STATUS.md](STATUS.md) | Current data state, test results, what works / doesn't |
| [ROADMAP.md](ROADMAP.md) | Planned data additions, free sources, integration plan |
| [docs/architecture.md](docs/architecture.md) | System design, layer definitions, stable IDs |
| [docs/data_dictionary.md](docs/data_dictionary.md) | Column-level schema for all tables |
| [docs/data_sources.md](docs/data_sources.md) | All data sources — current, planned, evaluated |
| [docs/ingestion_guide.md](docs/ingestion_guide.md) | How to run each ingest script |
| [docs/query_guide.md](docs/query_guide.md) | DuckDB query examples |
| [CHANGELOG.md](CHANGELOG.md) | Version history |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development setup and conventions |

---

## License

MIT — see [LICENSE](LICENSE)
