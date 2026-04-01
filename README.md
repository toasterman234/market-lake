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
| Macro series | 100K rows — 21 series — 1947 → Mar 2026 |
| FF factors | 26K rows — 7 factors — 1926 → Jan 2026 |

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
  dimensions/               ← dim_symbol, dim_option_contract, dim_calendar
  facts/                    ← fact_underlying_bar_daily, fact_option_eod,
  │                            fact_macro_series, fact_ff_factors_daily
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
python scripts/build/build_dim_calendar.py
python scripts/ingest/ingest_fred_macro.py
python scripts/ingest/ingest_yahoo_daily_bars.py --symbols SPY QQQ --start 2020-01-01 --end 2026-03-31

cd dbt && dbt run
python examples/query_examples.py
```

---

## Data Sources

| Source | Data | Cost | Status |
|---|---|---|---|
| ThetaData | Options EOD, VRP features | Paid (OPTION.STANDARD) | ✅ Active |
| Yahoo Finance | Equity OHLCV | Free | ✅ Working |
| FRED | Macro series (rates, VIX, CPI, etc.) | Free | ✅ Working |
| Kenneth French | FF5 + momentum factors | Free | ✅ Working |
| CBOE | VIX3M, GVZ, OVX | Free (via FRED) | ✅ Partial |
| Stooq | Equity OHLCV (cross-validation) | Free | ❌ Blocked |

**→ See [docs/data_sources.md](docs/data_sources.md) for full source reference and planned additions.**

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
