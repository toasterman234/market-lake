# market-lake

> A local-first historical market data lakehouse for quant research, backtesting, screening, and portfolio optimization.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![dbt](https://img.shields.io/badge/dbt-duckdb-orange.svg)](https://docs.getdbt.com/)

---

## What It Is

market-lake is a **local-first research lakehouse** — a unified, queryable database that consolidates historical market data from multiple sources into a single clean architecture built on Parquet files + DuckDB + dbt.

It is designed for quantitative researchers, options traders, and systematic investors who need:
- A single source of truth for historical equity, options, and macro data
- Fast ad-hoc SQL queries via DuckDB
- Reproducible, validated data pipelines
- Research-ready marts for backtesting, screening, and optimization

---

## Stack

| Layer | Technology |
|---|---|
| Storage | Apache Parquet (Hive-partitioned) |
| Query engine | DuckDB |
| Transform layer | dbt-duckdb |
| Ingestion + validation | Python 3.11+ |
| Orchestration | Shell scripts / launchd (local) |

---

## Architecture

```
raw/                        ← immutable vendor drops (never mutated)
canonical/
  dimensions/               ← dim_symbol, dim_option_contract, dim_calendar
  facts/                    ← fact_underlying_bar_daily, fact_option_eod,
  │                            fact_macro_series
  features/                 ← fact_option_feature_daily (VRP/IV/HV/skew)
  metadata/                 ← fact_dataset_manifest (ingest audit trail)
duckdb/market.duckdb        ← query layer: views onto all canonical parquet
dbt/                        ← staging → intermediate → mart transforms
src/market_lake/            ← Python package: ingest, validate, ids, io
scripts/                    ← runnable ingest + build scripts
config/                     ← symbols.yaml, macros.yaml, datasets.yaml
```

### Data flow

```
Vendor APIs / files
      │
      ▼  ingest_*.py + validation
  raw/ → canonical/ parquet datasets
                │
                ▼  dbt models
          marts/ (backtest panel, regime, optimizer, screening)
                │
                ▼  DuckDB views
  Research notebooks / backtester / optimizer
```

---

## Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/toasterman234/market-lake.git
cd market-lake
```

### 2. Set up Python environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — set MARKET_LAKE_ROOT to the absolute path of this repo
```

### 4. Bootstrap DuckDB

```bash
python scripts/build/bootstrap_duckdb.py
```

### 5. Build dimensions

```bash
python scripts/build/build_dim_symbol.py
python scripts/build/build_dim_calendar.py
```

### 6. Ingest data

```bash
# Yahoo Finance daily bars
python scripts/ingest/ingest_yahoo_daily_bars.py \
    --symbols SPY QQQ IWM TLT GLD \
    --start 2005-01-01 --end 2026-03-31

# Stooq free historical bars
python scripts/ingest/ingest_stooq_daily_bars.py \
    --symbols SPY QQQ AAPL --start 2005-01-01 --end 2026-03-31

# FRED macro series (from config/macros.yaml)
python scripts/ingest/ingest_fred_macro.py

# ThetaData VRP features (from existing parquet files)
python scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir /path/to/vrp_clean \
    --output-dir canonical/features/fact_option_feature_daily
```

### 7. Run dbt transforms

```bash
pip install "dbt-duckdb>=1.8.0"
cp dbt/profiles.yml.example dbt/profiles.yml
# Edit dbt/profiles.yml — set the DuckDB path

cd dbt
dbt run
dbt test
```

### 8. Query

```python
from market_lake.io.duckdb_conn import open_db
from market_lake.settings import Settings

with open_db(Settings.load().duckdb_path) as con:
    df = con.execute("""
        SELECT symbol, date, iv_30d, ivr_252d, vrp_30d
        FROM features.vw_option_features_daily
        WHERE date = (SELECT MAX(date) FROM features.vw_option_features_daily)
        ORDER BY ivr_252d DESC LIMIT 20
    """).df()
print(df)
```

---

## Data Sources

| Source | Data Type | Cost | Script |
|---|---|---|---|
| ThetaData | Options EOD, Greeks, VRP features | Paid subscription | `ingest_theta_vrp_features.py` |
| Yahoo Finance (yfinance) | Equity/ETF daily OHLCV | Free | `ingest_yahoo_daily_bars.py` |
| Stooq | Equity/ETF daily OHLCV | Free | `ingest_stooq_daily_bars.py` |
| FRED | Macro series (rates, VIX, CPI, etc.) | Free | `ingest_fred_macro.py` |

---

## dbt Models

| Model | Layer | Description |
|---|---|---|
| `stg_yahoo_daily_bars` | staging | Yahoo OHLCV bars |
| `stg_stooq_daily_bars` | staging | Stooq OHLCV bars |
| `stg_theta_contracts` | staging | Option contract dimension |
| `stg_theta_option_eod` | staging | Option EOD quotes + Greeks |
| `stg_theta_vrp_features` | staging | IV, HV, VRP, skew features |
| `stg_fred_macro` | staging | FRED macro series |
| `int_underlying_bars_daily` | intermediate | All-source deduped bar panel |
| `int_option_eod` | intermediate | EOD enriched with contract dim + DTE |
| `int_macro_series` | intermediate | Macro forward-filled to daily |
| `mart_backtest_equity_panel` | mart | Returns, vol, momentum per symbol/date |
| `mart_backtest_option_panel` | mart | Option panel with underlying + VRP |
| `mart_regime_panel` | mart | SPY + macro regime labels per date |
| `mart_optimization_inputs` | mart | Optimizer-ready signal panel |
| `mart_screening_panel` | mart | Cross-sectional scanner surface |

---

## Canonical Tables

| Table | Grain | Partition |
|---|---|---|
| `dim_symbol` | 1 row per symbol | — |
| `dim_option_contract` | 1 row per contract | — |
| `dim_calendar` | 1 row per weekday | — |
| `fact_underlying_bar_daily` | 1 row per symbol × date × source | year |
| `fact_option_eod` | 1 row per contract × date | underlying, year, month |
| `fact_option_feature_daily` | 1 row per symbol × date | year, month |
| `fact_macro_series` | 1 row per series × date | series_id |
| `fact_dataset_manifest` | 1 row per ingest batch | — |

---

## Development

```bash
# Run tests
pytest tests/ -v

# Lint
ruff check src/ scripts/ tests/

# Type check
mypy src/market_lake/

# Re-run full dbt pipeline
cd dbt && dbt run --full-refresh
```

---

## Design Principles

1. **Raw is immutable** — never modify files under `raw/`
2. **Stable IDs** — `symbol_id` is a deterministic SHA-256 hash; `contract_id` = `UNDERLYING|EXPIRY|STRIKE|TYPE`
3. **Layered** — raw → canonical → marts → query layer
4. **Validation first** — every ingest script validates before writing
5. **Manifest tracking** — every write records provenance in `fact_dataset_manifest`
6. **Local-first** — no cloud infra, no containers required

---

## License

MIT — see [LICENSE](LICENSE)

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
