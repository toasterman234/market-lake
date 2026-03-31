# Contributing to market-lake

Thank you for your interest in contributing. This document explains how the project is structured and how to contribute effectively.

---

## Development Setup

```bash
git clone https://github.com/toasterman234/market-lake.git
cd market-lake
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
# Edit .env — set MARKET_LAKE_ROOT
python scripts/build/bootstrap_duckdb.py
```

---

## Project Structure

```
src/market_lake/       Python package (ingest, io, ids, validation)
scripts/build/         One-time setup scripts (bootstrap, dim builds)
scripts/ingest/        Data ingestion scripts (one per source)
dbt/models/            dbt SQL models (staging → intermediate → marts)
config/                YAML configuration (symbols, macros, datasets)
tests/                 pytest unit tests
```

---

## Conventions

### Adding a new data source

1. Add source entry to `config/sources.yaml`
2. Write `scripts/ingest/ingest_<source>.py` following existing patterns:
   - Accept `--input-dir` / `--output-dir` / `--manifest-dir` args
   - Validate before writing (`validation/`)
   - Write a `ManifestRecord` after every successful ingest
   - Use `stable_symbol_id()` for all symbol IDs
3. Add a `dbt/models/staging/stg_<source>.sql` model
4. Register the dataset in `config/datasets.yaml`
5. Add tests in `tests/`

### Adding a new mart

1. Create `dbt/models/marts/mart_<name>.sql`
2. Add a DuckDB view in `duckdb/init/003_views.sql` if needed
3. Re-run `python scripts/build/bootstrap_duckdb.py` and `dbt run`

### Python style

- Black-compatible formatting (line length 100 via ruff)
- Type hints on all public functions
- No hardcoded paths — use `Settings.load()`
- Validation functions return `list[str]` of error messages (non-throwing)

---

## Tests

```bash
pytest tests/ -v
```

All new ingest scripts and utility functions should have unit tests.
Validation functions are especially important to test.

---

## Commit style

```
feat: add Stooq ingest script
fix: handle missing adj_close in Yahoo bars
docs: add CONTRIBUTING guide
refactor: extract normalize_bar() helper
test: add validation tests for option EOD
```

---

## Data Licensing Notes

- **FRED**: Free for research use. Do not redistribute raw series files.
- **ThetaData**: Commercial license required. Do not commit raw data files.
- **Yahoo Finance / Stooq**: Free for personal research. Review their terms before commercial use.

Raw data files must never be committed to the repo. The `.gitignore` excludes `canonical/`, `raw/`, and `duckdb/*.duckdb`.
