"""
test_canonical_tables.py
=========================
Integration tests that verify canonical parquet tables exist, have data,
and meet basic quality requirements.

These tests read directly from the parquet files — no DuckDB connection needed.
Skips tables that haven't been ingested yet (graceful degradation).
"""
import pytest
import duckdb
from pathlib import Path
import os

ROOT = Path(os.environ.get("MARKET_LAKE_ROOT", ".")).resolve()


def query(sql: str):
    """Run a DuckDB query against local parquets (in-memory)."""
    db = duckdb.connect(":memory:")
    return db.execute(sql).df()


def table_exists(rel_path: str) -> bool:
    p = ROOT / rel_path
    return p.exists() and any(p.rglob("*.parquet"))


def skip_if_missing(rel_path: str):
    return pytest.mark.skipif(
        not table_exists(rel_path),
        reason=f"{rel_path} not yet ingested"
    )


# ── fact_short_interest ──────────────────────────────────────────────────────

@skip_if_missing("canonical/facts/fact_short_interest")
class TestShortInterest:
    PATH = ROOT / "canonical/facts/fact_short_interest"

    def test_has_rows(self):
        r = query(f"SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)").iloc[0, 0]
        assert r > 1_000_000, f"Expected >1M rows, got {r:,}"

    def test_no_null_keys(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE symbol IS NULL OR settle_date IS NULL
        """).iloc[0, 0]
        assert r == 0, f"Found {r} rows with null symbol or settle_date"

    def test_no_negative_short_shares(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE short_shares < 0
        """).iloc[0, 0]
        assert r == 0, f"Found {r} rows with negative short_shares"

    def test_date_range(self):
        r = query(f"""
            SELECT MIN(settle_date)::VARCHAR AS min_d, MAX(settle_date)::VARCHAR AS max_d
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
        """).iloc[0]
        assert r["min_d"] < "2021-01-01", f"Data should start before 2021, got {r['min_d']}"
        assert r["max_d"] > "2025-01-01", f"Data should extend past 2025, got {r['max_d']}"

    def test_symbol_count(self):
        r = query(f"""
            SELECT COUNT(DISTINCT symbol) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
        """).iloc[0, 0]
        assert r > 5000, f"Expected >5000 symbols, got {r}"


# ── fact_earnings_calendar ───────────────────────────────────────────────────

@skip_if_missing("canonical/facts/fact_earnings_calendar")
class TestEarningsCalendar:
    PATH = ROOT / "canonical/facts/fact_earnings_calendar"

    def test_has_rows(self):
        r = query(f"SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)").iloc[0, 0]
        assert r > 10_000, f"Expected >10K rows, got {r:,}"

    def test_no_null_keys(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE symbol IS NULL OR earnings_date IS NULL
        """).iloc[0, 0]
        assert r == 0

    def test_no_duplicates(self):
        dups = query(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, earnings_date, COUNT(*) AS n
                FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
                GROUP BY symbol, earnings_date HAVING n > 1
            )
        """).iloc[0, 0]
        assert dups == 0, f"Found {dups} duplicate (symbol, earnings_date) pairs"

    def test_has_future_dates(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE earnings_date > CURRENT_DATE
        """).iloc[0, 0]
        assert r > 100, f"Expected future earnings events, got {r}"

    def test_symbol_count(self):
        r = query(f"""
            SELECT COUNT(DISTINCT symbol) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
        """).iloc[0, 0]
        assert r > 400


# ── fact_corporate_action ────────────────────────────────────────────────────

@skip_if_missing("canonical/facts/fact_corporate_action")
class TestCorporateActions:
    PATH = ROOT / "canonical/facts/fact_corporate_action"

    def test_has_rows(self):
        r = query(f"SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)").iloc[0, 0]
        assert r > 50_000

    def test_valid_action_types(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE action_type NOT IN ('dividend', 'split')
        """).iloc[0, 0]
        assert r == 0, f"Found {r} rows with invalid action_type"

    def test_positive_values(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE value IS NULL OR value <= 0
        """).iloc[0, 0]
        assert r == 0

    def test_no_null_keys(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE symbol IS NULL OR action_date IS NULL OR action_type IS NULL
        """).iloc[0, 0]
        assert r == 0


# ── fact_fundamentals_annual ─────────────────────────────────────────────────

@skip_if_missing("canonical/facts/fact_fundamentals_annual")
class TestFundamentalsAnnual:
    PATH = ROOT / "canonical/facts/fact_fundamentals_annual"

    def test_has_rows(self):
        r = query(f"SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)").iloc[0, 0]
        assert r > 2000

    def test_no_null_keys(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE symbol IS NULL OR fiscal_year_end IS NULL
        """).iloc[0, 0]
        assert r == 0

    def test_no_duplicate_symbol_fy(self):
        dups = query(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, fiscal_year_end, COUNT(*) AS n
                FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
                GROUP BY symbol, fiscal_year_end HAVING n > 1
            )
        """).iloc[0, 0]
        assert dups == 0

    def test_piotroski_score_range(self):
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE piotroski_score IS NOT NULL
              AND (piotroski_score < 0 OR piotroski_score > 9)
        """).iloc[0, 0]
        assert r == 0, f"Found {r} Piotroski scores outside [0,9]"

    def test_piotroski_is_integer(self):
        dtype = query(f"""
            SELECT typeof(piotroski_score) AS t
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE piotroski_score IS NOT NULL LIMIT 1
        """).iloc[0, 0]
        assert dtype in ("INTEGER", "INT", "INT32"), f"Expected INTEGER, got {dtype}"

    def test_fiscal_year_end_is_date(self):
        dtype = query(f"""
            SELECT typeof(fiscal_year_end) AS t
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            LIMIT 1
        """).iloc[0, 0]
        assert "DATE" in dtype.upper(), f"Expected DATE, got {dtype}"

    def test_ratios_in_sane_range(self):
        """Gross margin should be between -1 and 1 for most companies."""
        extreme = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE gross_margin IS NOT NULL
              AND (gross_margin < -2.0 OR gross_margin > 2.0)
        """).iloc[0, 0]
        assert extreme < 50, f"Too many extreme gross_margin values: {extreme}"


# ── fact_macro_series — new CBOE vol indices ─────────────────────────────────

@skip_if_missing("canonical/facts/fact_macro_series")
class TestMacroSeriesCompleteness:
    PATH = ROOT / "canonical/facts/fact_macro_series"

    REQUIRED_SERIES = [
        "VIXCLS", "VIX3M", "VVIX", "SKEW",
        "DGS10", "DGS2", "FEDFUNDS",
        "M2SL", "DCOILWTICO", "USEPUINDXD",
        "VIX9D", "VXD", "VXN", "VXEEM",
    ]

    def test_all_required_series_present(self):
        present = set(query(f"""
            SELECT DISTINCT series_id
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
        """)["series_id"].tolist())
        missing = [s for s in self.REQUIRED_SERIES if s not in present]
        assert not missing, f"Missing series: {missing}"

    def test_no_duplicate_series_date(self):
        dups = query(f"""
            SELECT COUNT(*) FROM (
                SELECT series_id, date, COUNT(*) AS n
                FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
                GROUP BY series_id, date HAVING n > 1
            )
        """).iloc[0, 0]
        assert dups == 0, f"Found {dups} duplicate (series_id, date) pairs"

    def test_no_null_keys(self):
        # series_id and date must never be null
        # value CAN be null on weekends/holidays (FRED skips non-business days)
        # int_macro_series forward-fills these nulls to trading days
        r = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE series_id IS NULL OR date IS NULL
        """).iloc[0, 0]
        assert r == 0
