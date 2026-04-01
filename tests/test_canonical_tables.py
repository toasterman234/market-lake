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


# ── fact_option_feature_daily — VRP data quality ─────────────────────────────

@skip_if_missing("canonical/features/fact_option_feature_daily")
class TestVRPFeatures:
    PATH = ROOT / "canonical/features/fact_option_feature_daily"

    def test_no_duplicates(self):
        dups = query(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, date, COUNT(*) n
                FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
                GROUP BY symbol, date HAVING n > 1
            )
        """).iloc[0, 0]
        assert dups == 0, f"Found {dups:,} duplicate (symbol, date) keys"

    def test_iv_positive(self):
        """Raw parquets preserve original ThetaData values.
        iv_30d may exceed 3.0 for very illiquid options (ThetaData artifact);
        stg_theta_vrp_features caps at 3.0 via LEAST(iv_30d, 3.0).
        We only enforce that present values are strictly positive."""
        bad = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE iv_30d IS NOT NULL AND iv_30d <= 0
        """).iloc[0, 0]
        assert bad == 0, f"{bad} rows with IV <= 0"

    def test_iv_artifact_count_acceptable(self):
        """Track count of extreme IV artifacts (> 3.0 = 300%). Should be < 1% of rows."""
        total = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE iv_30d IS NOT NULL
        """).iloc[0, 0]
        extreme = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE iv_30d > 3.0
        """).iloc[0, 0]
        pct = extreme / total * 100 if total > 0 else 0
        assert pct < 1.0, f"Too many extreme IV artifacts: {extreme:,} ({pct:.2f}%)"

    def test_ivr_bounds(self):
        bad = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE ivr_252d IS NOT NULL AND (ivr_252d < 0 OR ivr_252d > 1.0)
        """).iloc[0, 0]
        assert bad == 0, f"{bad} rows with IVR outside [0, 1]"

    def test_symbol_count(self):
        n = query(f"""
            SELECT COUNT(DISTINCT symbol)
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
        """).iloc[0, 0]
        assert n >= 500, f"Expected >= 500 VRP symbols, got {n}"

    def test_date_is_date_type(self):
        dtype = query(f"""
            SELECT typeof(date) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true) LIMIT 1
        """).iloc[0, 0]
        # Can be DATE or TIMESTAMP — both acceptable
        assert "DATE" in dtype.upper() or "TIMESTAMP" in dtype.upper()


# ── fact_underlying_bar_daily — single file per partition ─────────────────────

@skip_if_missing("canonical/facts/fact_underlying_bar_daily")
class TestEquityBarsPartitions:
    PATH = ROOT / "canonical/facts/fact_underlying_bar_daily"

    def test_no_duplicate_symbol_date(self):
        """After int dedup, no (symbol, date) duplicates should reach marts."""
        # Note: raw parquets MAY have multi-source duplicates (handled by int layer)
        # We just verify the year=2026 partition (most recent) is clean
        p2026 = self.PATH / "year=2026"
        if not p2026.exists():
            return
        dups = query(f"""
            SELECT COUNT(*) FROM (
                SELECT symbol, date, COUNT(*) n
                FROM read_parquet('{p2026}/*.parquet', union_by_name=true)
                GROUP BY symbol, date HAVING n > 1
            )
        """).iloc[0, 0]
        assert dups == 0, f"{dups} duplicate (symbol,date) in year=2026"

    def test_positive_prices(self):
        bad = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE close <= 0 OR high <= 0 OR low <= 0
        """).iloc[0, 0]
        assert bad == 0

    def test_high_gte_low(self):
        bad = query(f"""
            SELECT COUNT(*) FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE high < low
        """).iloc[0, 0]
        assert bad == 0

    def test_date_range_spx(self):
        r = query(f"""
            SELECT MIN(date)::VARCHAR, MAX(date)::VARCHAR
            FROM read_parquet('{self.PATH}/**/*.parquet', union_by_name=true)
            WHERE symbol = 'SPY'
        """).iloc[0]
        assert r.iloc[0] < "2010-01-01", "SPY data should go back before 2010"
        assert r.iloc[1] > "2026-01-01", "SPY data should extend into 2026"
