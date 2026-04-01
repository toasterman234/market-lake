#!/usr/bin/env python3
"""
scripts/ops/health_check.py
============================
Single-query system health dashboard.
Prints a green/red status card for every table and key metric.
Run after the daily pipeline to get an instant system snapshot.

Exit 0 = all green. Exit 1 = something needs attention.

Usage:
    python3 scripts/ops/health_check.py
    python3 scripts/ops/health_check.py --json     # machine-readable output
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(os.environ.get("MARKET_LAKE_ROOT", ".")).resolve()
TODAY = date.today()
YESTERDAY = TODAY - timedelta(days=1)
# Allow data to be up to 5 calendar days stale (weekends + holidays)
FRESHNESS_CUTOFF = TODAY - timedelta(days=5)

CHECKS: list[dict] = [
    # name, path, date_col, freshness_check, min_rows, natural_key
    {
        "name": "fact_underlying_bar_daily",
        "path": ROOT / "canonical/facts/fact_underlying_bar_daily",
        "date_col": "date",
        "min_rows": 2_000_000,
        "natural_key": ["symbol", "date"],
    },
    {
        "name": "fact_option_feature_daily (VRP)",
        "path": ROOT / "canonical/features/fact_option_feature_daily",
        "date_col": "date",
        "min_rows": 1_000_000,
        "natural_key": ["symbol", "date"],
    },
    {
        "name": "fact_macro_series",
        "path": ROOT / "canonical/facts/fact_macro_series",
        "date_col": "date",
        "min_rows": 100_000,
        "natural_key": ["series_id", "date"],
        "min_series": 25,
    },
    {
        "name": "fact_ff_factors_daily",
        "path": ROOT / "canonical/facts/fact_ff_factors_daily",
        "date_col": "date",
        "min_rows": 20_000,
        "natural_key": ["date"],
        "freshness_days": 70,   # French library lag ~2 months
    },
    {
        "name": "fact_corporate_action",
        "path": ROOT / "canonical/facts/fact_corporate_action",
        "date_col": "action_date",
        "min_rows": 40_000,
        "natural_key": ["symbol", "action_date", "action_type"],
        "freshness_days": 365,  # actions are historical
    },
    {
        "name": "fact_fundamentals_annual",
        "path": ROOT / "canonical/facts/fact_fundamentals_annual",
        "date_col": "fiscal_year_end",
        "min_rows": 2_000,
        "natural_key": ["symbol", "fiscal_year_end"],
        "freshness_days": 180,  # annual reports
    },
    {
        "name": "fact_earnings_calendar",
        "path": ROOT / "canonical/facts/fact_earnings_calendar",
        "date_col": "earnings_date",
        "min_rows": 10_000,
        "natural_key": ["symbol", "earnings_date"],
        "freshness_days": 30,
    },
    {
        "name": "fact_short_interest",
        "path": ROOT / "canonical/facts/fact_short_interest",
        "date_col": "settle_date",
        "min_rows": 10_000_000,
        "natural_key": ["symbol", "settle_date"],
        "freshness_days": 10,
    },
    {
        "name": "dim_symbol",
        "path": ROOT / "canonical/dimensions/dim_symbol",
        "date_col": None,
        "min_rows": 500,
        "natural_key": ["symbol"],
    },
    {
        "name": "dim_option_contract",
        "path": ROOT / "canonical/dimensions/dim_option_contract",
        "date_col": None,
        "min_rows": 1_000_000,
        "natural_key": ["contract_id"],
    },
]


def run_check(spec: dict, db: duckdb.DuckDBPyConnection) -> dict:
    path = spec["path"]
    name = spec["name"]
    result = {
        "name": name,
        "ok": True,
        "issues": [],
        "rows": None,
        "latest_date": None,
        "duplicates": None,
    }

    if not path.exists() or not list(path.rglob("*.parquet")):
        result["ok"] = False
        result["issues"].append("TABLE MISSING — no parquet files found")
        return result

    try:
        # Row count
        result["rows"] = db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet', union_by_name=true)"
        ).fetchone()[0]

        if result["rows"] < spec["min_rows"]:
            result["ok"] = False
            result["issues"].append(
                f"too few rows: {result['rows']:,} < {spec['min_rows']:,}"
            )

        # Freshness
        if spec.get("date_col"):
            latest = db.execute(
                f"SELECT MAX({spec['date_col']})::DATE FROM read_parquet('{path}/**/*.parquet', union_by_name=true)"
            ).fetchone()[0]
            result["latest_date"] = str(latest) if latest else None
            max_lag = spec.get("freshness_days", 5)
            cutoff  = TODAY - timedelta(days=max_lag)
            if latest and latest < cutoff:
                result["ok"] = False
                result["issues"].append(
                    f"stale: latest={latest}, cutoff={cutoff} ({max_lag}d)"
                )

        # Duplicate natural key check (fast — only checks if key columns exist)
        nk = spec.get("natural_key", [])
        if nk:
            try:
                dups = db.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {', '.join(nk)}, COUNT(*) n
                        FROM read_parquet('{path}/**/*.parquet', union_by_name=true)
                        GROUP BY {', '.join(nk)} HAVING n > 1
                    )
                """).fetchone()[0]
                result["duplicates"] = dups
                if dups > 0:
                    result["ok"] = False
                    result["issues"].append(f"{dups:,} duplicate natural-key rows")
            except Exception:
                result["duplicates"] = "skipped"

        # Extra check for macro series count
        if spec.get("min_series"):
            n_series = db.execute(
                f"SELECT COUNT(DISTINCT series_id) FROM read_parquet('{path}/**/*.parquet', union_by_name=true)"
            ).fetchone()[0]
            if n_series < spec["min_series"]:
                result["ok"] = False
                result["issues"].append(f"only {n_series} series, expected >= {spec['min_series']}")

    except Exception as e:
        result["ok"] = False
        result["issues"].append(f"ERROR: {e}")

    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--quiet", action="store_true", help="Only show failures")
    args = parser.parse_args()

    db = duckdb.connect(":memory:")
    results = [run_check(spec, db) for spec in CHECKS]
    db.close()

    failures = [r for r in results if not r["ok"]]

    if args.json:
        print(json.dumps({"date": str(TODAY), "results": results,
                          "all_ok": len(failures) == 0}, indent=2, default=str))
        return 0 if not failures else 1

    # Human-readable card
    print(f"\n{'='*64}")
    print(f"  MARKET-LAKE HEALTH CHECK  —  {TODAY}")
    print(f"{'='*64}\n")

    for r in results:
        if args.quiet and r["ok"]:
            continue
        icon   = "✅" if r["ok"] else "❌"
        rows   = f"{r['rows']:>14,}" if r["rows"] is not None else "    (missing)"
        latest = f"  latest={r['latest_date']}" if r["latest_date"] else ""
        dups   = (f"  dups={r['duplicates']}"
                  if r["duplicates"] not in (None, 0, "skipped") else "")
        issues = ""
        if r["issues"]:
            issues = "\n       ⤷ " + "\n       ⤷ ".join(r["issues"])
        print(f"  {icon}  {r['name']:42s}  {rows}  rows{latest}{dups}{issues}")

    print()
    if failures:
        print(f"  {'='*60}")
        print(f"  ❌ {len(failures)} TABLE(S) NEED ATTENTION")
        print(f"  {'='*60}\n")
        return 1

    print(f"  All {len(results)} tables healthy.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
