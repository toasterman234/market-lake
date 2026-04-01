#!/usr/bin/env python3
"""
scripts/ops/row_count_check.py
================================
Compares current row counts against the previous run's manifest snapshot.
Flags any table that grew >50% or shrank >5% unexpectedly.

Runs as the LAST step of daily_refresh.sh.
Exits 0 if all checks pass, exits 1 if any anomaly is found.

Usage:
    python3 scripts/ops/row_count_check.py
    python3 scripts/ops/row_count_check.py --save   # update baseline after intentional bulk load
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import duckdb

ROOT = Path(os.environ.get("MARKET_LAKE_ROOT", ".")).resolve()
BASELINE_FILE = ROOT / "config" / "row_count_baseline.json"

TABLES = {
    "fact_underlying_bar_daily":  ROOT / "canonical/facts/fact_underlying_bar_daily",
    "fact_option_eod":            ROOT / "canonical/facts/fact_option_eod",
    "fact_option_feature_daily":  ROOT / "canonical/features/fact_option_feature_daily",
    "fact_macro_series":          ROOT / "canonical/facts/fact_macro_series",
    "fact_ff_factors_daily":      ROOT / "canonical/facts/fact_ff_factors_daily",
    "fact_corporate_action":      ROOT / "canonical/facts/fact_corporate_action",
    "fact_financial_statements":  ROOT / "canonical/facts/fact_financial_statements",
    "fact_fundamentals_annual":   ROOT / "canonical/facts/fact_fundamentals_annual",
    "fact_earnings_calendar":     ROOT / "canonical/facts/fact_earnings_calendar",
    "fact_short_interest":        ROOT / "canonical/facts/fact_short_interest",
    "dim_symbol":                 ROOT / "canonical/dimensions/dim_symbol",
    "dim_option_contract":        ROOT / "canonical/dimensions/dim_option_contract",
    "dim_calendar":               ROOT / "canonical/dimensions/dim_calendar",
}

# Thresholds
SHRINK_THRESHOLD = 0.05   # alert if table loses > 5% of rows
GROW_THRESHOLD   = 0.50   # alert if table grows > 50% in one run (likely duplication)

# Tables exempt from grow check (legitimately grow in bulk)
BULK_LOAD_EXEMPT = {"fact_option_eod", "fact_short_interest", "dim_option_contract"}


def count_rows(path: Path) -> int | None:
    db = duckdb.connect(":memory:")
    try:
        return db.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}/**/*.parquet', union_by_name=true)"
        ).fetchone()[0]
    except Exception:
        return None
    finally:
        db.close()


def load_baseline() -> dict[str, int]:
    if BASELINE_FILE.exists():
        with open(BASELINE_FILE) as f:
            data = json.load(f)
        return data.get("counts", {})
    return {}


def save_baseline(counts: dict[str, int]) -> None:
    BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(BASELINE_FILE, "w") as f:
        json.dump({"date": str(date.today()), "counts": counts}, f, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Row count anomaly detector")
    parser.add_argument("--save", action="store_true",
                        help="Save current counts as new baseline (use after bulk loads)")
    parser.add_argument("--quiet", action="store_true",
                        help="Only print anomalies")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  ROW COUNT CHECK  —  {date.today()}")
    print(f"{'='*60}\n")

    baseline = load_baseline()
    current: dict[str, int] = {}
    anomalies: list[str] = []

    for table_name, path in TABLES.items():
        count = count_rows(path)
        if count is None:
            print(f"  ⚠️  {table_name:40s} (could not read)")
            continue

        current[table_name] = count
        prev = baseline.get(table_name)

        if prev is None:
            status = "NEW — no baseline"
            icon = "🆕"
        else:
            delta = count - prev
            pct   = delta / prev if prev > 0 else 0

            if count < prev * (1 - SHRINK_THRESHOLD):
                status = f"⚠️  SHRANK  {prev:,} → {count:,}  (−{abs(pct)*100:.1f}%)"
                anomalies.append(f"{table_name}: shrank {prev:,} → {count:,}")
                icon = "🔴"
            elif (table_name not in BULK_LOAD_EXEMPT
                  and count > prev * (1 + GROW_THRESHOLD)):
                status = f"⚠️  GREW BIG  {prev:,} → {count:,}  (+{pct*100:.1f}%)  possible duplication"
                anomalies.append(f"{table_name}: grew {prev:,} → {count:,} (+{pct*100:.1f}%)")
                icon = "🔴"
            elif delta > 0:
                status = f"✅  {prev:,} → {count:,}  (+{delta:,})"
                icon = "✅"
            elif delta == 0:
                status = f"✅  {count:,} (unchanged)"
                icon = "✅"
            else:
                status = f"ℹ️   {prev:,} → {count:,}  (−{abs(delta):,})"
                icon = "ℹ️ "

        if not args.quiet or icon in ("🔴", "🆕"):
            print(f"  {icon}  {table_name:40s} {status}")

    print()

    if args.save:
        save_baseline(current)
        print(f"  ✅ Baseline saved to {BASELINE_FILE.relative_to(ROOT)}")
    elif not baseline:
        save_baseline(current)
        print(f"  ✅ First run — baseline created at {BASELINE_FILE.relative_to(ROOT)}")

    if anomalies:
        print(f"\n  {'='*56}")
        print(f"  ❌ {len(anomalies)} ANOMALIE(S) DETECTED:")
        for a in anomalies:
            print(f"     • {a}")
        print(f"  {'='*56}")
        print(f"\n  If this was intentional (bulk load), re-run with --save to update baseline.\n")
        return 1

    print(f"  All row counts nominal.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
