"""
ingest_earnings_calendar.py
============================
Fetches expected earnings dates for all symbols via yfinance.
Writes to canonical/facts/fact_earnings_calendar.

Used to:
  - Flag earnings risk in option screening (avoid selling premium into earnings)
  - Compute days_to_earnings for any given date
  - Track historical earnings dates for backtesting

Schema: symbol, earnings_date, period_end, eps_estimate, revenue_estimate, is_confirmed, year
"""
from __future__ import annotations
import argparse, gc, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd
import yfinance as yf

from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings


def fetch_earnings(symbol: str) -> pd.DataFrame | None:
    try:
        t = yf.Ticker(symbol)

        # Historical earnings dates
        cal = t.earnings_dates
        if cal is None or cal.empty:
            return None

        df = cal.reset_index()
        df.columns = [c.strip() for c in df.columns]

        # Rename columns
        rename = {
            df.columns[0]:                                   "earnings_date",
            "EPS Estimate":                                  "eps_estimate",
            "Reported EPS":                                  "eps_actual",
            "Surprise(%)":                                   "eps_surprise_pct",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

        df["earnings_date"] = pd.to_datetime(df["earnings_date"], errors="coerce", utc=True).dt.tz_localize(None).dt.date
        df                  = df.dropna(subset=["earnings_date"])
        df["symbol"]        = symbol.upper()
        df["symbol_id"]     = stable_symbol_id(symbol)
        df["source"]        = "yfinance"
        df["year"]          = pd.to_datetime(df["earnings_date"]).dt.year

        keep = ["symbol_id", "symbol", "earnings_date",
                "eps_estimate", "eps_actual", "eps_surprise_pct", "source", "year"]
        return df[[c for c in keep if c in df.columns]].dropna(subset=["earnings_date"])

    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols",      nargs="*")
    parser.add_argument("--output-dir",   default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--delay",        type=float, default=0.3)
    args = parser.parse_args()

    settings     = Settings.load()
    output_dir   = Path(args.output_dir)   if args.output_dir   else settings.canonical_root / "facts" / "fact_earnings_calendar"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        import duckdb
        db = duckdb.connect(":memory:")
        symbols = db.execute(f"""
            SELECT symbol FROM read_parquet(
                '{settings.canonical_root}/dimensions/dim_symbol/**/*.parquet', union_by_name=true
            ) WHERE asset_type = 'stock' ORDER BY symbol
        """).df()["symbol"].tolist()
        db.close()

    print(f"Fetching earnings calendar for {len(symbols)} symbols...")

    total_rows, done, min_date, max_date = 0, 0, None, None

    for i, sym in enumerate(symbols, 1):
        try:
            df = fetch_earnings(sym)
            if df is not None and not df.empty:
                write_parquet(df, output_dir, partition_cols=["year"])
                total_rows += len(df)
                done += 1
                d_min = str(df["earnings_date"].min())
                d_max = str(df["earnings_date"].max())
                if min_date is None or d_min < min_date: min_date = d_min
                if max_date is None or d_max > max_date: max_date = d_max
            if i % 50 == 0 or i == len(symbols):
                print(f"  [{i}/{len(symbols)}] {done} with data, {total_rows:,} rows")
            time.sleep(args.delay)
        except Exception as e:
            print(f"  [{i}] {sym}: ERROR — {e}")
        finally:
            try: del df
            except NameError: pass
            gc.collect()

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("earnings_calendar", now_utc_iso()[:10]),
        dataset_name="fact_earnings_calendar", source="yfinance",
        file_path="yfinance/earnings_dates", row_count=total_rows,
        schema_hash=build_batch_id("earnings", str(total_rows)),
        min_date=min_date, max_date=max_date,
        ingested_at=now_utc_iso(), status="success",
    ), manifest_dir)
    print(f"\n✅ {total_rows:,} earnings rows, {done} symbols")

if __name__ == "__main__":
    main()
