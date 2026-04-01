"""
ingest_stooq_daily_bars.py
===========================
Downloads daily OHLCV bars from Stooq and writes to canonical parquet.

NOTE: Stooq blocks automated bulk historical downloads (returns HTTP 200 with
empty body). This script is kept as a secondary cross-validation source; use
ingest_yahoo_daily_bars.py as the primary equity ingestion path.

If Stooq starts returning data again, this script will work without changes.
Current status (2026-04): Historical endpoint returns empty responses.
"""
from __future__ import annotations
import argparse, io, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd, requests
from tenacity import retry, stop_after_attempt, wait_exponential
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.prices import validate_daily_bars

URL = "https://stooq.com/q/d/l/?s={sym}&d1={s}&d2={e}&i=d"
HDR = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

STOOQ_BLOCKED_MSG = """
  ⚠  Stooq is blocking automated historical data requests (HTTP 200, empty body).
     This is a known Stooq anti-scraping measure as of 2026.
     Use ingest_yahoo_daily_bars.py instead — Yahoo Finance works without restrictions.
"""

def stooq_sym(symbol):
    overrides = {"^gspc": "^spx", "spx": "^spx", "^dji": "^dji", "^vix": "^vix"}
    s = symbol.lower()
    if s in overrides:
        return overrides[s]
    return s if "." in s or s.startswith("^") else s + ".us"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
def fetch(symbol, start, end):
    url = URL.format(sym=stooq_sym(symbol), s=start.replace("-",""), e=end.replace("-",""))
    r = requests.get(url, headers=HDR, timeout=30)
    r.raise_for_status()
    text = r.text.strip()
    if not text or "No data" in text or len(text.splitlines()) < 2:
        raise ValueError(f"Empty response from Stooq for {symbol} — Stooq may be blocking requests")
    return pd.read_csv(io.StringIO(text))

def normalize(symbol, df):
    out = df.copy()
    out.columns = [c.lower().strip() for c in out.columns]
    out = out.rename(columns={"vol": "volume"})
    out["date"]      = pd.to_datetime(out["date"]).dt.date
    out["symbol"]    = symbol.upper()
    out["symbol_id"] = stable_symbol_id(symbol)
    out["adj_close"] = out.get("close", out.get("adj_close"))
    out["source"]    = "stooq"
    out["year"]      = pd.to_datetime(out["date"]).dt.year
    keep = ["symbol_id", "symbol", "date", "open", "high", "low",
            "close", "adj_close", "volume", "source", "year"]
    return out[[c for c in keep if c in out.columns]].sort_values("date")

def main():
    parser = argparse.ArgumentParser(description="Download daily bars from Stooq.")
    parser.add_argument("--symbols",     nargs="+", required=True)
    parser.add_argument("--start",       required=True)
    parser.add_argument("--end",         required=True)
    parser.add_argument("--output-dir",  default=None)
    parser.add_argument("--manifest-dir",default=None)
    parser.add_argument("--delay",       type=float, default=1.5,
                        help="Seconds between requests (default 1.5)")
    args = parser.parse_args()

    settings     = Settings.load()
    output_dir   = Path(args.output_dir)   if args.output_dir   else settings.canonical_root / "facts" / "fact_underlying_bar_daily"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root / "metadata" / "fact_dataset_manifest"

    frames, skipped = [], []
    for sym in args.symbols:
        print(f"  {sym}...", end=" ", flush=True)
        try:
            frames.append(normalize(sym, fetch(sym, args.start, args.end)))
            print("✓")
            time.sleep(args.delay)
        except Exception as e:
            print(f"FAILED — {e}")
            skipped.append(sym)

    if not frames:
        print(STOOQ_BLOCKED_MSG)
        print(f"  All {len(args.symbols)} symbols skipped. No data written.")
        print("  Suggestion: use ingest_yahoo_daily_bars.py instead.")
        return

    if skipped:
        print(f"  Skipped {len(skipped)} symbols: {', '.join(skipped)}")

    df   = pd.concat(frames, ignore_index=True)
    errs = validate_daily_bars(df)
    if errs:
        print("  Validation warnings: " + "; ".join(errs))

    write_parquet(df, output_dir, partition_cols=["year"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("stooq", "|".join(args.symbols), args.start, args.end),
        dataset_name="fact_underlying_bar_daily",
        source="stooq",
        file_path=f"symbols={','.join(args.symbols)}",
        row_count=len(df),
        schema_hash=schema_hash_for_frame(df),
        min_date=str(df["date"].min()),
        max_date=str(df["date"].max()),
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)
    print(f"  Wrote {len(df):,} rows ({len(frames)} symbols, {skipped and f'{len(skipped)} skipped' or 'all succeeded'})")

if __name__ == "__main__":
    main()
