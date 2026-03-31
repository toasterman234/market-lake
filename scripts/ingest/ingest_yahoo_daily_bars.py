from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd, yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.prices import validate_daily_bars

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def fetch(symbol, start, end):
    df = yf.Ticker(symbol).history(start=start, end=end, auto_adjust=False, actions=False)
    if df.empty:
        raise ValueError(f"No data for {symbol}")
    return df

def normalize(symbol, df):
    out = df.reset_index()
    out.columns = [c.lower().replace(" ","_") for c in out.columns]
    date_col = next((c for c in ("date","datetime") if c in out.columns), None)
    if not date_col:
        raise ValueError(f"No date column for {symbol}")
    out = out.rename(columns={date_col: "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["symbol"] = symbol.upper()
    out["symbol_id"] = stable_symbol_id(symbol)
    out["adj_close"] = out.get("adj_close", out["close"])
    out["source"] = "yahoo"
    out["year"] = pd.to_datetime(out["date"]).dt.year
    keep = ["symbol_id","symbol","date","open","high","low","close","adj_close","volume","source","year"]
    return out[[c for c in keep if c in out.columns]]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()
    settings = Settings.load()
    output_dir  = Path(args.output_dir)  if args.output_dir  else settings.canonical_root/"facts"/"fact_underlying_bar_daily"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    frames = []
    for sym in args.symbols:
        print(f"  {sym}...")
        try:
            frames.append(normalize(sym, fetch(sym, args.start, args.end)))
        except Exception as e:
            print(f"  {sym}: {e}")
    df = pd.concat(frames, ignore_index=True)
    errs = validate_daily_bars(df)
    if errs:
        print("Warnings: " + "; ".join(errs))
    write_parquet(df, output_dir, partition_cols=["year"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("yahoo", "|".join(args.symbols), args.start, args.end),
        dataset_name="fact_underlying_bar_daily", source="yahoo",
        file_path=f"symbols={','.join(args.symbols)}", row_count=len(df),
        schema_hash=schema_hash_for_frame(df), min_date=str(df["date"].min()), max_date=str(df["date"].max()),
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} rows")

if __name__ == "__main__":
    main()
