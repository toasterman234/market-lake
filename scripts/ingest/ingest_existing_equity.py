from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.prices import validate_daily_bars

def sym_from_path(path):
    parts = path.stem.split("_daily_")
    return parts[0].upper() if len(parts) >= 2 else None

def normalize(symbol, df, source_label):
    out = df.copy()
    out.columns = [c.lower().strip() for c in out.columns]
    for possible in ("date","datetime","index"):
        if possible in out.columns:
            out = out.rename(columns={possible:"date"})
            break
    if "date" not in out.columns and hasattr(out.index, "name") and out.index.name:
        out = out.reset_index().rename(columns={out.index.name:"date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["symbol"] = symbol
    out["symbol_id"] = stable_symbol_id(symbol)
    for ac in ("adj_close","adj close","adjusted_close"):
        if ac in out.columns:
            out["adj_close"] = out[ac]
            break
    if "adj_close" not in out.columns:
        out["adj_close"] = out.get("close")
    out["source"] = source_label
    out["year"] = pd.to_datetime(out["date"]).dt.year
    keep = ["symbol_id","symbol","date","open","high","low","close","adj_close","volume","source","year"]
    return out[[c for c in keep if c in out.columns]]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--source-label", default="existing_cache")
    parser.add_argument("--symbols", nargs="*")
    args = parser.parse_args()
    settings = Settings.load()
    input_dir = Path(args.input_dir)
    output_dir  = Path(args.output_dir)  if args.output_dir  else settings.canonical_root/"facts"/"fact_underlying_bar_daily"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    files = sorted(input_dir.rglob("*_daily_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No *_daily_*.parquet found under {input_dir}")
    if args.symbols:
        sym_set = {s.upper() for s in args.symbols}
        files = [f for f in files if sym_from_path(f) in sym_set]
    print(f"  Processing {len(files)} equity files...")
    frames = []
    for f in files:
        sym = sym_from_path(f)
        if not sym:
            continue
        try:
            frames.append(normalize(sym, pd.read_parquet(f), args.source_label))
        except Exception as e:
            print(f"  Skipping {f.name}: {e}")
    df = pd.concat(frames, ignore_index=True).sort_values("date").drop_duplicates(subset=["symbol","date"], keep="last")
    errs = validate_daily_bars(df)
    if errs:
        print("Warnings: " + "; ".join(errs))
    write_parquet(df, output_dir, partition_cols=["year"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("existing_equity", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_underlying_bar_daily", source=args.source_label,
        file_path=str(input_dir), row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=str(df["date"].min()), max_date=str(df["date"].max()),
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} rows ({df['symbol'].nunique()} symbols)")

if __name__ == "__main__":
    main()
