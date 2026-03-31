from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

CANONICAL = ["symbol_id","symbol","date","year","month","spot_price",
    "iv_7d","iv_14d","iv_21d","iv_30d","iv_45d","iv_60d","iv_90d","iv_180d",
    "ts_slope_30_60","ts_slope_30_90","put_iv_10d","put_iv_25d","put_iv_50d",
    "call_iv_10d","call_iv_25d","call_iv_50d","put_skew_25d","put_skew_10d",
    "skew_slope","wing_premium","pc_volume_ratio","total_volume",
    "hv5","hv10","hv20","hv30","hv60","hv90","vrp_30d","vrp_60d","vrp_90d",
    "ivr_252d","ivp_252d","source"]

def sym_from_path(p):
    parts = p.stem.split("_vrp_")
    return parts[0].upper() if len(parts) >= 2 else None

def normalize(df, symbol):
    out = df.copy()
    if "symbol" not in out.columns:
        out["symbol"] = symbol
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    out["date"]  = pd.to_datetime(out["date"]).dt.date
    out["year"]  = pd.to_datetime(out["date"]).dt.year
    out["month"] = pd.to_datetime(out["date"]).dt.month
    out["symbol_id"] = stable_symbol_id(symbol)
    out["source"] = "thetadata"
    for c in [col for col in out.columns if col not in ("symbol","date","source")]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    avail = [c for c in CANONICAL if c in out.columns]
    extra = [c for c in out.columns if c not in CANONICAL]
    return out[avail + extra]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--symbols", nargs="*")
    args = parser.parse_args()
    settings = Settings.load()
    input_dir, output_dir = Path(args.input_dir), Path(args.output_dir)
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    files = sorted(input_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files under {input_dir}")
    if args.symbols:
        sym_set = {s.upper() for s in args.symbols}
        files = [f for f in files if sym_from_path(f) in sym_set]
    print(f"  Ingesting {len(files)} VRP files...")
    frames = []
    for f in files:
        sym = sym_from_path(f)
        if not sym:
            continue
        try:
            frames.append(normalize(pd.read_parquet(f), sym))
        except Exception as e:
            print(f"  Skipping {f.name}: {e}")
    df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["symbol","date"])
    write_parquet(df, output_dir, partition_cols=["year","month"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_vrp", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_option_feature_daily", source="thetadata",
        file_path=str(input_dir), row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=str(df["date"].min()) if not df.empty else None,
        max_date=str(df["date"].max()) if not df.empty else None,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} VRP rows ({df['symbol'].nunique()} symbols)")

if __name__ == "__main__":
    main()
