from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd
from market_lake.ids.contract_id import make_contract_id
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.options import validate_option_eod

FLOAT_COLS = ["bid","ask","last","iv","delta","gamma","theta","vega","rho"]
INT_COLS   = ["volume","open_interest"]

def normalize(df):
    out = df.copy()
    if "contract_id" not in out.columns:
        out["underlying_symbol"] = out["underlying_symbol"].astype(str).str.upper().str.strip()
        out["expiry"] = pd.to_datetime(out["expiry"]).dt.date.astype(str)
        out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
        out["option_type"] = out["option_type"].astype(str).str.upper().str[0]
        out["contract_id"] = out.apply(lambda r: make_contract_id(r["underlying_symbol"],r["expiry"],r["strike"],r["option_type"]), axis=1)
    if "underlying_symbol" not in out.columns:
        out["underlying_symbol"] = out["contract_id"].str.split("|").str[0]
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["year"]  = pd.to_datetime(out["date"]).dt.year
    out["month"] = pd.to_datetime(out["date"]).dt.month
    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    out["source"] = "thetadata"
    for c in FLOAT_COLS:
        out[c] = pd.to_numeric(out.get(c, float("nan")), errors="coerce")
    for c in INT_COLS:
        out[c] = pd.to_numeric(out.get(c, pd.NA), errors="coerce").astype("Int64")
    keep = ["contract_id","symbol_id","underlying_symbol","date","year","month","bid","ask","last","volume","open_interest","iv","delta","gamma","theta","vega","source"]
    return out[[c for c in keep if c in out.columns]]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()
    settings = Settings.load()
    input_dir, output_dir = Path(args.input_dir), Path(args.output_dir)
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    files = sorted(list(input_dir.rglob("*.parquet")) + list(input_dir.rglob("*.csv")))
    if not files:
        raise FileNotFoundError(f"No files under {input_dir}")
    frames = []
    for p in files:
        try:
            raw = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p)
            frames.append(normalize(raw))
        except Exception as e:
            print(f"  Skipping {p.name}: {e}")
    df = pd.concat(frames, ignore_index=True)
    errs = validate_option_eod(df)
    if errs:
        print("Warnings: " + "; ".join(errs))
    write_parquet(df, output_dir, partition_cols=["underlying_symbol","year","month"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_eod", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_option_eod", source="thetadata",
        file_path=str(input_dir), row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=str(df["date"].min()) if not df.empty else None,
        max_date=str(df["date"].max()) if not df.empty else None,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} option EOD rows")

if __name__ == "__main__":
    main()
