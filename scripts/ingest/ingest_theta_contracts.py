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
from market_lake.validation.options import validate_option_contracts

REQUIRED = {"underlying_symbol","expiry","strike","option_type"}

def read_file(path):
    return pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)

def normalize(df):
    missing = REQUIRED - set(df.columns)
    if missing:
        raise ValueError(f"Missing: {sorted(missing)}")
    out = df.copy()
    out["underlying_symbol"] = out["underlying_symbol"].astype(str).str.upper().str.strip()
    out["expiry"] = pd.to_datetime(out["expiry"]).dt.date.astype(str)
    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
    out["option_type"] = out["option_type"].astype(str).str.upper().str[0]
    out["multiplier"] = pd.to_numeric(out.get("multiplier", pd.Series(100, index=out.index)), errors="coerce").fillna(100).astype(int)
    out["occ_symbol"] = out.get("occ_symbol", pd.Series([None]*len(out), dtype=object))
    out["first_seen"] = pd.to_datetime(out.get("first_seen"), errors="coerce").dt.date.astype("string")
    out["last_seen"] = pd.to_datetime(out.get("last_seen"), errors="coerce").dt.date.astype("string")
    out["contract_id"] = out.apply(lambda r: make_contract_id(r["underlying_symbol"],r["expiry"],r["strike"],r["option_type"]), axis=1)
    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    cols = ["contract_id","symbol_id","underlying_symbol","occ_symbol","expiry","strike","option_type","multiplier","first_seen","last_seen"]
    return out[cols].drop_duplicates(subset=["contract_id"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()
    settings = Settings.load()
    input_dir, output_dir = Path(args.input_dir), Path(args.output_dir)
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    files = sorted(list(input_dir.rglob("*.csv")) + list(input_dir.rglob("*.parquet")))
    if not files:
        raise FileNotFoundError(f"No files under {input_dir}")
    df = pd.concat([normalize(read_file(f)) for f in files], ignore_index=True).drop_duplicates(subset=["contract_id"])
    errs = validate_option_contracts(df)
    if errs:
        raise ValueError("Validation: " + "; ".join(errs))
    write_parquet(df, output_dir, filename="dim_option_contract.parquet")
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_contracts", *[str(f) for f in files]),
        dataset_name="dim_option_contract", source="thetadata",
        file_path=str(input_dir), row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=df["expiry"].min() if not df.empty else None,
        max_date=df["expiry"].max() if not df.empty else None,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} contracts")

if __name__ == "__main__":
    main()
