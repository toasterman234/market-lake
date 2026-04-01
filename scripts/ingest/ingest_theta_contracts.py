"""
ingest_theta_contracts.py
==========================
Builds dim_option_contract from ThetaData chain parquets or contract listings.

Handles all known ThetaData column variants:
  option_type: 'right', 'type', 'option_type'   (values: C/P or call/put)
  expiry:      'expiration', 'exp', 'expiry'
  underlying:  'symbol', 'ticker', 'underlying_symbol'

Input: directory of parquet or CSV files (one per symbol or combined)
Output: canonical/dimensions/dim_option_contract/dim_option_contract.parquet
"""
from __future__ import annotations
import argparse, gc, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd
from market_lake.ids.contract_id import make_contract_id
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import (
    ManifestRecord, build_batch_id, now_utc_iso,
    schema_hash_for_frame, write_manifest,
)
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.options import validate_option_contracts

# All known column name variants across ThetaData export formats
COL_MAP = {
    "right":            "option_type",
    "type":             "option_type",
    "expiration":       "expiry",
    "exp":              "expiry",
    "symbol":           "underlying_symbol",
    "ticker":           "underlying_symbol",
}
OPTION_TYPE_MAP = {"call": "C", "c": "C", "put": "P", "p": "P"}
OUT_COLS = [
    "contract_id", "symbol_id", "underlying_symbol", "occ_symbol",
    "expiry", "strike", "option_type", "multiplier", "first_seen", "last_seen",
]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.lower().strip() for c in out.columns]
    out = out.rename(columns={k: v for k, v in COL_MAP.items() if k in out.columns})

    # underlying_symbol
    if "underlying_symbol" not in out.columns:
        if "contract_id" in out.columns:
            out["underlying_symbol"] = (
                out["contract_id"].str.extract(r'^([A-Z]+)', expand=False).str.upper()
            )
        else:
            raise ValueError("Cannot determine underlying_symbol")
    out["underlying_symbol"] = out["underlying_symbol"].astype(str).str.upper().str.strip()

    # option_type — normalise call/put → C/P
    if "option_type" not in out.columns:
        raise ValueError("Cannot determine option_type (tried: right, type, option_type)")
    out["option_type"] = (
        out["option_type"].astype(str).str.lower()
        .map(OPTION_TYPE_MAP)
        .fillna(out["option_type"].astype(str).str.upper().str[0])
    )
    out = out[out["option_type"].isin(["C", "P"])]

    # expiry
    if "expiry" not in out.columns:
        raise ValueError("Cannot determine expiry (tried: expiration, exp, expiry)")
    out["expiry"] = pd.to_datetime(out["expiry"], errors="coerce").dt.date.astype(str)

    # strike
    out["strike"] = pd.to_numeric(out["strike"], errors="coerce")
    out = out.dropna(subset=["strike", "expiry"])

    # derived
    out["contract_id"] = out.apply(
        lambda r: make_contract_id(
            r["underlying_symbol"], r["expiry"], r["strike"], r["option_type"]
        ), axis=1,
    )
    out["symbol_id"]  = out["underlying_symbol"].map(stable_symbol_id)
    out["multiplier"] = int(100)
    out["occ_symbol"] = out.get("occ_symbol", pd.Series([None] * len(out), dtype=object))

    # first_seen / last_seen — derive from date column if present
    if "date" in out.columns:
        out["date_parsed"] = pd.to_datetime(out["date"], errors="coerce").dt.date
        first = out.groupby("contract_id")["date_parsed"].min().reset_index().rename(columns={"date_parsed": "first_seen"})
        last  = out.groupby("contract_id")["date_parsed"].max().reset_index().rename(columns={"date_parsed": "last_seen"})
        out = out.merge(first, on="contract_id", how="left").merge(last, on="contract_id", how="left")
        out["first_seen"] = out["first_seen"].astype(str)
        out["last_seen"]  = out["last_seen"].astype(str)
    else:
        out["first_seen"] = None
        out["last_seen"]  = None

    return out[[c for c in OUT_COLS if c in out.columns]].drop_duplicates(subset=["contract_id"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir",    required=True)
    parser.add_argument("--output-dir",   required=True)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()

    settings     = Settings.load()
    input_dir    = Path(args.input_dir)
    output_dir   = Path(args.output_dir)
    manifest_dir = (
        Path(args.manifest_dir) if args.manifest_dir
        else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    )

    files = sorted(
        list(input_dir.rglob("*.parquet")) + list(input_dir.rglob("*.csv"))
    )
    files = [f for f in files if "validation" not in f.name and "diag" not in f.name]
    if not files:
        raise FileNotFoundError(f"No parquet/csv files under {input_dir}")

    print(f"Processing {len(files)} file(s)...")
    all_frames = []
    for i, path in enumerate(files, 1):
        try:
            raw = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
            df  = normalize(raw)
            all_frames.append(df)
            print(f"  [{i}/{len(files)}] {path.name}: {len(df):,} unique contracts  ✓")
        except Exception as e:
            print(f"  [{i}/{len(files)}] {path.name}: SKIPPED — {e}")
        finally:
            try: del raw, df
            except NameError: pass
            gc.collect()

    if not all_frames:
        raise RuntimeError("No contracts extracted from any file")

    combined = pd.concat(all_frames, ignore_index=True).drop_duplicates(subset=["contract_id"])
    print(f"\nTotal unique contracts: {len(combined):,}")

    errs = validate_option_contracts(combined[["contract_id","underlying_symbol","expiry","strike","option_type"]])
    if errs:
        print(f"Validation warnings: {'; '.join(errs)}")

    write_parquet(combined, output_dir, filename="dim_option_contract.parquet")
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_contracts", str(input_dir)),
        dataset_name="dim_option_contract",
        source="thetadata",
        file_path=str(input_dir),
        row_count=len(combined),
        schema_hash=schema_hash_for_frame(combined),
        min_date=combined["expiry"].min() if not combined.empty else None,
        max_date=combined["expiry"].max() if not combined.empty else None,
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)
    print(f"Written to {output_dir}/dim_option_contract.parquet")


if __name__ == "__main__":
    main()
