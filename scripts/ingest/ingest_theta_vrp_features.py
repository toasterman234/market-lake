"""
Memory-safe ThetaData VRP feature ingestion.
Processes and writes one symbol at a time.
"""
from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import (
    ManifestRecord, build_batch_id, now_utc_iso,
    schema_hash_for_frame, write_manifest,
)
from market_lake.settings import Settings

CANONICAL = [
    "symbol_id", "symbol", "date", "year", "month", "spot_price",
    "iv_7d", "iv_14d", "iv_21d", "iv_30d", "iv_45d", "iv_60d", "iv_90d", "iv_180d",
    "ts_slope_30_60", "ts_slope_30_90",
    "put_iv_10d", "put_iv_25d", "put_iv_50d",
    "call_iv_10d", "call_iv_25d", "call_iv_50d",
    "put_skew_25d", "put_skew_10d", "skew_slope", "wing_premium",
    "pc_volume_ratio", "total_volume",
    "hv5", "hv10", "hv20", "hv30", "hv60", "hv90",
    "vrp_30d", "vrp_60d", "vrp_90d",
    "ivr_252d", "ivp_252d", "source",
]


def sym_from_path(p: Path) -> str | None:
    parts = p.stem.split("_vrp_")
    return parts[0].upper() if len(parts) >= 2 else None


def normalize(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = df.copy()
    if "symbol" not in out.columns:
        out["symbol"] = symbol
    out["symbol"] = out["symbol"].astype(str).str.upper().str.strip()
    out["date"]   = pd.to_datetime(out["date"]).dt.date
    out["year"]   = pd.to_datetime(out["date"]).dt.year
    out["month"]  = pd.to_datetime(out["date"]).dt.month
    out["symbol_id"] = stable_symbol_id(symbol)
    out["source"] = "thetadata"
    for col in [c for c in out.columns if c not in ("symbol", "date", "source")]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    avail = [c for c in CANONICAL if c in out.columns]
    extra = [c for c in out.columns if c not in CANONICAL]
    return out[avail + extra].drop_duplicates(subset=["symbol", "date"])


def write_one(df: pd.DataFrame, output_dir: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["year", "month"],
        existing_data_behavior="overwrite_or_ignore",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir",    required=True)
    parser.add_argument("--output-dir",   required=True)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--symbols",      nargs="*")
    args = parser.parse_args()

    settings     = Settings.load()
    input_dir    = Path(args.input_dir)
    output_dir   = Path(args.output_dir)
    manifest_dir = (
        Path(args.manifest_dir) if args.manifest_dir
        else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files under {input_dir}")
    if args.symbols:
        sym_set = {s.upper() for s in args.symbols}
        files = [f for f in files if sym_from_path(f) in sym_set]

    print(f"Processing {len(files)} VRP file(s) one at a time (memory-safe)...")

    total_rows  = 0
    total_syms  = 0
    min_date, max_date = None, None

    for i, path in enumerate(files, 1):
        sym = sym_from_path(path)
        if not sym:
            print(f"  [{i}/{len(files)}] {path.name}: could not parse symbol — skipped")
            continue
        try:
            df = normalize(pd.read_parquet(path), sym)
            write_one(df, output_dir)

            rows = len(df)
            total_rows += rows
            total_syms += 1

            d_min = str(df["date"].min()) if not df.empty else None
            d_max = str(df["date"].max()) if not df.empty else None
            if d_min and (min_date is None or d_min < min_date): min_date = d_min
            if d_max and (max_date is None or d_max > max_date): max_date = d_max

            if i % 50 == 0 or i == len(files):
                print(f"  [{i}/{len(files)}] {sym}: {rows:,} rows  ✓")

        except Exception as e:
            print(f"  [{i}/{len(files)}] {path.name}: SKIPPED — {e}")
        finally:
            try:
                del df
            except NameError:
                pass
            gc.collect()

    print(f"\n  Total: {total_rows:,} rows, {total_syms} symbols → {output_dir}")

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_vrp", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_option_feature_daily",
        source="thetadata",
        file_path=str(input_dir),
        row_count=total_rows,
        schema_hash=build_batch_id("vrp", str(total_syms)),
        min_date=min_date,
        max_date=max_date,
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)


if __name__ == "__main__":
    main()
