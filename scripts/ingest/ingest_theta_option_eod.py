"""
Memory-safe ThetaData option EOD ingestion.
Processes one symbol file at a time — never loads more than one
file into memory, writes immediately, then discards.

Usage:
    python scripts/ingest/ingest_theta_option_eod.py \
        --input-dir raw/thetadata/options_eod \
        --output-dir canonical/facts/fact_option_eod

For the vrp_validate chain parquets:
    python scripts/ingest/ingest_theta_option_eod.py \
        --input-dir "/path/to/cache/vrp_validate" \
        --output-dir canonical/facts/fact_option_eod
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

from market_lake.ids.contract_id import make_contract_id
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import (
    ManifestRecord, build_batch_id, now_utc_iso,
    schema_hash_for_frame, write_manifest,
)
from market_lake.settings import Settings
from market_lake.validation.options import validate_option_eod

FLOAT_COLS = ["bid", "ask", "last", "iv", "delta", "gamma", "theta", "vega", "rho"]
INT_COLS   = ["volume", "open_interest"]

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Build contract_id if not present
    if "contract_id" not in out.columns:
        for col in ("underlying_symbol", "expiry", "strike", "option_type"):
            if col not in out.columns:
                raise ValueError(f"Missing required column: {col}")
        out["underlying_symbol"] = out["underlying_symbol"].astype(str).str.upper().str.strip()
        out["expiry"]      = pd.to_datetime(out["expiry"]).dt.date.astype(str)
        out["strike"]      = pd.to_numeric(out["strike"], errors="coerce")
        out["option_type"] = out["option_type"].astype(str).str.upper().str[0]
        out["contract_id"] = out.apply(
            lambda r: make_contract_id(
                r["underlying_symbol"], r["expiry"], r["strike"], r["option_type"]
            ), axis=1,
        )
    if "underlying_symbol" not in out.columns:
        out["underlying_symbol"] = out["contract_id"].str.split("|").str[0]

    out["date"]  = pd.to_datetime(out["date"]).dt.date
    out["year"]  = pd.to_datetime(out["date"]).dt.year
    out["month"] = pd.to_datetime(out["date"]).dt.month
    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    out["source"]    = "thetadata"

    for col in FLOAT_COLS:
        out[col] = pd.to_numeric(out.get(col, float("nan")), errors="coerce")
    for col in INT_COLS:
        out[col] = pd.to_numeric(out.get(col, pd.NA), errors="coerce").astype("Int64")

    keep = [
        "contract_id", "symbol_id", "underlying_symbol", "date",
        "year", "month", "bid", "ask", "last", "volume", "open_interest",
        "iv", "delta", "gamma", "theta", "vega", "source",
    ]
    return out[[c for c in keep if c in out.columns]]


def write_one(df: pd.DataFrame, output_dir: Path) -> None:
    """Write a single normalized DataFrame directly to partitioned parquet.
    Never accumulates multiple files in memory."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["underlying_symbol", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
    )


def main() -> None:
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
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(
        list(input_dir.rglob("*.parquet")) + list(input_dir.rglob("*.csv"))
    )
    if not files:
        raise FileNotFoundError(f"No files found under {input_dir}")

    print(f"Processing {len(files)} file(s) one at a time (memory-safe)...")

    total_rows = 0
    schemas_seen: list[str] = []
    min_date, max_date = None, None

    for i, path in enumerate(files, 1):
        try:
            # Load one file
            raw = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
            df  = normalize(raw)

            # Validate this file
            errs = validate_option_eod(df)
            if errs:
                print(f"  [{i}/{len(files)}] {path.name}: warnings — {'; '.join(errs)}")

            # Write immediately — never accumulate
            write_one(df, output_dir)

            rows = len(df)
            total_rows += rows
            schemas_seen.append("|".join(f"{c}:{t}" for c, t in df.dtypes.items()))

            file_min = str(df["date"].min()) if not df.empty else None
            file_max = str(df["date"].max()) if not df.empty else None
            if file_min and (min_date is None or file_min < min_date):
                min_date = file_min
            if file_max and (max_date is None or file_max > max_date):
                max_date = file_max

            print(f"  [{i}/{len(files)}] {path.name}: {rows:,} rows  ✓")

        except Exception as e:
            print(f"  [{i}/{len(files)}] {path.name}: SKIPPED — {e}")

        finally:
            # Explicitly free memory after every file
            try:
                del raw, df
            except NameError:
                pass
            gc.collect()

    print(f"\n  Total: {total_rows:,} rows written to {output_dir}")

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_eod", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_option_eod",
        source="thetadata",
        file_path=str(input_dir),
        row_count=total_rows,
        schema_hash=build_batch_id(*schemas_seen) if schemas_seen else "none",
        min_date=min_date,
        max_date=max_date,
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)


if __name__ == "__main__":
    main()
