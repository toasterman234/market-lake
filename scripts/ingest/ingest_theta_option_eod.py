"""
Memory-safe ThetaData option EOD ingestion.
Handles both raw vrp_validate chain parquets AND standard EOD files.
Processes one file at a time — never loads more than one in memory.
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
    ManifestRecord, build_batch_id, now_utc_iso, write_manifest,
)
from market_lake.settings import Settings
from market_lake.validation.options import validate_option_eod

# Column name variants across different ThetaData exports
COL_MAP = {
    # option_type variants
    "type": "option_type",
    "right": "option_type",
    # expiry variants
    "expiration": "expiry",
    "exp": "expiry",
    # IV variants
    "implied_volatility": "iv",
    "impliedvolatility": "iv",
    # underlying variants
    "symbol": "underlying_symbol",
    "ticker": "underlying_symbol",
    # mark/mid
    "mark": "mid",
}

OPTION_TYPE_MAP = {
    "call": "C", "c": "C", "put": "P", "p": "P",
}


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Lowercase all column names
    out.columns = [c.lower().strip() for c in out.columns]

    # Apply column renames
    out = out.rename(columns={k: v for k, v in COL_MAP.items() if k in out.columns})

    # Ensure underlying_symbol exists
    if "underlying_symbol" not in out.columns:
        if "contract_id" in out.columns:
            # try to derive from OCC-style contract_id e.g. NVDA080119C00020000
            out["underlying_symbol"] = (
                out["contract_id"].str.extract(r'^([A-Z]+)', expand=False).str.upper()
            )
        else:
            raise ValueError("Cannot determine underlying_symbol — no symbol/ticker column found")

    out["underlying_symbol"] = out["underlying_symbol"].astype(str).str.upper().str.strip()

    # Normalise option_type: call/put → C/P
    if "option_type" in out.columns:
        out["option_type"] = (
            out["option_type"].astype(str).str.lower()
            .map(OPTION_TYPE_MAP)
            .fillna(out["option_type"].astype(str).str.upper().str[0])
        )

    # Build contract_id if missing
    if "contract_id" not in out.columns:
        from market_lake.ids.contract_id import make_contract_id
        out["expiry"]  = pd.to_datetime(out["expiry"]).dt.date.astype(str)
        out["strike"]  = pd.to_numeric(out["strike"], errors="coerce")
        out["contract_id"] = out.apply(
            lambda r: make_contract_id(
                r["underlying_symbol"], r["expiry"], r["strike"], r["option_type"]
            ), axis=1,
        )

    # Date columns
    out["date"]  = pd.to_datetime(out["date"]).dt.date
    out["year"]  = pd.to_datetime(out["date"]).dt.year
    out["month"] = pd.to_datetime(out["date"]).dt.month

    # Expiry
    if "expiry" in out.columns:
        out["expiry"] = pd.to_datetime(out["expiry"]).dt.date

    # IDs
    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    out["source"]    = "thetadata"

    # Numeric columns
    for col in ["bid", "ask", "last", "mid", "iv", "delta", "gamma", "theta", "vega", "rho"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["volume", "open_interest"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    keep = [
        "contract_id", "symbol_id", "underlying_symbol", "date",
        "year", "month", "bid", "ask", "last", "mid",
        "volume", "open_interest", "iv", "delta", "gamma", "theta", "vega", "source",
    ]
    return out[[c for c in keep if c in out.columns]]


def write_one(df: pd.DataFrame, output_dir: Path) -> None:
    """Partition by year/month only — underlying_symbol causes too many partitions."""
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
        [f for f in list(input_dir.rglob("*.parquet")) + list(input_dir.rglob("*.csv"))
         if "validation" not in f.name and "diag" not in f.name]
    )
    if not files:
        raise FileNotFoundError(f"No data files found under {input_dir}")

    print(f"Processing {len(files)} file(s) one at a time (memory-safe)...")

    total_rows = 0
    min_date, max_date = None, None

    for i, path in enumerate(files, 1):
        try:
            print(f"  [{i}/{len(files)}] {path.name} — loading...")
            raw = (pd.read_parquet(path) if path.suffix == ".parquet"
                   else pd.read_csv(path))

            print(f"    {len(raw):,} rows — normalizing...")
            df = normalize(raw)
            del raw
            gc.collect()

            errs = validate_option_eod(df)
            non_fatal = [e for e in errs if "bid > ask" in e]
            fatal     = [e for e in errs if e not in non_fatal]
            if non_fatal:
                print(f"    note: {non_fatal[0]}")
            if fatal:
                print(f"    WARNING: {'; '.join(fatal)}")

            print(f"    writing {len(df):,} rows...")
            write_one(df, output_dir)
            total_rows += len(df)

            d_min = str(df["date"].min()) if not df.empty else None
            d_max = str(df["date"].max()) if not df.empty else None
            if d_min and (min_date is None or d_min < min_date): min_date = d_min
            if d_max and (max_date is None or d_max > max_date): max_date = d_max

            print(f"    ✓ done")

        except Exception as e:
            print(f"  [{i}/{len(files)}] {path.name}: SKIPPED — {e}")
        finally:
            try:
                del df
            except NameError:
                pass
            gc.collect()

    print(f"\n  Total: {total_rows:,} rows → {output_dir}")

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_eod", str(input_dir), now_utc_iso()[:10]),
        dataset_name="fact_option_eod",
        source="thetadata",
        file_path=str(input_dir),
        row_count=total_rows,
        schema_hash=build_batch_id("eod", str(total_rows)),
        min_date=min_date,
        max_date=max_date,
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)


if __name__ == "__main__":
    main()
