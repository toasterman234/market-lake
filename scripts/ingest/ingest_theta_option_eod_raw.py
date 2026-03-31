"""
ingest_theta_option_eod_raw.py

Ingests ThetaData raw option EOD files into:
  - canonical/facts/fact_option_eod/
  - canonical/dimensions/dim_option_contract/

Handles two source formats found in the existing cache:

FORMAT A — vrp_validate/ files (e.g. spy_options.parquet)
  cols: contract_id (OCC), symbol, expiration, strike, type (call/put),
        last, mark, bid, bid_size, ask, ask_size, volume, open_interest,
        date, implied_volatility, delta, gamma, theta, vega, rho, in_the_money

FORMAT B — chain/ files (e.g. AAPL_chain_2024-06-01_2026-03-20.parquet)
  cols: date, symbol, expiration (str), dte, right (C/P), strike,
        bid, ask, bid_size, ask_size, open, high, low, close,
        volume, open_interest, mid, iv, delta, gamma, theta, vega

Usage:
    python scripts/ingest/ingest_theta_option_eod_raw.py \\
        --input-files "path/spy_options.parquet" "path/qqq_options.parquet" \\
        --format vrp_validate

    python scripts/ingest/ingest_theta_option_eod_raw.py \\
        --input-files "path/AAPL_chain_2024-06-01_2026-03-20.parquet" \\
        --format chain
"""
from __future__ import annotations

import argparse
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


def normalize_vrp_validate(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize FORMAT A — vrp_validate files."""
    out = df.copy()

    # Standardise symbol
    out["underlying_symbol"] = out["symbol"].astype(str).str.upper().str.strip()

    # expiry: convert Timestamp → date string
    out["expiry"] = pd.to_datetime(out["expiration"]).dt.date.astype(str)

    # option_type: 'call'/'put' → 'C'/'P'
    out["option_type"] = out["type"].astype(str).str[0].str.upper()

    # Canonical contract_id (our pipe-delimited format)
    out["contract_id"] = out.apply(
        lambda r: make_contract_id(r["underlying_symbol"], r["expiry"], r["strike"], r["option_type"]),
        axis=1,
    )

    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    out["date"]    = pd.to_datetime(out["date"]).dt.date
    out["year"]    = pd.to_datetime(out["date"]).dt.year
    out["month"]   = pd.to_datetime(out["date"]).dt.month

    # Rename implied_volatility → iv
    out = out.rename(columns={"implied_volatility": "iv", "mark": "mark_price"})

    out["source"] = "thetadata_vrp_validate"

    keep = [
        "contract_id", "symbol_id", "underlying_symbol", "date", "year", "month",
        "expiry", "strike", "option_type",
        "bid", "ask", "mark_price", "last",
        "bid_size", "ask_size", "volume", "open_interest",
        "iv", "delta", "gamma", "theta", "vega", "rho",
        "in_the_money", "source",
    ]
    return out[[c for c in keep if c in out.columns]]


def normalize_chain(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize FORMAT B — chain/ files."""
    out = df.copy()

    out["underlying_symbol"] = out["symbol"].astype(str).str.upper().str.strip()

    # expiry: may be string or timestamp
    out["expiry"] = pd.to_datetime(out["expiration"]).dt.date.astype(str)

    # option_type: 'C'/'P' already (right column)
    out["option_type"] = out["right"].astype(str).str.upper().str[0]

    out["contract_id"] = out.apply(
        lambda r: make_contract_id(r["underlying_symbol"], r["expiry"], r["strike"], r["option_type"]),
        axis=1,
    )

    out["symbol_id"] = out["underlying_symbol"].map(stable_symbol_id)
    out["date"]  = pd.to_datetime(out["date"]).dt.date
    out["year"]  = pd.to_datetime(out["date"]).dt.year
    out["month"] = pd.to_datetime(out["date"]).dt.month

    # mid is already present; last = close
    if "close" in out.columns:
        out["last"] = out["close"]

    out["source"] = "thetadata_chain"

    keep = [
        "contract_id", "symbol_id", "underlying_symbol", "date", "year", "month",
        "expiry", "strike", "option_type", "dte",
        "bid", "ask", "mid", "last",
        "bid_size", "ask_size", "volume", "open_interest",
        "iv", "delta", "gamma", "theta", "vega", "source",
    ]
    return out[[c for c in keep if c in out.columns]]


def extract_contracts(eod_df: pd.DataFrame) -> pd.DataFrame:
    """Extract unique contracts from a normalized EOD DataFrame."""
    return (
        eod_df[["contract_id", "symbol_id", "underlying_symbol", "expiry", "strike", "option_type"]]
        .drop_duplicates(subset=["contract_id"])
        .assign(multiplier=100, occ_symbol=None, first_seen=None, last_seen=None)
    )


def write_partitioned(df: pd.DataFrame, output_dir: Path) -> None:
    """Write partitioned by underlying_symbol / year / month."""
    output_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["underlying_symbol", "year", "month"],
        existing_data_behavior="overwrite_or_ignore",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-files", nargs="+", required=True)
    parser.add_argument("--format", choices=["vrp_validate", "chain"], required=True)
    parser.add_argument("--eod-output-dir", default=None)
    parser.add_argument("--contracts-output-dir", default=None)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()

    settings = Settings.load()
    eod_dir       = Path(args.eod_output_dir)       if args.eod_output_dir       else settings.canonical_root / "facts"      / "fact_option_eod"
    contracts_dir = Path(args.contracts_output_dir) if args.contracts_output_dir else settings.canonical_root / "dimensions" / "dim_option_contract"
    manifest_dir  = Path(args.manifest_dir)         if args.manifest_dir         else settings.canonical_root / "metadata"   / "fact_dataset_manifest"

    normalize_fn = normalize_vrp_validate if args.format == "vrp_validate" else normalize_chain

    all_contracts: list[pd.DataFrame] = []
    total_rows = 0

    for fp in args.input_files:
        path = Path(fp)
        print(f"\nProcessing {path.name} ...")
        raw = pd.read_parquet(path)
        print(f"  {len(raw):,} raw rows")

        eod = normalize_fn(raw)
        del raw  # free memory

        contracts = extract_contracts(eod)
        all_contracts.append(contracts)

        # Write EOD partitioned (appends to existing partitions)
        write_partitioned(eod, eod_dir)
        total_rows += len(eod)
        print(f"  {len(eod):,} rows written → {eod_dir}")
        del eod

    # Merge + deduplicate contracts
    print("\nWriting dim_option_contract ...")
    contracts_df = (
        pd.concat(all_contracts, ignore_index=True)
        .drop_duplicates(subset=["contract_id"])
        .reset_index(drop=True)
    )
    contracts_dir.mkdir(parents=True, exist_ok=True)
    existing = contracts_dir / "dim_option_contract.parquet"
    if existing.exists():
        old = pd.read_parquet(existing)
        contracts_df = (
            pd.concat([old, contracts_df], ignore_index=True)
            .drop_duplicates(subset=["contract_id"])
            .reset_index(drop=True)
        )
    contracts_df.to_parquet(existing, index=False)
    print(f"  {len(contracts_df):,} unique contracts → {existing}")

    # Manifest
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("theta_eod_raw", args.format, *args.input_files),
        dataset_name="fact_option_eod",
        source=f"thetadata_{args.format}",
        file_path="; ".join(args.input_files),
        row_count=total_rows,
        schema_hash="n/a",
        min_date=None,
        max_date=None,
        ingested_at=now_utc_iso(),
        status="success",
        notes=f"format={args.format}",
    ), manifest_dir)

    print(f"\n✅ Total: {total_rows:,} EOD rows, {len(contracts_df):,} contracts")


if __name__ == "__main__":
    main()
