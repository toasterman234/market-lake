"""
Memory-safe equity bar ingestion from existing *_daily_*.parquet files.
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
from market_lake.validation.prices import validate_daily_bars


def sym_from_path(path: Path) -> str | None:
    parts = path.stem.split("_daily_")
    return parts[0].upper() if len(parts) >= 2 else None


def normalize(symbol: str, df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.lower().strip() for c in out.columns]
    for possible in ("date", "datetime", "index"):
        if possible in out.columns:
            out = out.rename(columns={possible: "date"})
            break
    if "date" not in out.columns and hasattr(out.index, "name") and out.index.name:
        out = out.reset_index().rename(columns={out.index.name: "date"})
    out["date"] = pd.to_datetime(out["date"]).dt.date
    out["symbol"]    = symbol
    out["symbol_id"] = stable_symbol_id(symbol)
    for ac in ("adj_close", "adj close", "adjusted_close"):
        if ac in out.columns:
            out["adj_close"] = out[ac]
            break
    if "adj_close" not in out.columns:
        out["adj_close"] = out.get("close")
    out["source"] = source_label
    out["year"]   = pd.to_datetime(out["date"]).dt.year
    keep = ["symbol_id", "symbol", "date", "open", "high", "low",
            "close", "adj_close", "volume", "source", "year"]
    return out[[c for c in keep if c in out.columns]]


def write_one(df: pd.DataFrame, output_dir: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["year"],
        existing_data_behavior="overwrite_or_ignore",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir",    required=True)
    parser.add_argument("--output-dir",   default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--source-label", default="existing_cache")
    parser.add_argument("--symbols",      nargs="*")
    args = parser.parse_args()

    settings     = Settings.load()
    input_dir    = Path(args.input_dir)
    output_dir   = (
        Path(args.output_dir) if args.output_dir
        else settings.canonical_root / "facts" / "fact_underlying_bar_daily"
    )
    manifest_dir = (
        Path(args.manifest_dir) if args.manifest_dir
        else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(input_dir.rglob("*_daily_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No *_daily_*.parquet found under {input_dir}")
    if args.symbols:
        sym_set = {s.upper() for s in args.symbols}
        files = [f for f in files if sym_from_path(f) in sym_set]

    print(f"Processing {len(files)} equity file(s) one at a time (memory-safe)...")

    total_rows  = 0
    total_syms  = 0
    min_date, max_date = None, None

    for i, path in enumerate(files, 1):
        sym = sym_from_path(path)
        if not sym:
            continue
        try:
            raw = pd.read_parquet(path)
            df  = normalize(sym, raw, args.source_label)

            errs = validate_daily_bars(df)
            # Only print warnings for hard errors — minor OHLC imprecision is common
            hard = [e for e in errs if "NaN" in e or "Missing" in e or "negative" in e.lower()]
            if hard:
                print(f"  [{i}] {sym}: {'; '.join(hard)}")

            write_one(df, output_dir)

            rows = len(df)
            total_rows += rows
            total_syms += 1

            d_min = str(df["date"].min()) if not df.empty else None
            d_max = str(df["date"].max()) if not df.empty else None
            if d_min and (min_date is None or d_min < min_date): min_date = d_min
            if d_max and (max_date is None or d_max > max_date): max_date = d_max

            if i % 100 == 0 or i == len(files):
                print(f"  [{i}/{len(files)}] {sym}: {rows:,} rows  ✓")

        except Exception as e:
            print(f"  [{i}/{len(files)}] {path.name}: SKIPPED — {e}")
        finally:
            try:
                del raw, df
            except NameError:
                pass
            gc.collect()

    print(f"\n  Total: {total_rows:,} rows, {total_syms} symbols → {output_dir}")

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id(
            "existing_equity", str(input_dir), now_utc_iso()[:10]
        ),
        dataset_name="fact_underlying_bar_daily",
        source=args.source_label,
        file_path=str(input_dir),
        row_count=total_rows,
        schema_hash=build_batch_id("equity", str(total_syms)),
        min_date=min_date,
        max_date=max_date,
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)


if __name__ == "__main__":
    main()
