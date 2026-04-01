"""
Ingest Kenneth French Data Library factor files into
canonical/facts/fact_ff_factors_daily.

Free download from: https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html

Handles both pre-existing CSV files (ff5_daily.csv, momentum_daily.csv)
and direct downloads from the French library ZIP files.

Usage:
    # From existing files:
    python scripts/ingest/ingest_ff_factors.py \
        --input-files /path/to/ff5_daily.csv /path/to/momentum_daily.csv

    # Download fresh:
    python scripts/ingest/ingest_ff_factors.py --download
"""
from __future__ import annotations

import argparse
import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd
import requests

from market_lake.io.manifests import (
    ManifestRecord, build_batch_id, now_utc_iso, write_manifest,
)
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

HDR = {"User-Agent": "Mozilla/5.0 (research/market-lake)"}

FF_SOURCES = {
    "ff5_daily": {
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
        "file_in_zip": "F-F_Research_Data_5_Factors_2x3_daily.CSV",
        "factors": ["mkt_rf", "smb", "hml", "rmw", "cma", "rf"],
        "raw_cols": ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"],
    },
    "momentum_daily": {
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip",
        "file_in_zip": "F-F_Momentum_Factor_daily.CSV",
        "factors": ["mom"],
        "raw_cols": ["Mom"],
    },
    "ff3_daily": {
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_daily_CSV.zip",
        "file_in_zip": "F-F_Research_Data_Factors_daily.CSV",
        "factors": ["mkt_rf", "smb", "hml", "rf"],
        "raw_cols": ["Mkt-RF", "SMB", "HML", "RF"],
    },
}


def parse_french_csv(content: str, raw_cols: list[str], factors: list[str]) -> pd.DataFrame:
    """Parse Kenneth French CSV format — skips header text, reads data section."""
    lines = content.splitlines()
    # Find the data start: look for a line that starts with a 8-digit date
    start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and stripped[:8].isdigit() and len(stripped[:8]) == 8:
            start = i
            break

    # Find data end (blank line or next header section after data)
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i].strip()
        if not stripped or (stripped and not stripped[:1].isdigit() and stripped[:1] != '-'):
            end = i
            break

    data_lines = lines[start:end]
    if not data_lines:
        raise ValueError("Could not parse any data rows from French CSV")

    text = "\n".join(data_lines)
    df = pd.read_csv(
        io.StringIO(text),
        header=None,
        sep=r"\s+",
        skipinitialspace=True,
    )

    # Assign column names: first col is date, rest are factors
    col_names = ["date_raw"] + raw_cols[:df.shape[1]-1]
    df.columns = col_names[:df.shape[1]]

    # Parse date (YYYYMMDD format)
    df["date"] = pd.to_datetime(df["date_raw"].astype(str), format="%Y%m%d", errors="coerce").dt.date
    df = df.dropna(subset=["date"])

    # Rename factor columns
    rename = dict(zip(raw_cols, factors))
    df = df.rename(columns=rename)

    # Convert to decimal (French data is in percent — divide by 100)
    for col in factors:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 100.0

    keep = ["date"] + [f for f in factors if f in df.columns]
    return df[keep].sort_values("date").reset_index(drop=True)


def download_french(key: str) -> pd.DataFrame:
    meta = FF_SOURCES[key]
    print(f"  Downloading {key} from French library...")
    resp = requests.get(meta["url"], headers=HDR, timeout=60)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        fname = meta["file_in_zip"]
        # Try to find the file case-insensitively
        match = next((n for n in z.namelist() if n.lower() == fname.lower()), None)
        if not match:
            match = z.namelist()[0]  # fallback to first file
        content = z.read(match).decode("latin-1")
    return parse_french_csv(content, meta["raw_cols"], meta["factors"])


def load_existing(path: Path, key: str) -> pd.DataFrame:
    meta = FF_SOURCES[key]
    print(f"  Loading {key} from {path}...")
    content = path.read_text(encoding="latin-1")
    return parse_french_csv(content, meta["raw_cols"], meta["factors"])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true",
                        help="Download fresh from French library")
    parser.add_argument("--input-files", nargs="*",
                        help="Existing CSV files to load (ff5_daily.csv, momentum_daily.csv)")
    parser.add_argument("--output-dir",   default=None)
    parser.add_argument("--manifest-dir", default=None)
    args = parser.parse_args()

    settings     = Settings.load()
    output_dir   = Path(args.output_dir)   if args.output_dir   else settings.canonical_root / "facts" / "fact_ff_factors_daily"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root / "metadata" / "fact_dataset_manifest"

    all_frames: dict[str, pd.DataFrame] = {}

    if args.download:
        for key in FF_SOURCES:
            try:
                all_frames[key] = download_french(key)
            except Exception as e:
                print(f"  {key}: FAILED — {e}")

    elif args.input_files:
        for path_str in args.input_files:
            path = Path(path_str)
            # Guess which key this file corresponds to
            key = None
            for k in FF_SOURCES:
                if k.lower().replace("_", "") in path.stem.lower().replace("_", "").replace("-", ""):
                    key = k
                    break
            if not key:
                # Try by factor columns
                if "mom" in path.stem.lower():
                    key = "momentum_daily"
                elif "5" in path.stem or "five" in path.stem.lower():
                    key = "ff5_daily"
                else:
                    key = "ff3_daily"
            try:
                all_frames[key] = load_existing(path, key)
                print(f"    {len(all_frames[key]):,} rows loaded")
            except Exception as e:
                print(f"  {path.name}: FAILED — {e}")
    else:
        # Default: try to load from alphaquant cache
        cache_path = Path("/Volumes/Extra Storage Crucial 1TB SSD/Projects-archive-20260330/Trading/alphaquant-cursor-clone/alphaquant-vectorbt-sprint2/cache/fama_french")
        for fname, key in [("ff5_daily.csv", "ff5_daily"), ("momentum_daily.csv", "momentum_daily")]:
            p = cache_path / fname
            if p.exists():
                try:
                    all_frames[key] = load_existing(p, key)
                    print(f"  {fname}: {len(all_frames[key]):,} rows")
                except Exception as e:
                    print(f"  {fname}: FAILED — {e}")

    if not all_frames:
        print("No data loaded. Use --download or --input-files.")
        return

    # Merge all factor sets on date
    combined = None
    for key, df in all_frames.items():
        df = df.add_suffix(f"") if combined is None else df
        if combined is None:
            combined = df
        else:
            combined = combined.merge(df, on="date", how="outer", suffixes=("", f"_{key}"))

    combined = combined.sort_values("date").reset_index(drop=True)
    combined["year"]  = pd.to_datetime(combined["date"]).dt.year
    combined["month"] = pd.to_datetime(combined["date"]).dt.month
    combined["source"] = "french_library"

    write_parquet(combined, output_dir, filename="ff_factors_daily.parquet")

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("ff_factors", now_utc_iso()[:10]),
        dataset_name="fact_ff_factors_daily",
        source="french_library",
        file_path=str(list(all_frames.keys())),
        row_count=len(combined),
        schema_hash=build_batch_id("ff", str(list(combined.columns))),
        min_date=str(combined["date"].min()),
        max_date=str(combined["date"].max()),
        ingested_at=now_utc_iso(),
        status="success",
    ), manifest_dir)

    print(f"\n  ✅ {len(combined):,} rows, columns: {[c for c in combined.columns if c not in ('date','year','month','source')]}")
    print(f"     Coverage: {combined['date'].min()} → {combined['date'].max()}")
    print(f"     Output: {output_dir}")


if __name__ == "__main__":
    main()
