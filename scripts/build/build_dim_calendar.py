from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

def build_calendar(start: str, end: str) -> pd.DataFrame:
    days = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"date": days[days.weekday < 5]})
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["quarter"] = df["date"].dt.quarter
    df["day_of_week"] = df["date"].dt.day_name()
    df["week_of_year"] = df["date"].dt.isocalendar().week.astype(int)
    df["is_month_end"] = df["date"].dt.is_month_end
    df["is_quarter_end"] = df["date"].dt.is_quarter_end
    df["is_year_end"] = df["date"].dt.is_year_end
    df["date"] = df["date"].dt.date
    return df

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2005-01-01")
    parser.add_argument("--end", default="2035-12-31")
    args = parser.parse_args()
    settings = Settings.load()
    output_dir = settings.canonical_root / "dimensions" / "dim_calendar"
    manifest_dir = settings.canonical_root / "metadata" / "fact_dataset_manifest"
    df = build_calendar(args.start, args.end)
    write_parquet(df, output_dir, filename="dim_calendar.parquet")
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("dim_calendar", args.start, args.end),
        dataset_name="dim_calendar", source="generated",
        file_path="", row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=args.start, max_date=args.end,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df)} calendar rows to {output_dir}")

if __name__ == "__main__":
    main()
