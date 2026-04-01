"""
ingest_short_interest.py
=========================
Fetches short interest data from FINRA's free bi-monthly data files.
Writes to canonical/facts/fact_short_interest.

FINRA publishes short interest for all US-listed securities twice monthly
(as of the 15th and end of month). Data is free and requires no API key.

URL pattern:
  https://cdn.finra.org/equity/regsho/biweekly/CNMSshvol{YYYYMMDD}.txt

Schema: symbol, settle_date, short_shares, avg_daily_volume, days_to_cover, year
"""
from __future__ import annotations
import argparse, gc, io, sys, time
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd
import requests

from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

FINRA_BASE = "https://cdn.finra.org/equity/regsho/daily"
HDR = {"User-Agent": "Mozilla/5.0 (research/market-lake)"}


def finra_dates(start_year: int = 2010) -> list[str]:
    """Generate all weekdays (Mon-Fri) from start_year to today.
    FINRA publishes daily short sale volume for each trading day."""
    import pandas as pd
    dates = pd.bdate_range(
        start=f"{start_year}-01-01",
        end=date.today().strftime("%Y-%m-%d")
    ).strftime("%Y%m%d").tolist()
    return dates


def fetch_one(date_str: str) -> pd.DataFrame | None:
    url = f"{FINRA_BASE}/CNMSshvol{date_str}.txt"
    try:
        r = requests.get(url, headers=HDR, timeout=30)
        if r.status_code in (403, 404):
            return None
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), sep="|", on_bad_lines="skip")
        # FINRA columns: symbol, short_volume, short_exempt_volume, total_volume, market
        if not any(c.lower() == "symbol" for c in df.columns):
            return None

        # Actual FINRA daily column names: Date, Symbol, ShortVolume, ShortExemptVolume, TotalVolume, Market
        df = df.rename(columns={
            "Symbol":              "symbol",
            "ShortVolume":         "short_shares",
            "TotalVolume":         "avg_daily_volume",
            "Date":                "settle_date_raw",
        })
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df["settle_date"] = pd.to_datetime(df.get("settle_date_raw", date_str).astype(str), errors="coerce").dt.date
        df = df.dropna(subset=["symbol"])
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df["symbol_id"]   = df["symbol"].map(stable_symbol_id)
        df["short_shares"]       = pd.to_numeric(df.get("short_shares",       0), errors="coerce")
        df["avg_daily_volume"]   = pd.to_numeric(df.get("avg_daily_volume",   1), errors="coerce")
        df["days_to_cover"]      = df["short_shares"] / df["avg_daily_volume"].replace(0, pd.NA)
        df["source"]  = "finra"
        df["year"]    = pd.to_datetime(df["settle_date"]).dt.year
        keep = ["symbol_id","symbol","settle_date","short_shares","avg_daily_volume","days_to_cover","source","year"]
        return df[[c for c in keep if c in df.columns]].dropna(subset=["symbol","settle_date"])
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year",   type=int,  default=2020)
    parser.add_argument("--symbols",      nargs="*", help="Filter to specific symbols")
    parser.add_argument("--output-dir",   default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--delay",        type=float, default=0.5)
    args = parser.parse_args()

    settings     = Settings.load()
    output_dir   = Path(args.output_dir)   if args.output_dir   else settings.canonical_root / "facts" / "fact_short_interest"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    output_dir.mkdir(parents=True, exist_ok=True)

    sym_filter = {s.upper() for s in args.symbols} if args.symbols else None
    dates = finra_dates(args.start_year)
    print(f"Fetching FINRA short interest: {len(dates)} daily files from {dates[0]} to {dates[-1]}")

    total_rows, min_date, max_date = 0, None, None

    for i, date_str in enumerate(dates, 1):
        df = fetch_one(date_str)
        if df is not None and not df.empty:
            if sym_filter:
                df = df[df["symbol"].isin(sym_filter)]
            if not df.empty:
                write_parquet(df, output_dir, partition_cols=["year"])
                total_rows += len(df)
                d = date_str[:4] + "-" + date_str[4:6] + "-" + date_str[6:]
                if min_date is None or d < min_date: min_date = d
                if max_date is None or d > max_date: max_date = d
        if i % 12 == 0 or i == len(dates):
            print(f"  [{i}/{len(dates)}] {total_rows:,} rows so far")
        time.sleep(args.delay)
        gc.collect()

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("short_interest", str(args.start_year), now_utc_iso()[:10]),
        dataset_name="fact_short_interest", source="finra",
        file_path=f"finra_bimonthly_{args.start_year}",
        row_count=total_rows,
        schema_hash=build_batch_id("si", str(total_rows)),
        min_date=min_date, max_date=max_date,
        ingested_at=now_utc_iso(), status="success",
    ), manifest_dir)
    print(f"\n✅ {total_rows:,} short interest rows written")

if __name__ == "__main__":
    main()
