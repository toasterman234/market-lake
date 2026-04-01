"""
ingest_corporate_actions.py
============================
Fetches stock splits and dividends for all symbols in dim_symbol
via yfinance and writes to canonical/facts/fact_corporate_action.

Processes one symbol at a time (memory-safe).
"""
from __future__ import annotations
import argparse, gc, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import pandas as pd
import yfinance as yf

from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings


def fetch_actions(symbol: str) -> pd.DataFrame | None:
    try:
        t = yf.Ticker(symbol)
        actions = t.actions  # dividends + splits combined
        if actions is None or actions.empty:
            return None
        df = actions.reset_index()
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]
        df = df.rename(columns={"date": "action_date"})
        df["action_date"] = pd.to_datetime(df["action_date"]).dt.date
        df["symbol"]      = symbol.upper()
        df["symbol_id"]   = stable_symbol_id(symbol)

        rows = []
        if "dividends" in df.columns:
            div = df[df["dividends"] > 0][["symbol_id","symbol","action_date","dividends"]].copy()
            div["action_type"] = "dividend"
            div["value"]       = div["dividends"]
            div["split_ratio"] = None
            rows.append(div[["symbol_id","symbol","action_date","action_type","value","split_ratio"]])
        if "stock_splits" in df.columns:
            splits = df[df["stock_splits"] > 0][["symbol_id","symbol","action_date","stock_splits"]].copy()
            splits["action_type"] = "split"
            splits["value"]       = splits["stock_splits"]
            splits["split_ratio"] = splits["stock_splits"].apply(lambda x: f"{x:.0f}:1" if x > 1 else f"1:{1/x:.0f}")
            rows.append(splits[["symbol_id","symbol","action_date","action_type","value","split_ratio"]])

        if not rows:
            return None
        out = pd.concat(rows, ignore_index=True).sort_values("action_date")
        out["year"] = pd.to_datetime(out["action_date"]).dt.year
        return out
    except Exception as e:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols",      nargs="*")
    parser.add_argument("--output-dir",   default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--delay",        type=float, default=0.5)
    args = parser.parse_args()

    settings     = Settings.load()
    output_dir   = Path(args.output_dir)   if args.output_dir   else settings.canonical_root / "facts" / "fact_corporate_action"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get symbol list
    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        import duckdb
        db = duckdb.connect(":memory:")
        symbols = db.execute(f"""
            SELECT symbol FROM read_parquet('{settings.canonical_root}/dimensions/dim_symbol/**/*.parquet', union_by_name=true)
            WHERE asset_type = 'stock' ORDER BY symbol
        """).df()["symbol"].tolist()
        db.close()

    print(f"Fetching corporate actions for {len(symbols)} symbols...")

    total_rows, done, with_actions = 0, 0, 0
    min_date, max_date = None, None

    for i, sym in enumerate(symbols, 1):
        try:
            df = fetch_actions(sym)
            if df is not None and not df.empty:
                write_parquet(df, output_dir, partition_cols=["year"])
                total_rows += len(df)
                with_actions += 1
                d_min = str(df["action_date"].min())
                d_max = str(df["action_date"].max())
                if min_date is None or d_min < min_date: min_date = d_min
                if max_date is None or d_max > max_date: max_date = d_max
            done += 1
            if i % 50 == 0 or i == len(symbols):
                print(f"  [{i}/{len(symbols)}] {done} done, {with_actions} had actions, {total_rows:,} rows total")
            time.sleep(args.delay)
        except Exception as e:
            print(f"  [{i}] {sym}: ERROR — {e}")
        finally:
            try: del df
            except NameError: pass
            gc.collect()

    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("corporate_actions", now_utc_iso()[:10]),
        dataset_name="fact_corporate_action", source="yfinance",
        file_path="yfinance/actions", row_count=total_rows,
        schema_hash=build_batch_id("ca", str(total_rows)),
        min_date=min_date, max_date=max_date,
        ingested_at=now_utc_iso(), status="success",
    ), manifest_dir)
    print(f"\n✅ Done: {total_rows:,} rows, {with_actions} symbols had actions")

if __name__ == "__main__":
    main()
