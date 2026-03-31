from __future__ import annotations
import argparse, io, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import pandas as pd, requests, yaml
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings
from market_lake.validation.macros import validate_macro_series

CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={id}"
API_URL = "https://api.stlouisfed.org/fred/series/observations"
HDR = {"User-Agent": "Mozilla/5.0 (research/market-lake)"}

def fetch_csv(sid):
    r = requests.get(CSV_URL.format(id=sid), headers=HDR, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = ["date","value"]
    return df

def fetch_api(sid, key):
    r = requests.get(API_URL, params={"series_id":sid,"api_key":key,"file_type":"json","observation_start":"1900-01-01"}, headers=HDR, timeout=30)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    return pd.DataFrame(obs)[["date","value"]]

def normalize(sid, label, df):
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date"])
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out["series_id"] = sid.upper()
    out["label"] = label
    out["source"] = "fred"
    out["year"] = pd.to_datetime(out["date"]).dt.year
    return out[["series_id","label","date","value","source","year"]].sort_values("date")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", nargs="*")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--manifest-dir", default=None)
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()
    settings = Settings.load()
    output_dir  = Path(args.output_dir)  if args.output_dir  else settings.canonical_root/"facts"/"fact_macro_series"
    manifest_dir = Path(args.manifest_dir) if args.manifest_dir else settings.canonical_root/"metadata"/"fact_dataset_manifest"
    macro_path = settings.config_dir / "macros.yaml"
    cfg = yaml.safe_load(macro_path.read_text()) if macro_path.exists() else {}
    series_list = [{"series_id":s,"label":s} for s in args.series] if args.series else cfg.get("macro_series", [])
    if not series_list:
        print("No macro series configured.")
        return
    frames = []
    for entry in series_list:
        sid, label = entry["series_id"], entry.get("label", entry["series_id"])
        print(f"  {sid}...")
        try:
            raw = fetch_api(sid, settings.fred_api_key) if settings.fred_api_key else fetch_csv(sid)
            frames.append(normalize(sid, label, raw))
            time.sleep(args.delay)
        except Exception as e:
            print(f"  {sid}: {e}")
    df = pd.concat(frames, ignore_index=True)
    errs = validate_macro_series(df)
    if errs:
        print("Warnings: " + "; ".join(errs))
    write_parquet(df, output_dir, partition_cols=["series_id"])
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("fred", "|".join(e["series_id"] for e in series_list), now_utc_iso()[:10]),
        dataset_name="fact_macro_series", source="fred",
        file_path="fred_csv", row_count=len(df), schema_hash=schema_hash_for_frame(df),
        min_date=str(df["date"].min()) if not df.empty else None,
        max_date=str(df["date"].max()) if not df.empty else None,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df):,} macro rows ({df['series_id'].nunique()} series)")

if __name__ == "__main__":
    main()
