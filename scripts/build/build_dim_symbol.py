from __future__ import annotations
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
import yaml
from market_lake.ids.symbol_map import build_dim_symbol
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, schema_hash_for_frame, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--asset-type", nargs="*")
    args = parser.parse_args()
    settings = Settings.load()
    output_dir = settings.canonical_root / "dimensions" / "dim_symbol"
    manifest_dir = settings.canonical_root / "metadata" / "fact_dataset_manifest"
    sym_path = settings.config_dir / "symbols.yaml"
    cfg = yaml.safe_load(sym_path.read_text()) if sym_path.exists() else {}
    symbols = args.symbols or cfg.get("symbols", [])
    asset_type_map = cfg.get("asset_types", {})
    aliases = cfg.get("symbol_aliases", {})
    if args.symbols and args.asset_type:
        asset_type_map = dict(zip([s.upper() for s in args.symbols], args.asset_type))
    if not symbols:
        print("No symbols configured.")
        return
    df = build_dim_symbol(symbols, asset_type_map=asset_type_map, aliases=aliases)
    write_parquet(df, output_dir, filename="dim_symbol.parquet")
    write_manifest(ManifestRecord(
        ingest_batch_id=build_batch_id("dim_symbol", str(sorted(symbols))),
        dataset_name="dim_symbol", source="config",
        file_path=str(sym_path), row_count=len(df),
        schema_hash=schema_hash_for_frame(df), min_date=None, max_date=None,
        ingested_at=now_utc_iso(), status="success"), manifest_dir)
    print(f"  Wrote {len(df)} symbols to {output_dir}")

if __name__ == "__main__":
    main()
