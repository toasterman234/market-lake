#!/usr/bin/env bash
# load_existing_data.sh
# Absorbs pre-existing market data from other projects into market-lake.
# Edit paths below to match your local cache locations.
# Run from repo root: bash examples/load_existing_data.sh

set -e
cd "$(dirname "$0")/.."
export MARKET_LAKE_ROOT="$(pwd)"

echo "=== market-lake: Loading existing data ==="

echo "1/3  Ingesting equity bars (alphaquant cache)..."
python scripts/ingest/ingest_existing_equity.py \
    --input-dir "/Volumes/Extra Storage Crucial 1TB SSD/Projects-archive-20260330/Trading/alphaquant-cursor-clone/alphaquant-vectorbt-sprint2/cache/history" \
    --source-label "alphaquant_cache"

echo "2/3  Ingesting VRP features (archive vrp_clean)..."
python scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir "/Volumes/Extra Storage Crucial 1TB SSD/Projects-archive-20260330/Trading/options data/data/vrp_clean" \
    --output-dir "canonical/features/fact_option_feature_daily"

echo "3/3  Fetching FRED macro series..."
python scripts/ingest/ingest_fred_macro.py

echo ""
echo "Done. Run: python examples/query_examples.py"
