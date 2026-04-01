#!/bin/bash
# daily_refresh.sh — market-lake daily data pipeline
# Runs every weekday at 6:45am via launchd.
# Install: launchctl load ~/Library/LaunchAgents/com.market-lake.daily-refresh.plist

set -e
MARKET_LAKE="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Infrastructure/Master Data Folder/market-lake"
OPTIONS_RESEARCH="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/options-research"
THETA_DIR="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/ThetaTerminal"
PYTHON="$MARKET_LAKE/.venv/bin/python3"
LOG="/tmp/market_lake_daily_$(date +%Y-%m-%d).log"
export MARKET_LAKE_ROOT="$MARKET_LAKE"

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }
log "=== market-lake daily refresh ==="

# 1. Ensure ThetaTerminal is running
if ! curl -s http://127.0.0.1:25503/health > /dev/null 2>&1; then
    log "Starting ThetaTerminal..."
    cd "$THETA_DIR" && java -jar ThetaTerminalv3.jar --creds-file creds.txt > /tmp/theta.log 2>&1 &
    sleep 15
fi

# 2. VRP gap-fill (all 513 symbols → yesterday)
log "VRP gap-fill..."
cd "$OPTIONS_RESEARCH"
.venv/bin/python scripts/daily_gapfill.py --workers 4 2>&1 | tee -a "$LOG"

# 3. Ingest updated VRP into market-lake
log "Ingesting VRP..."
cd "$MARKET_LAKE"
"$PYTHON" scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir "$OPTIONS_RESEARCH/data/vrp_clean" \
    --output-dir "$MARKET_LAKE/canonical/features/fact_option_feature_daily" 2>&1 | tee -a "$LOG"

# 4. Equity bars top-up (core 38 symbols)
log "Equity bars top-up..."
"$PYTHON" scripts/ingest/ingest_yahoo_daily_bars.py \
    --symbols SPY QQQ IWM TLT GLD DIA XLK XLF XLV XLE XLI XLP XLU XLY XLB XLRE \
              AAPL MSFT NVDA AMZN GOOGL META TSLA JPM BAC GS COST LLY AVGO AMD NFLX \
    --start "$(date -v-5d +%Y-%m-%d 2>/dev/null || date -d '5 days ago' +%Y-%m-%d)" \
    --end "$(date +%Y-%m-%d)" 2>&1 | tee -a "$LOG"

# 5. FRED macro (picks up latest daily series)
log "FRED macro refresh..."
"$PYTHON" scripts/ingest/ingest_fred_macro.py 2>&1 | tee -a "$LOG"

# 6. Re-bootstrap DuckDB views
"$PYTHON" scripts/build/bootstrap_duckdb.py 2>&1 | tee -a "$LOG"

# 7. dbt rebuild
log "dbt run..."
cd "$MARKET_LAKE/dbt"
"$MARKET_LAKE/.venv/bin/dbt" run --profiles-dir "$(pwd)" 2>&1 | tee -a "$LOG"

log "=== Done. Log: $LOG ==="
