#!/bin/bash
# daily_refresh.sh — market-lake daily data pipeline
# Runs every weekday at 6:45am via launchd.
# Install: launchctl load ~/Library/LaunchAgents/com.market-lake.daily-refresh.plist
#
# Pipeline:
#   1. ThetaTerminal health check / start
#   2. VRP gap-fill (options-research)
#   3. Ingest updated VRP into market-lake
#   4. Equity bars top-up (38 core symbols)
#   5. FRED macro refresh
#   6. Re-bootstrap DuckDB
#   7. dbt run (all 17 models)
#   8. dbt test (schema + uniqueness tests)
#   9. pytest (60+ unit + integration tests)
#  10. Row count anomaly check
#  11. Health dashboard snapshot

set -euo pipefail

MARKET_LAKE="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Infrastructure/Master Data Folder/market-lake"
OPTIONS_RESEARCH="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/options-research"
THETA_DIR="/Volumes/Extra Storage Crucial 1TB SSD/Projects/Trading/ThetaTerminal"
PYTHON="$MARKET_LAKE/.venv/bin/python3"
DBT="$MARKET_LAKE/.venv/bin/dbt"
LOG="/tmp/market_lake_daily_$(date +%Y-%m-%d).log"
export MARKET_LAKE_ROOT="$MARKET_LAKE"

PASS=0
FAIL=0

log() { echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG"; }

step() {
    # Usage: step "label" command...
    local label="$1"; shift
    log "▸ $label"
    if "$@" 2>&1 | tee -a "$LOG"; then
        log "  ✅ $label"
        PASS=$((PASS + 1))
    else
        log "  ❌ $label FAILED (exit $?)"
        FAIL=$((FAIL + 1))
        # Don't abort — run all steps and report at the end
    fi
}

log "=== market-lake daily refresh ==="

# ── 1. ThetaTerminal ─────────────────────────────────────────────────────────
if ! curl -s http://127.0.0.1:25503/health > /dev/null 2>&1; then
    log "  Starting ThetaTerminal..."
    cd "$THETA_DIR" && java -jar ThetaTerminalv3.jar --creds-file creds.txt > /tmp/theta.log 2>&1 &
    sleep 15
    curl -s http://127.0.0.1:25503/health > /dev/null 2>&1 && log "  ✅ ThetaTerminal started" || log "  ❌ ThetaTerminal failed to start"
else
    log "  ✅ ThetaTerminal already running"
fi

# ── 2. VRP gap-fill ──────────────────────────────────────────────────────────
step "VRP gap-fill" bash -c "cd '$OPTIONS_RESEARCH' && .venv/bin/python scripts/daily_gapfill.py --workers 4"

# ── 3. Ingest VRP into market-lake ───────────────────────────────────────────
step "Ingest VRP features" "$PYTHON" scripts/ingest/ingest_theta_vrp_features.py \
    --input-dir "$OPTIONS_RESEARCH/data/vrp_clean" \
    --output-dir "$MARKET_LAKE/canonical/features/fact_option_feature_daily"

# ── 4. Equity bars top-up ────────────────────────────────────────────────────
START_DATE="$(date -v-5d +%Y-%m-%d 2>/dev/null || date -d '5 days ago' +%Y-%m-%d)"
step "Equity bars top-up" "$PYTHON" scripts/ingest/ingest_yahoo_daily_bars.py \
    --symbols SPY QQQ IWM TLT GLD DIA XLK XLF XLV XLE XLI XLP XLU XLY XLB XLRE \
              AAPL MSFT NVDA AMZN GOOGL META TSLA JPM BAC GS COST LLY AVGO AMD NFLX \
    --start "$START_DATE" \
    --end "$(date +%Y-%m-%d)"

# ── 5. FRED macro refresh ─────────────────────────────────────────────────────
step "FRED macro refresh" "$PYTHON" scripts/ingest/ingest_fred_macro.py

# ── 6. DuckDB bootstrap ───────────────────────────────────────────────────────
step "Bootstrap DuckDB" "$PYTHON" scripts/build/bootstrap_duckdb.py

# ── 7. dbt run ────────────────────────────────────────────────────────────────
step "dbt run" bash -c "cd '$MARKET_LAKE/dbt' && '$DBT' run --profiles-dir '$(pwd)'"

# ── 8. dbt test ───────────────────────────────────────────────────────────────
step "dbt test" bash -c "cd '$MARKET_LAKE/dbt' && '$DBT' test --profiles-dir '$(pwd)'"

# ── 9. pytest ─────────────────────────────────────────────────────────────────
step "pytest" bash -c "cd '$MARKET_LAKE' && '$PYTHON' -m pytest tests/ -q --tb=short"

# ── 10. Row count anomaly check ───────────────────────────────────────────────
step "Row count check" "$PYTHON" scripts/ops/row_count_check.py

# ── 11. Health dashboard ──────────────────────────────────────────────────────
log "▸ Health snapshot"
"$PYTHON" scripts/ops/health_check.py 2>&1 | tee -a "$LOG" || FAIL=$((FAIL + 1))

# ── Summary ───────────────────────────────────────────────────────────────────
log ""
log "=== Daily refresh complete ==="
log "   PASSED: $PASS  FAILED: $FAIL"
log "   Full log: $LOG"

if [ "$FAIL" -gt 0 ]; then
    log "   ❌ $FAIL step(s) failed — review log above"
    exit 1
fi
log "   ✅ All steps passed"
exit 0
