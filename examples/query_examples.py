"""
market-lake query examples
Run from repo root after ingest is complete:
    MARKET_LAKE_ROOT=$(pwd) python examples/query_examples.py
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from market_lake.io.duckdb_conn import open_db
from market_lake.settings import Settings


def run(con, label: str, sql: str) -> None:
    print(f"\n{'─'*60}\n  {label}\n{'─'*60}")
    try:
        print(con.execute(sql).df().to_string(index=False))
    except Exception as e:
        print(f"  ⚠  {e}")


def main() -> None:
    settings = Settings.load()
    with open_db(settings.duckdb_path) as con:
        run(con, "Latest close prices (top 10)", """
            SELECT symbol, date, close, volume, source
            FROM canonical.vw_prices_daily
            WHERE date = (SELECT MAX(date) FROM canonical.vw_prices_daily)
            ORDER BY symbol LIMIT 10
        """)
        run(con, "SPY daily bar count + date range", """
            SELECT symbol, COUNT(*) AS trading_days, MIN(date) AS first, MAX(date) AS last
            FROM canonical.vw_prices_daily WHERE symbol = 'SPY' GROUP BY symbol
        """)
        run(con, "Latest VRP snapshot — top 10 by IVR", """
            SELECT symbol, date, iv_30d, ivr_252d, ivp_252d, vrp_30d, put_skew_25d
            FROM features.vw_option_features_daily
            WHERE date = (SELECT MAX(date) FROM features.vw_option_features_daily)
            ORDER BY ivr_252d DESC NULLS LAST LIMIT 10
        """)
        run(con, "Latest macro readings", """
            SELECT series_id, label, date, value
            FROM canonical.vw_macro_series
            WHERE (series_id, date) IN (
                SELECT series_id, MAX(date) FROM canonical.vw_macro_series GROUP BY series_id
            ) ORDER BY series_id
        """)
        run(con, "Regime panel — last 5 days", """
            SELECT date, spy_close, vix, vol_regime, trend_regime, yield_curve_regime,
                   fed_funds_rate, rate_10y
            FROM main_marts.mart_regime_panel ORDER BY date DESC LIMIT 5
        """)
        run(con, "Screening panel — top 15 by IVR rank today", """
            SELECT symbol, date, ROUND(ivr_252d,3) AS ivr, ROUND(vrp_30d,4) AS vrp,
                   ROUND(ivr_rank,3) AS ivr_rank, ROUND(vrp_rank,3) AS vrp_rank
            FROM main_marts.mart_screening_panel
            WHERE date = (SELECT MAX(date) FROM main_marts.mart_screening_panel)
            ORDER BY ivr_rank DESC NULLS LAST LIMIT 15
        """)
        run(con, "Ingest manifest", """
            SELECT dataset_name, source, row_count, min_date, max_date, status
            FROM metadata.vw_dataset_manifest ORDER BY ingested_at DESC
        """)
        print("\n✅ Query examples complete.")


if __name__ == "__main__":
    main()
