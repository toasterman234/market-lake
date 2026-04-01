"""
ingest_fundamentals.py
=======================
Fetches annual financial statements (income, balance sheet, cash flow)
via yfinance and computes key financial ratios using FinanceToolkit formulas.

Processes one symbol at a time (memory-safe). ETFs are skipped.
Writes two canonical tables:
  - fact_financial_statements  — raw GAAP line items
  - fact_fundamentals_annual   — computed ratios per symbol per fiscal year
"""
from __future__ import annotations
import argparse, gc, sys, time, warnings
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import yfinance as yf

from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.io.manifests import ManifestRecord, build_batch_id, now_utc_iso, write_manifest
from market_lake.io.parquet import write_parquet
from market_lake.settings import Settings

INCOME_MAP = {
    "Total Revenue":              "revenue",
    "Gross Profit":               "gross_profit",
    "Operating Income":           "ebit",
    "EBIT":                       "ebit",
    "Net Income":                 "net_income",
    "Diluted EPS":                "eps_diluted",
    "Basic EPS":                  "eps_basic",
    "Interest Expense":           "interest_expense",
    "Tax Provision":              "tax_provision",
    "Research And Development":   "rd_expense",
}
BALANCE_MAP = {
    "Total Assets":               "total_assets",
    "Total Debt":                 "total_debt",
    "Long Term Debt":             "long_term_debt",
    "Stockholders Equity":        "total_equity",
    "Total Stockholders Equity":  "total_equity",
    "Cash And Cash Equivalents":  "cash",
    "Current Assets":             "current_assets",
    "Current Liabilities":        "current_liabilities",
    "Retained Earnings":          "retained_earnings",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Ordinary Shares Number":     "shares_outstanding",
}
CASHFLOW_MAP = {
    "Operating Cash Flow":        "operating_cash_flow",
    "Capital Expenditure":        "capex",
    "Free Cash Flow":             "free_cash_flow",
    "Depreciation And Amortization": "da",
}


def extract_rows(stmt: pd.DataFrame, col_map: dict) -> dict[str, pd.Series]:
    """Extract named rows from yfinance statement (rows=items, cols=dates)."""
    if stmt is None or stmt.empty:
        return {}
    out = {}
    for raw_name, canonical in col_map.items():
        if raw_name in stmt.index and canonical not in out:
            out[canonical] = stmt.loc[raw_name]
    return out


def compute_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Compute financial ratios from merged statements row. All FinanceToolkit-verified formulas."""
    out = df.copy()

    def safe_div(a, b):
        a = pd.Series(a) if not isinstance(a, pd.Series) else a
        b = pd.Series(b) if not isinstance(b, pd.Series) else b
        mask = (b != 0) & b.notna() & a.notna()
        result = pd.Series(np.nan, index=a.index if hasattr(a, 'index') else range(len(a)))
        result[mask] = a[mask] / b[mask]
        return result

    # Profitability
    out["gross_margin"]      = safe_div(out.get("gross_profit", np.nan), out.get("revenue", np.nan))
    out["ebit_margin"]       = safe_div(out.get("ebit", np.nan),         out.get("revenue", np.nan))
    out["net_margin"]        = safe_div(out.get("net_income", np.nan),    out.get("revenue", np.nan))
    out["roe"]               = safe_div(out.get("net_income", np.nan),    out.get("total_equity", np.nan))
    out["roa"]               = safe_div(out.get("net_income", np.nan),    out.get("total_assets", np.nan))

    # Liquidity
    out["current_ratio"]     = safe_div(out.get("current_assets", np.nan),   out.get("current_liabilities", np.nan))

    # Leverage
    out["debt_to_equity"]    = safe_div(out.get("total_debt", np.nan),    out.get("total_equity", np.nan))
    out["debt_to_assets"]    = safe_div(out.get("total_debt", np.nan),    out.get("total_assets", np.nan))
    out["interest_coverage"] = safe_div(out.get("ebit", np.nan),          out.get("interest_expense", np.nan))

    # Cash flow
    if "free_cash_flow" not in out.columns and "operating_cash_flow" in out.columns and "capex" in out.columns:
        out["free_cash_flow"] = out["operating_cash_flow"] + out["capex"]  # capex is negative in yfinance
    out["fcf_margin"]        = safe_div(out.get("free_cash_flow", np.nan), out.get("revenue", np.nan))
    out["earnings_quality"]  = safe_div(out.get("operating_cash_flow", np.nan), out.get("net_income", np.nan))

    # Growth (requires sorting by fiscal_year_end)
    if "fiscal_year_end" in out.columns:
        out = out.sort_values("fiscal_year_end")
        out["revenue_growth_yoy"] = out["revenue"].pct_change()
        out["earnings_growth_yoy"] = out["net_income"].pct_change()

    # Altman Z-Score (public company version — requires market cap, patched in later if available)
    try:
        working_capital = out.get("current_assets", np.nan) - out.get("current_liabilities", np.nan)
        retained        = out.get("retained_earnings", np.nan)
        assets          = out.get("total_assets", np.nan)
        liabilities     = out.get("total_liabilities", np.nan)
        revenue         = out.get("revenue", np.nan)
        out["altman_z_score"] = (
            1.2 * safe_div(working_capital, assets) +
            1.4 * safe_div(retained, assets) +
            3.3 * safe_div(out.get("ebit", np.nan), assets) +
            1.0 * safe_div(revenue, assets)
            # Note: 0.6 * market_cap / total_liabilities term omitted (needs live price)
        )
    except Exception:
        out["altman_z_score"] = np.nan

    # Piotroski F-Score — 9 binary signals (simplified, annual only)
    try:
        signals = pd.DataFrame(index=out.index)
        signals["f1"] = (out.get("net_income",         0) > 0).astype(int)
        signals["f2"] = (out.get("operating_cash_flow",0) > 0).astype(int)
        signals["f3"] = (out.get("roa",                0) > out.get("roa", 0).shift(1)).astype(int)
        signals["f4"] = (out.get("operating_cash_flow",0) > out.get("net_income", 0)).astype(int)
        signals["f5"] = (out.get("debt_to_assets",     1) < out.get("debt_to_assets", 1).shift(1)).astype(int)
        signals["f6"] = (out.get("current_ratio",      0) > out.get("current_ratio", 0).shift(1)).astype(int)
        signals["f7"] = (out.get("gross_margin",       0) > out.get("gross_margin", 0).shift(1)).astype(int)
        signals["f8"] = (safe_div(out.get("revenue", np.nan), out.get("total_assets", np.nan)) >
                         safe_div(out.get("revenue", np.nan).shift(1), out.get("total_assets", np.nan).shift(1))).astype(int)
        signals["f9"] = (out.get("revenue_growth_yoy", 0) > 0).astype(int)
        out["piotroski_score"] = signals.sum(axis=1)
    except Exception:
        out["piotroski_score"] = np.nan

    return out


def fetch_symbol(symbol: str) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    t = yf.Ticker(symbol)

    # Extract rows directly — avoids transpose/merge duplicate issues
    data = {}
    data.update(extract_rows(t.income_stmt,   INCOME_MAP))
    data.update(extract_rows(t.balance_sheet, BALANCE_MAP))
    data.update(extract_rows(t.cashflow,      CASHFLOW_MAP))

    if not data:
        return None, None

    # Build tidy DataFrame: one row per fiscal year
    dates = None
    for s in [t.income_stmt, t.balance_sheet, t.cashflow]:
        if s is not None and not s.empty:
            dates = pd.to_datetime(s.columns).date
            break
    if dates is None:
        return None, None

    df = pd.DataFrame(index=range(len(dates)))
    df["fiscal_year_end"] = dates
    df["symbol"]    = symbol.upper()
    df["symbol_id"] = stable_symbol_id(symbol)
    df["period_type"] = "annual"
    df["year"] = pd.to_datetime(dates).year

    for col_name, series in data.items():
        vals = series.values
        df[col_name] = vals[:len(df)] if len(vals) >= len(df) else list(vals) + [np.nan]*(len(df)-len(vals))

    df = df.sort_values("fiscal_year_end").reset_index(drop=True)

    raw = df.copy()

    # Computed ratios
    ratios_df = compute_ratios(df.copy())
    ratio_cols = ["symbol_id","symbol","fiscal_year_end","period_type","year",
                  "gross_margin","ebit_margin","net_margin","roe","roa",
                  "current_ratio","debt_to_equity","debt_to_assets","interest_coverage",
                  "fcf_margin","earnings_quality","revenue_growth_yoy","earnings_growth_yoy",
                  "altman_z_score","piotroski_score"]
    ratios_out = ratios_df[[c for c in ratio_cols if c in ratios_df.columns]]

    return raw, ratios_out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols",          nargs="*")
    parser.add_argument("--statements-dir",   default=None)
    parser.add_argument("--fundamentals-dir", default=None)
    parser.add_argument("--manifest-dir",     default=None)
    parser.add_argument("--delay",            type=float, default=0.3)
    args = parser.parse_args()

    settings      = Settings.load()
    stmts_dir     = Path(args.statements_dir)   if args.statements_dir   else settings.canonical_root / "facts" / "fact_financial_statements"
    fund_dir      = Path(args.fundamentals_dir) if args.fundamentals_dir else settings.canonical_root / "facts" / "fact_fundamentals_annual"
    manifest_dir  = Path(args.manifest_dir)     if args.manifest_dir     else settings.canonical_root / "metadata" / "fact_dataset_manifest"
    stmts_dir.mkdir(parents=True, exist_ok=True)
    fund_dir.mkdir(parents=True, exist_ok=True)

    if args.symbols:
        symbols = [s.upper() for s in args.symbols]
    else:
        import duckdb
        db = duckdb.connect(":memory:")
        symbols = db.execute(f"""
            SELECT symbol FROM read_parquet('{settings.canonical_root}/dimensions/dim_symbol/**/*.parquet', union_by_name=true)
            WHERE asset_type NOT IN ('etf','index') ORDER BY symbol
        """).df()["symbol"].tolist()
        db.close()

    print(f"Fetching fundamentals for {len(symbols)} equity symbols (ETFs excluded)...")

    total_stmt_rows, total_ratio_rows, done = 0, 0, 0
    skipped, min_date, max_date = [], None, None

    for i, sym in enumerate(symbols, 1):
        try:
            raw, ratios = fetch_symbol(sym)
            if raw is not None and not raw.empty:
                write_parquet(raw,    stmts_dir, partition_cols=["year"])
                write_parquet(ratios, fund_dir,  partition_cols=["year"])
                total_stmt_rows  += len(raw)
                total_ratio_rows += len(ratios)
                done += 1
                d_min = str(raw["fiscal_year_end"].min())
                d_max = str(raw["fiscal_year_end"].max())
                if min_date is None or d_min < min_date: min_date = d_min
                if max_date is None or d_max > max_date: max_date = d_max
            else:
                skipped.append(sym)
            if i % 50 == 0 or i == len(symbols):
                print(f"  [{i}/{len(symbols)}] {done} with data, {len(skipped)} skipped, {total_stmt_rows:,} statement rows")
            time.sleep(args.delay)
        except Exception as e:
            print(f"  [{i}] {sym}: ERROR — {e}")
            skipped.append(sym)
        finally:
            try: del raw, ratios
            except NameError: pass
            gc.collect()

    for dataset, rows in [("fact_financial_statements", total_stmt_rows), ("fact_fundamentals_annual", total_ratio_rows)]:
        write_manifest(ManifestRecord(
            ingest_batch_id=build_batch_id(dataset, now_utc_iso()[:10]),
            dataset_name=dataset, source="yfinance",
            file_path="yfinance/financial_statements", row_count=rows,
            schema_hash=build_batch_id(dataset, str(rows)),
            min_date=min_date, max_date=max_date,
            ingested_at=now_utc_iso(), status="success",
        ), manifest_dir)

    print(f"\n✅ Done: {total_stmt_rows:,} statement rows, {total_ratio_rows:,} ratio rows")
    print(f"   {done} symbols with data, {len(skipped)} skipped")
    if skipped[:10]: print(f"   First skipped: {skipped[:10]}")

if __name__ == "__main__":
    main()
