"""
Build dim_symbol with proper asset_type, sector, and industry
by querying yfinance for each symbol's quoteType and sector info.
"""
import duckdb, time, gc, sys
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

sys.path.insert(0, "src")
from market_lake.ids.symbol_map import stable_symbol_id
from market_lake.settings import Settings

settings = Settings.load()
ROOT = settings.canonical_root
dim_dir = ROOT / "dimensions" / "dim_symbol"

KNOWN_ETFS = {
    "SPY","QQQ","IWM","DIA","TLT","GLD","SLV","USO","HYG","LQD","VNQ","EFA","EEM",
    "VXX","UVXY","SVXY","ARKK","XLK","XLF","XLV","XLE","XLI","XLP","XLU","XLY",
    "XLB","XLRE","SHY","IEF","TIP","AGG","BND","EMB","JNK","BIL",
    "SQQQ","TQQQ","SPXS","SPXL","GDX","GDXJ","ICLN","ARKG","ARKW","ARKF","ARKQ",
    "WCLD","SKYY","CLOU","CIBR","HACK","DRIV","ROBO","BOTZ",
    "PDBC","GSG","PALL","PPLT","DBO","UNG","CORN","SOYB","WEAT",
    "EWJ","EWZ","EWG","EWU","EWC","EWA","EWH","EWT","EWY","EWS",
    "FXI","ASHR","KWEB","MCHI","VEA","VWO","RSP","MDY","IJH",
    "VTI","VOO","IVV","IAU","SGOL","VIXY","VIXM","SVOL","PUTW","BTAL","CTA",
}

db = duckdb.connect(":memory:")
all_syms = db.execute(f"""
    SELECT symbol FROM read_parquet('{ROOT}/dimensions/dim_symbol/**/*.parquet', union_by_name=true)
    ORDER BY symbol
""").df()["symbol"].tolist()
db.close()

print(f"Enriching {len(all_syms)} symbols with asset_type, sector, industry...")

results = []
for i, sym in enumerate(all_syms, 1):
    asset_type = "unknown"
    sector = None
    industry = None

    if sym.upper() in KNOWN_ETFS:
        asset_type = "etf"
    else:
        try:
            info = yf.Ticker(sym).info
            qt = info.get("quoteType", "").upper()
            if qt in ("ETF", "MUTUALFUND"):
                asset_type = "etf"
            elif qt in ("EQUITY", "STOCK"):
                asset_type = "stock"
                sector   = info.get("sector")
                industry = info.get("industry")
            elif qt == "INDEX":
                asset_type = "index"
            elif qt:
                asset_type = qt.lower()
            time.sleep(0.15)
        except Exception as e:
            pass

    results.append({
        "symbol_id":  stable_symbol_id(sym),
        "symbol":     sym.upper(),
        "asset_type": asset_type,
        "sector":     sector,
        "industry":   industry,
    })

    if i % 50 == 0 or i == len(all_syms):
        stocks = sum(1 for r in results if r["asset_type"] == "stock")
        etfs   = sum(1 for r in results if r["asset_type"] == "etf")
        print(f"  [{i}/{len(all_syms)}] {stocks} stocks, {etfs} ETFs so far")
    gc.collect()

df = pd.DataFrame(results)
print(f"\nFinal breakdown:\n{df['asset_type'].value_counts().to_string()}")

for f in dim_dir.glob("*.parquet"):
    f.unlink()
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(table, dim_dir / "dim_symbol.parquet")
print(f"\n✅ Written {len(df)} symbols → dim_symbol.parquet")
