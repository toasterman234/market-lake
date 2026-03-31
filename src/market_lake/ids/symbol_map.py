from __future__ import annotations
import hashlib
from pathlib import Path
import pandas as pd
import yaml

def load_symbol_aliases(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def stable_symbol_id(symbol: str) -> int:
    h = hashlib.sha256(symbol.upper().encode("utf-8")).hexdigest()[:8]
    return int(h, 16) % (2**31 - 1)

def build_dim_symbol(symbols, asset_type_map=None, aliases=None) -> pd.DataFrame:
    asset_type_map = asset_type_map or {}
    aliases = aliases or {}
    rows = []
    for sym in sorted(set(s.upper() for s in symbols)):
        rows.append({
            "symbol_id": stable_symbol_id(sym),
            "symbol": sym,
            "asset_type": asset_type_map.get(sym, "unknown"),
            "yahoo_symbol": aliases.get(sym, {}).get("yahoo", sym),
            "stooq_symbol": aliases.get(sym, {}).get("stooq", sym.lower() + ".us"),
        })
    return pd.DataFrame(rows)
