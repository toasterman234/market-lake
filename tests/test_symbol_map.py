import pandas as pd
from market_lake.ids.symbol_map import stable_symbol_id, build_dim_symbol

def test_deterministic():    assert stable_symbol_id("SPY") == stable_symbol_id("SPY")
def test_case_insensitive(): assert stable_symbol_id("spy") == stable_symbol_id("SPY")
def test_different():        assert stable_symbol_id("SPY") != stable_symbol_id("QQQ")
def test_positive_int():
    sid = stable_symbol_id("SPY")
    assert isinstance(sid, int) and 0 < sid < 2**31
def test_build_basic():
    df = build_dim_symbol(["SPY","QQQ","AAPL"])
    assert len(df) == 3 and set(df["symbol"]) == {"SPY","QQQ","AAPL"}
def test_deduplicates():     assert len(build_dim_symbol(["SPY","spy","SPY"])) == 1
def test_asset_types():
    df = build_dim_symbol(["SPY", "AAPL"])
    assert "asset_type" in df.columns
