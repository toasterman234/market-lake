import pandas as pd
from market_lake.validation.options import validate_option_contracts, validate_option_eod


def make_valid_contracts(n=3):
    return pd.DataFrame({
        "contract_id":       [f"AAPL|2026-01-16|{150+i*5}|C" for i in range(n)],
        "underlying_symbol": ["AAPL"] * n,
        "expiry":            ["2026-01-16"] * n,
        "strike":            [150.0 + i * 5 for i in range(n)],
        "option_type":       ["C"] * n,
    })


def test_valid_contracts():
    assert validate_option_contracts(make_valid_contracts()) == []


def test_duplicate_contract_id():
    df = make_valid_contracts()
    df.loc[1, "contract_id"] = df.loc[0, "contract_id"]
    assert any("duplicate" in e.lower() for e in validate_option_contracts(df))


def test_invalid_option_type():
    df = make_valid_contracts()
    df.loc[0, "option_type"] = "X"
    assert any("option_type" in e for e in validate_option_contracts(df))


def test_negative_strike():
    df = make_valid_contracts()
    df.loc[0, "strike"] = -10.0
    assert any("strike" in e.lower() for e in validate_option_contracts(df))


def test_missing_columns():
    df = pd.DataFrame({"contract_id": ["X|2026-01-16|100|C"]})
    assert any("Missing" in e for e in validate_option_contracts(df))


def test_valid_eod():
    df = pd.DataFrame({
        "contract_id": ["AAPL|2026-01-16|150|C", "AAPL|2026-01-16|155|C"],
        "date":        ["2025-10-01", "2025-10-01"],
        "bid":         [2.5, 1.8],
        "ask":         [2.6, 1.9],
    })
    assert validate_option_eod(df) == []


def test_bid_greater_than_ask():
    df = pd.DataFrame({
        "contract_id": ["AAPL|2026-01-16|150|C"],
        "date":        ["2025-10-01"],
        "bid":         [3.0],
        "ask":         [2.0],
    })
    assert any("bid > ask" in e for e in validate_option_eod(df))
