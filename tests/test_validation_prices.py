import pandas as pd
import pytest
from market_lake.validation.prices import validate_daily_bars


def make_valid_bars(n=5):
    return pd.DataFrame({
        "symbol": ["SPY"] * n,
        "date": pd.date_range("2024-01-01", periods=n).date,
        "open":      [480.0] * n,
        "high":      [485.0] * n,
        "low":       [478.0] * n,
        "close":     [482.0] * n,
        "adj_close": [482.0] * n,
        "volume":    [50_000_000] * n,
    })


def test_valid_bars_no_errors():
    assert validate_daily_bars(make_valid_bars()) == []


def test_high_less_than_low():
    df = make_valid_bars()
    df.loc[0, "high"] = 470.0
    assert any("high < low" in e for e in validate_daily_bars(df))


def test_negative_close():
    df = make_valid_bars()
    df.loc[1, "close"] = -1.0
    assert any("close" in e for e in validate_daily_bars(df))


def test_zero_open():
    df = make_valid_bars()
    df.loc[0, "open"] = 0.0
    assert any("open" in e for e in validate_daily_bars(df))


def test_nan_in_high():
    df = make_valid_bars()
    df.loc[2, "high"] = float("nan")
    errors = validate_daily_bars(df)
    assert any("NaN" in e and "high" in e for e in errors)


def test_negative_volume():
    df = make_valid_bars()
    df.loc[0, "volume"] = -100
    assert any("volume" in e for e in validate_daily_bars(df))


def test_missing_required_columns():
    df = pd.DataFrame({"symbol": ["SPY"], "date": ["2024-01-01"]})
    assert any("Missing" in e for e in validate_daily_bars(df))
