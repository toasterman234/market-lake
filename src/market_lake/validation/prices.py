from __future__ import annotations
import pandas as pd

def validate_daily_bars(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    required = ["symbol", "date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors
    for col in ["open", "high", "low", "close"]:
        if df[col].isna().any():
            errors.append(f"NaN values found in {col}")
        if (df[col] <= 0).any():
            errors.append(f"Non-positive values in {col}")
    if (df["high"] < df["low"]).any():
        errors.append("Found rows where high < low")
    if (df["high"] < df["close"]).any():
        errors.append("Found rows where high < close")
    if (df["low"] > df["close"]).any():
        errors.append("Found rows where low > close")
    if "volume" in df.columns and (df["volume"] < 0).any():
        errors.append("Negative volume found")
    if "adj_close" in df.columns and df["adj_close"].isna().any():
        errors.append("NaN values in adj_close")
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        errors.append("Unparseable dates found")
    return errors
