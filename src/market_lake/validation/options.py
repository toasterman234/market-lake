from __future__ import annotations
import pandas as pd

def validate_option_contracts(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    required = ["contract_id", "underlying_symbol", "expiry", "strike", "option_type"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors
    if df["contract_id"].isna().any():
        errors.append("Null contract_id values")
    if df["contract_id"].duplicated().any():
        errors.append(f"{df['contract_id'].duplicated().sum()} duplicate contract_id values")
    if not df["option_type"].isin(["C","P"]).all():
        bad = df.loc[~df["option_type"].isin(["C","P"]),"option_type"].unique().tolist()
        errors.append(f"Invalid option_type values: {bad}")
    if (df["strike"] <= 0).any():
        errors.append("Non-positive strike prices found")
    return errors

def validate_option_eod(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    required = ["contract_id", "date", "bid", "ask"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors
    if df[["contract_id","date"]].duplicated().any():
        errors.append(f"{df[['contract_id','date']].duplicated().sum()} duplicate (contract_id, date) rows")
    both = df["bid"].notna() & df["ask"].notna()
    if (df.loc[both,"bid"] > df.loc[both,"ask"]).any():
        errors.append("Found rows where bid > ask")
    if "iv" in df.columns:
        iv = df["iv"].dropna()
        if len(iv) > 0 and (iv < 0).any():
            errors.append("Negative implied volatility found")
    return errors
