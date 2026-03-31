from __future__ import annotations
import pandas as pd

def validate_macro_series(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    required = ["series_id", "date", "value"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return errors
    if df[["series_id","date"]].duplicated().any():
        errors.append(f"{df[['series_id','date']].duplicated().sum()} duplicate (series_id, date) rows")
    if df["value"].isna().all():
        errors.append("value column is entirely null")
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.isna().any():
        errors.append("Unparseable date values in macro series")
    return errors
