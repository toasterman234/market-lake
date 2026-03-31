from __future__ import annotations
from decimal import Decimal

def format_strike(value: float | int | str) -> str:
    dec = Decimal(str(value)).normalize()
    s = format(dec, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s

def make_contract_id(underlying: str, expiry: str, strike: float | int | str, option_type: str) -> str:
    return f"{underlying.upper()}|{expiry}|{format_strike(strike)}|{option_type.upper()[0]}"
