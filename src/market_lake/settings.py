from __future__ import annotations
import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

_env = Path(os.getenv("MARKET_LAKE_ROOT", ".")).resolve() / ".env"
load_dotenv(_env, override=False)

@dataclass(frozen=True)
class Settings:
    root: Path
    duckdb_path: Path
    raw_root: Path
    canonical_root: Path
    marts_root: Path
    config_dir: Path
    fred_api_key: str | None
    thetadata_username: str | None
    thetadata_password: str | None

    @staticmethod
    def load() -> "Settings":
        root = Path(os.getenv("MARKET_LAKE_ROOT", ".")).resolve()
        duckdb_rel = os.getenv("DUCKDB_PATH", "duckdb/market.duckdb")
        return Settings(
            root=root, duckdb_path=(root / duckdb_rel).resolve(),
            raw_root=root / "raw", canonical_root=root / "canonical",
            marts_root=root / "marts", config_dir=root / "config",
            fred_api_key=os.getenv("FRED_API_KEY") or None,
            thetadata_username=os.getenv("THETADATA_USERNAME") or None,
            thetadata_password=os.getenv("THETADATA_PASSWORD") or None,
        )
