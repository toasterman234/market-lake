from __future__ import annotations
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import pandas as pd

@dataclass
class ManifestRecord:
    ingest_batch_id: str
    dataset_name: str
    source: str
    file_path: str
    row_count: int
    schema_hash: str
    min_date: str | None
    max_date: str | None
    ingested_at: str
    status: str
    notes: str = ""

def schema_hash_for_frame(df: pd.DataFrame) -> str:
    text = "|".join(f"{c}:{t}" for c, t in df.dtypes.items())
    return hashlib.sha256(text.encode()).hexdigest()[:16]

def write_manifest(record: ManifestRecord, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"manifest-{record.ingest_batch_id}.parquet"
    pd.DataFrame([asdict(record)]).to_parquet(target, engine="pyarrow", index=False)

def load_manifests(manifest_dir: Path) -> pd.DataFrame:
    files = sorted(manifest_dir.rglob("manifest-*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def build_batch_id(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
