from __future__ import annotations
import hashlib
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def _content_hash(df: pd.DataFrame) -> str:
    raw = pd.util.hash_pandas_object(df).sum()
    return hashlib.sha256(str(raw).encode()).hexdigest()[:12]

def write_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    partition_cols: list[str] | None = None,
    filename: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    if partition_cols:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(output_dir),
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
        )
        return output_dir
    fname = filename or f"part-{_content_hash(df)}.parquet"
    target = output_dir / fname
    df.to_parquet(target, engine="pyarrow", index=False)
    return target

def read_parquet_dir(path: Path, **kwargs) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {path}")
    return pd.read_parquet(path, engine="pyarrow", **kwargs)

def read_parquet_file(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")
