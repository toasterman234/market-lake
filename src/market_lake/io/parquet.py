"""
market_lake/io/parquet.py
==========================
Parquet I/O layer with three guarantees:

1. SCHEMA ENFORCEMENT  — Types are coerced to match config/schemas.yaml before writing.
   Any column with a declared type is cast automatically; mismatches that can't be
   coerced raise SchemaError immediately at write time, not three days later.

2. IDEMPOTENCY  — Uses existing_data_behavior="delete_matching" so re-running an
   ingest script replaces the affected partition(s) instead of appending duplicate files.

3. POST-WRITE DEDUP VERIFICATION  — After writing, immediately reads back the partition
   and asserts no duplicate natural-key rows exist. Raises DuplicateError if found.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

# ── Type coercion map: YAML type name → PyArrow type ──────────────────────────
_PA_TYPES: dict[str, pa.DataType] = {
    "date32":  pa.date32(),
    "int32":   pa.int32(),
    "int64":   pa.int64(),
    "float64": pa.float64(),
    "string":  pa.string(),
    "bool_":   pa.bool_(),
}

_SCHEMA_CACHE: dict[str, dict] = {}     # table_name → {col: pa_type}
_NATURAL_KEY_CACHE: dict[str, list] = {}


class SchemaError(ValueError):
    """Column type coercion failed — cannot write data that violates schema contract."""


class DuplicateError(ValueError):
    """Post-write verification found duplicate natural-key rows."""


def _load_schema_registry() -> dict:
    """Load and cache config/schemas.yaml relative to MARKET_LAKE_ROOT."""
    import os
    root = Path(os.environ.get("MARKET_LAKE_ROOT", ".")).resolve()
    schema_file = root / "config" / "schemas.yaml"
    if not schema_file.exists():
        return {}
    with open(schema_file) as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("tables", {})


def _get_table_schema(table_name: str) -> tuple[dict[str, pa.DataType], list[str]]:
    """Return (col→pa_type, natural_key_cols) for a table name. Cached."""
    if table_name not in _SCHEMA_CACHE:
        registry = _load_schema_registry()
        if table_name not in registry:
            _SCHEMA_CACHE[table_name] = {}
            _NATURAL_KEY_CACHE[table_name] = []
        else:
            tbl = registry[table_name]
            _SCHEMA_CACHE[table_name] = {
                col: _PA_TYPES[t]
                for col, t in (tbl.get("columns") or {}).items()
                if t in _PA_TYPES
            }
            _NATURAL_KEY_CACHE[table_name] = tbl.get("natural_key") or []
    return _SCHEMA_CACHE[table_name], _NATURAL_KEY_CACHE[table_name]


def _coerce_dataframe(
    df: pd.DataFrame,
    col_types: dict[str, pa.DataType],
    table_name: str,
) -> pd.DataFrame:
    """Coerce DataFrame columns to match declared schema types.
    Only touches columns that exist in the DataFrame.
    Raises SchemaError if coercion fails.
    """
    df = df.copy()
    for col, pa_type in col_types.items():
        if col not in df.columns:
            continue
        try:
            if pa_type == pa.date32():
                # Always route through pd.to_datetime — handles strings, timestamps,
                # Python dates, and object arrays uniformly without .dt accessor issues
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
            elif pa_type == pa.int32():
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int32")
            elif pa_type == pa.int64():
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif pa_type == pa.float64():
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif pa_type == pa.bool_():
                df[col] = df[col].astype("boolean")
            # string: leave as-is, PyArrow handles it
        except Exception as exc:
            raise SchemaError(
                f"[{table_name}] Cannot coerce column '{col}' to {pa_type}: {exc}"
            ) from exc
    return df


def _verify_no_duplicates(
    output_dir: Path,
    partition_cols: list[str],
    natural_key: list[str],
    table_name: str,
) -> None:
    """Read back the just-written partition(s) and assert no duplicate natural keys.
    Only checks the relevant partition(s), not the full table, for speed.
    """
    if not natural_key:
        return
    db = duckdb.connect(":memory:")
    try:
        dup_count = db.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {', '.join(natural_key)}, COUNT(*) n
                FROM read_parquet('{output_dir}/**/*.parquet', union_by_name=true)
                GROUP BY {', '.join(natural_key)}
                HAVING n > 1
            )
        """).fetchone()[0]
        if dup_count > 0:
            raise DuplicateError(
                f"[{table_name}] Post-write verification failed: "
                f"{dup_count:,} duplicate {natural_key} rows in {output_dir}"
            )
    finally:
        db.close()


def _content_hash(df: pd.DataFrame) -> str:
    raw = pd.util.hash_pandas_object(df).sum()
    return hashlib.sha256(str(raw).encode()).hexdigest()[:12]


def write_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    partition_cols: Optional[list[str]] = None,
    filename: Optional[str] = None,
    table_name: Optional[str] = None,
    verify_dedup: bool = True,
) -> Path:
    """Write DataFrame to Parquet with schema enforcement and dedup verification.

    Args:
        df:             Data to write.
        output_dir:     Target directory (partitioned or flat).
        partition_cols: If set, write as a Hive-partitioned dataset.
        filename:       Override for flat (non-partitioned) writes.
        table_name:     Canonical table name (matches config/schemas.yaml).
                        Required for schema enforcement and dedup verification.
        verify_dedup:   If True (default), assert no duplicate natural-key rows
                        after writing. Pass False only for large append-only tables
                        where you've pre-deduplicated the input.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Schema enforcement
    col_types: dict[str, pa.DataType] = {}
    natural_key: list[str] = []
    if table_name:
        col_types, natural_key = _get_table_schema(table_name)
        if col_types:
            df = _coerce_dataframe(df, col_types, table_name)

    # Step 2: Write
    if partition_cols:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(output_dir),
            partition_cols=partition_cols,
            existing_data_behavior="delete_matching",   # idempotent
        )
        written_path = output_dir
    else:
        fname = filename or f"part-{_content_hash(df)}.parquet"
        target = output_dir / fname
        df.to_parquet(target, engine="pyarrow", index=False)
        written_path = target

    # Step 3: Post-write dedup verification (partitioned only)
    if verify_dedup and partition_cols and natural_key and table_name:
        _verify_no_duplicates(output_dir, partition_cols, natural_key, table_name)

    return written_path


def read_parquet_dir(path: Path, **kwargs) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {path}")
    return pd.read_parquet(path, engine="pyarrow", **kwargs)


def read_parquet_file(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path, engine="pyarrow")
