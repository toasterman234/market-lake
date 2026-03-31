import tempfile
import pytest
from pathlib import Path
import pandas as pd
from market_lake.io.parquet import write_parquet, read_parquet_dir


def make_df():
    return pd.DataFrame({
        "symbol": ["SPY", "QQQ"],
        "date":   ["2024-01-02", "2024-01-02"],
        "close":  [480.0, 410.0],
        "year":   [2024, 2024],
    })


def test_write_and_read_flat():
    with tempfile.TemporaryDirectory() as tmpdir:
        write_parquet(make_df(), Path(tmpdir), filename="test.parquet")
        loaded = read_parquet_dir(Path(tmpdir))
        assert len(loaded) == 2
        assert set(loaded["symbol"].tolist()) == {"SPY", "QQQ"}


def test_write_partitioned():
    with tempfile.TemporaryDirectory() as tmpdir:
        write_parquet(make_df(), Path(tmpdir), partition_cols=["year"])
        loaded = read_parquet_dir(Path(tmpdir))
        assert len(loaded) == 2


def test_write_idempotent_filenames():
    with tempfile.TemporaryDirectory() as tmpdir:
        p1 = write_parquet(make_df(), Path(tmpdir))
        p2 = write_parquet(make_df(), Path(tmpdir))
        assert p1.name == p2.name


def test_read_missing_dir_raises():
    with pytest.raises(FileNotFoundError):
        read_parquet_dir(Path("/nonexistent/path/xyz"))
