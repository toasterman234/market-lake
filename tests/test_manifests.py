import tempfile
from pathlib import Path
import pandas as pd
from market_lake.io.manifests import (
    ManifestRecord, build_batch_id, load_manifests,
    schema_hash_for_frame, write_manifest, now_utc_iso,
)


def make_record():
    return ManifestRecord(
        ingest_batch_id="abc123def456",
        dataset_name="test_dataset",
        source="test",
        file_path="/some/path",
        row_count=100,
        schema_hash="deadbeef01234567",
        min_date="2024-01-01",
        max_date="2024-12-31",
        ingested_at=now_utc_iso(),
        status="success",
    )


def test_write_and_load_manifest():
    with tempfile.TemporaryDirectory() as tmpdir:
        record = make_record()
        write_manifest(record, Path(tmpdir))
        loaded = load_manifests(Path(tmpdir))
        assert len(loaded) == 1
        assert loaded.iloc[0]["dataset_name"] == "test_dataset"
        assert loaded.iloc[0]["row_count"] == 100


def test_build_batch_id_deterministic():
    id1 = build_batch_id("foo", "bar", "baz")
    id2 = build_batch_id("foo", "bar", "baz")
    assert id1 == id2
    assert len(id1) == 16


def test_build_batch_id_different():
    assert build_batch_id("a", "b") != build_batch_id("a", "c")


def test_schema_hash_deterministic():
    df = pd.DataFrame({"a": [1, 2], "b": [3.0, 4.0]})
    assert schema_hash_for_frame(df) == schema_hash_for_frame(df)


def test_load_manifests_empty_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        result = load_manifests(Path(tmpdir))
        assert result.empty
