from datetime import datetime, timezone

from india_equity_engine.core.schemas.contracts import RawArtifact
from india_equity_engine.storage.local_fs import RawArtifactStore


def test_raw_store_uses_source_date_hash_layout(tmp_path) -> None:
    artifact = RawArtifact(
        source_code="S01",
        source_family="nse",
        logical_name="nse_equity_l_master",
        source_url="https://example.test/EQUITY_L.csv",
        content=b"SYMBOL,ISIN NUMBER\nABC,INE000A01000\n",
        extension="csv",
        retrieved_at=datetime(2026, 4, 25, 12, 30, tzinfo=timezone.utc),
        content_type="text/csv",
    )

    record = RawArtifactStore(tmp_path, "test-parser").store(artifact)

    assert record.path.exists()
    assert record.metadata_path.exists()
    assert "nse" in record.path.parts
    assert "2026" in record.path.parts
    assert record.sha256 in record.path.name
