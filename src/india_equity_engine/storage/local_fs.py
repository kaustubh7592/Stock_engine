"""Immutable local raw artifact store."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from india_equity_engine.core.schemas.contracts import RawArtifact, RawArtifactRecord
from india_equity_engine.core.time_utils import utc_now


class RawArtifactStore:
    """Store raw downloaded artifacts using the Stage A naming convention."""

    def __init__(self, root: str | Path, parser_version: str) -> None:
        self.root = Path(root)
        self.parser_version = parser_version

    def store(self, artifact: RawArtifact) -> RawArtifactRecord:
        sha256 = hashlib.sha256(artifact.content).hexdigest()
        retrieved = artifact.retrieved_at
        partition = (
            self.root
            / artifact.source_family
            / f"{retrieved:%Y}"
            / f"{retrieved:%m}"
            / f"{retrieved:%d}"
        )
        partition.mkdir(parents=True, exist_ok=True)

        timestamp = retrieved.strftime("%Y%m%dT%H%M%SZ")
        safe_name = _safe_name(artifact.logical_name)
        suffix = artifact.extension.lstrip(".")
        filename = f"{safe_name}__retrieved={timestamp}__sha256={sha256}.{suffix}"
        path = partition / filename
        metadata_path = path.with_suffix(path.suffix + ".json")

        if not path.exists():
            path.write_bytes(artifact.content)

        record = RawArtifactRecord(
            source_code=artifact.source_code,
            source_family=artifact.source_family,
            logical_name=artifact.logical_name,
            source_url=artifact.source_url,
            path=path,
            metadata_path=metadata_path,
            sha256=sha256,
            size_bytes=len(artifact.content),
            retrieved_at=artifact.retrieved_at,
            content_type=artifact.content_type,
            parser_version=self.parser_version,
        )

        metadata = record.model_dump(mode="json")
        metadata["stored_at"] = utc_now().isoformat()
        metadata["artifact_metadata"] = artifact.metadata
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
        return record


def _safe_name(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in ("-", "_") else "_" for char in value
    ).strip("_")
