"""Connector and pipeline contracts."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class SourceConfig(BaseModel):
    """Configured source from the Stage A source register."""

    code: str
    family: str
    name: str
    purpose: str
    url: HttpUrl
    fetch_mode: str
    cost: str
    cadence: str
    enabled: bool = True


class SourceObject(BaseModel):
    """A concrete source object available to download."""

    source: SourceConfig
    logical_name: str
    url: HttpUrl
    expected_extension: str
    as_of_date: date | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RawArtifact(BaseModel):
    """Downloaded source artifact before immutable storage."""

    source_code: str
    source_family: str
    logical_name: str
    source_url: str
    content: bytes
    extension: str
    retrieved_at: datetime
    content_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class RawArtifactRecord(BaseModel):
    """Stored raw artifact metadata."""

    source_code: str
    source_family: str
    logical_name: str
    source_url: str
    path: Path
    metadata_path: Path
    sha256: str
    size_bytes: int
    retrieved_at: datetime
    content_type: str | None = None
    parser_version: str | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ValidationResult(BaseModel):
    """Validation result for downloaded artifacts or normalized rows."""

    ok: bool
    rule_name: str
    message: str
    severity: str = "low"
    metadata: dict[str, Any] = Field(default_factory=dict)


class NormalizedRecord(BaseModel):
    """A normalized row heading toward a canonical table."""

    table_name: str
    row: dict[str, Any]


class WarehouseWriteResult(BaseModel):
    """Result from a warehouse write."""

    table_name: str
    records_in: int
    records_out: int
    path: Path | None = None
    warnings: list[str] = Field(default_factory=list)

    model_config = ConfigDict(arbitrary_types_allowed=True)


class JobRunResult(BaseModel):
    """Small job-run summary returned by CLI and pipeline calls."""

    job_name: str
    status: str
    records_in: int = 0
    records_out: int = 0
    warnings: list[str] = Field(default_factory=list)
    outputs: dict[str, Any] = Field(default_factory=dict)
