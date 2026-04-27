"""Universal connector contract from the Stage A design."""

from __future__ import annotations

from abc import ABC, abstractmethod

from india_equity_engine.core.schemas.contracts import (
    NormalizedRecord,
    RawArtifact,
    SourceObject,
    ValidationResult,
)


class SourceConnector(ABC):
    """Base interface implemented by every source adapter."""

    @abstractmethod
    def discover(self) -> list[SourceObject]:
        """Discover source objects available for download."""

    @abstractmethod
    def download(self, source_object: SourceObject) -> RawArtifact:
        """Download a source object into memory."""

    @abstractmethod
    def validate(self, raw_artifact: RawArtifact) -> ValidationResult:
        """Validate a raw artifact before storage and parsing."""

    @abstractmethod
    def parse(self, raw_artifact: RawArtifact) -> list[dict[str, object]]:
        """Parse source-native rows from a raw artifact."""

    @abstractmethod
    def normalize(
        self,
        records: list[dict[str, object]],
        raw_artifact: RawArtifact,
    ) -> list[NormalizedRecord]:
        """Normalize parsed source rows into canonical records."""
