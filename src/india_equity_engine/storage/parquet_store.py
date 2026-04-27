"""Parquet dataset writer for normalized Stage A tables."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from india_equity_engine.core.schemas.contracts import NormalizedRecord, WarehouseWriteResult


class ParquetStore:
    """Write normalized records to local Parquet files."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write_records(
        self,
        records: Iterable[NormalizedRecord],
        dataset_label: str,
    ) -> list[WarehouseWriteResult]:
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for record in records:
            grouped[record.table_name].append(record.row)

        results: list[WarehouseWriteResult] = []
        for table_name, rows in grouped.items():
            table_dir = self.root / table_name
            table_dir.mkdir(parents=True, exist_ok=True)
            path = table_dir / f"{dataset_label}.parquet"
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, path)
            results.append(
                WarehouseWriteResult(
                    table_name=table_name,
                    records_in=len(rows),
                    records_out=len(rows),
                    path=path,
                )
            )
        return results

    def write_current_records(
        self,
        records: Iterable[NormalizedRecord],
    ) -> list[WarehouseWriteResult]:
        """Write the current table snapshot for idempotent DuckDB views."""

        return self.write_records(records, "current")
