"""DuckDB helper over local Parquet datasets."""

from __future__ import annotations

from pathlib import Path

import duckdb


class DuckDBStore:
    """Small helper for local DuckDB analytics."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def refresh_parquet_view(self, table_name: str, parquet_glob: str | Path) -> None:
        """Create or replace a view over a Parquet glob."""

        with duckdb.connect(str(self.db_path)) as con:
            escaped_table = table_name.replace('"', '""')
            escaped_glob = str(parquet_glob).replace("'", "''")
            con.execute(
                f'CREATE OR REPLACE VIEW "{escaped_table}" AS '
                f"SELECT * FROM read_parquet('{escaped_glob}')"
            )

    def table_count(self, table_name: str) -> int:
        """Return a row count from a view or table."""

        with duckdb.connect(str(self.db_path)) as con:
            escaped_table = table_name.replace('"', '""')
            result = con.execute(f'SELECT count(*) FROM "{escaped_table}"').fetchone()
            return int(result[0]) if result else 0
