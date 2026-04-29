"""Run local data-quality and observability checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from india_equity_engine.core.schemas.contracts import JobRunResult, NormalizedRecord
from india_equity_engine.core.settings import Settings
from india_equity_engine.observability.job_log import (
    data_quality_issue_row,
    utc_now,
    write_observability_records,
)


@dataclass(frozen=True)
class TableQualityRule:
    table_name: str
    primary_key: tuple[str, ...]
    required_columns: tuple[str, ...]
    freshness_column: str | None = None
    max_age_days: int | None = None


TABLE_RULES = (
    TableQualityRule(
        "instruments",
        ("instrument_id",),
        ("instrument_id", "isin", "legal_name"),
    ),
    TableQualityRule(
        "listings",
        ("listing_id",),
        ("listing_id", "instrument_id", "exchange_code"),
    ),
    TableQualityRule(
        "price_daily",
        ("instrument_id", "trade_date"),
        ("instrument_id", "trade_date", "open_price", "high_price", "low_price", "close_price"),
        "trade_date",
        7,
    ),
    TableQualityRule(
        "corporate_announcements",
        ("announcement_id",),
        ("announcement_id", "headline", "available_at"),
        "available_at",
        14,
    ),
    TableQualityRule(
        "filings",
        ("filing_id",),
        ("filing_id", "filing_family", "available_at"),
        "available_at",
        30,
    ),
    TableQualityRule(
        "feature_snapshots",
        ("instrument_id", "as_of_date", "horizon", "feature_family", "feature_name"),
        ("instrument_id", "as_of_date", "horizon", "feature_family", "feature_name"),
        "as_of_date",
        7,
    ),
    TableQualityRule(
        "score_snapshots",
        ("instrument_id", "as_of_date", "horizon"),
        ("instrument_id", "as_of_date", "horizon", "classification"),
        "as_of_date",
        7,
    ),
    TableQualityRule(
        "stock_snapshots",
        ("instrument_id", "as_of_date"),
        ("instrument_id", "as_of_date", "snapshot_json_path"),
        "as_of_date",
        7,
    ),
    TableQualityRule(
        "llm_explanations",
        ("explanation_id",),
        ("explanation_id", "instrument_id", "as_of_date", "output_json"),
        "as_of_date",
        7,
    ),
    TableQualityRule(
        "job_runs",
        ("job_run_id",),
        ("job_run_id", "job_name", "started_at", "finished_at", "status"),
        "finished_at",
        30,
    ),
)


def run_data_quality_review(settings: Settings, *, max_age_days: int | None = None) -> JobRunResult:
    """Run current-table checks and write data_quality_issues."""

    settings.ensure_runtime_dirs()
    detected_at = utc_now()
    issues: list[dict[str, Any]] = []
    checked_tables = 0
    rows_scanned = 0
    for rule in TABLE_RULES:
        path = _current_table_path(settings, rule.table_name)
        if path is None:
            issues.append(
                data_quality_issue_row(
                    table_name=rule.table_name,
                    entity_key="table",
                    rule_name="missing_current_table",
                    severity="high" if rule.table_name in _required_tables() else "medium",
                    issue_text=f"No current Parquet table found for {rule.table_name}.",
                    detected_at=detected_at,
                )
            )
            continue
        checked_tables += 1
        table = _read_table(path)
        if table is None:
            issues.append(
                data_quality_issue_row(
                    table_name=rule.table_name,
                    entity_key=str(path),
                    rule_name="unreadable_parquet",
                    severity="high",
                    issue_text=f"Could not read current Parquet table at {path}.",
                    detected_at=detected_at,
                )
            )
            continue
        columns = table["columns"]
        rows_scanned += int(table["row_count"])
        issues.extend(_missing_column_issues(rule, columns, detected_at))
        if all(column in columns for column in rule.primary_key):
            issues.extend(_duplicate_key_issues(path, rule, detected_at))
        issues.extend(_missing_value_issues(path, rule, columns, detected_at))
        freshness_limit = max_age_days if max_age_days is not None else rule.max_age_days
        if rule.freshness_column and freshness_limit and rule.freshness_column in columns:
            issues.extend(
                _freshness_issues(path, rule, freshness_limit, detected_at.date(), detected_at)
            )

    issue_rows = _dedupe_issue_rows(issues)
    issue_rows.extend(_resolved_existing_issues(settings, issue_rows, detected_at))
    records = [
        NormalizedRecord(table_name="data_quality_issues", row=row)
        for row in issue_rows
    ]
    warnings = write_observability_records(settings, records)
    active_issue_rows = [row for row in issue_rows if row.get("resolved_flag") is not True]
    severity_counts: dict[str, int] = {}
    for row in active_issue_rows:
        severity = str(row["severity"])
        severity_counts[severity] = severity_counts.get(severity, 0) + 1
    has_high_severity = any(row["severity"] in {"high", "critical"} for row in active_issue_rows)
    return JobRunResult(
        job_name="run_data_quality_review",
        status="partial_success" if has_high_severity else "success",
        records_in=rows_scanned,
        records_out=len(active_issue_rows),
        warnings=warnings,
        outputs={
            "checked_tables": checked_tables,
            "issues": len(active_issue_rows),
            "resolved_issues": len(issue_rows) - len(active_issue_rows),
            "severity_counts": severity_counts,
            "data_quality_table": str(
                settings.gold_root / "data_quality_issues" / "current.parquet"
            ),
            "duckdb_path": str(settings.duckdb_path),
        },
    )


def _current_table_path(settings: Settings, table_name: str) -> Path | None:
    for root in (settings.gold_root, settings.silver_root):
        path = root / table_name / "current.parquet"
        if path.exists():
            return path
        dated_paths = sorted(
            (root / table_name).glob("*.parquet"),
            key=lambda candidate: candidate.stat().st_mtime,
            reverse=True,
        )
        if dated_paths:
            return dated_paths[0]
    return None


def _read_table(path: Path) -> dict[str, object] | None:
    try:
        with duckdb.connect() as con:
            result = con.execute(f"select * from read_parquet('{_escape(path)}') limit 0")
            columns = tuple(column[0] for column in result.description)
            row_count = con.execute(
                f"select count(*) from read_parquet('{_escape(path)}')"
            ).fetchone()[0]
    except duckdb.Error:
        return None
    return {"columns": columns, "row_count": int(row_count)}


def _missing_column_issues(
    rule: TableQualityRule,
    columns: tuple[str, ...],
    detected_at,
) -> list[dict[str, Any]]:
    issues = []
    for column in set(rule.primary_key + rule.required_columns) - set(columns):
        issues.append(
            data_quality_issue_row(
                table_name=rule.table_name,
                entity_key=column,
                rule_name="missing_required_column",
                severity="high",
                issue_text=f"Required column {column} is missing from {rule.table_name}.",
                detected_at=detected_at,
            )
        )
    return issues


def _duplicate_key_issues(
    path: Path,
    rule: TableQualityRule,
    detected_at,
) -> list[dict[str, Any]]:
    key_expr = ", ".join(f'"{column}"' for column in rule.primary_key)
    entity_expr = " || ':' || ".join(
        f"coalesce(cast(\"{column}\" as varchar), '<null>')" for column in rule.primary_key
    )
    query = f"""
        select {entity_expr} as entity_key, count(*) as duplicate_count
        from read_parquet('{_escape(path)}')
        group by {key_expr}
        having count(*) > 1
        limit 25
    """
    rows = _query_rows(query)
    return [
        data_quality_issue_row(
            table_name=rule.table_name,
            entity_key=str(row["entity_key"]),
            rule_name="duplicate_primary_key",
            severity="critical",
            issue_text=f"Primary key appears {row['duplicate_count']} times.",
            detected_at=detected_at,
        )
        for row in rows
    ]


def _missing_value_issues(
    path: Path,
    rule: TableQualityRule,
    columns: tuple[str, ...],
    detected_at,
) -> list[dict[str, Any]]:
    issues = []
    for column in rule.required_columns:
        if column not in columns:
            continue
        query = f"""
            select count(*) as missing_count
            from read_parquet('{_escape(path)}')
            where "{column}" is null
        """
        rows = _query_rows(query)
        missing_count = int(rows[0]["missing_count"]) if rows else 0
        if missing_count > 0:
            issues.append(
                data_quality_issue_row(
                    table_name=rule.table_name,
                    entity_key=column,
                    rule_name="missing_required_value",
                    severity="medium",
                    issue_text=f"{missing_count} rows have null {column}.",
                    detected_at=detected_at,
                )
            )
    return issues


def _freshness_issues(
    path: Path,
    rule: TableQualityRule,
    max_age_days: int,
    today: date,
    detected_at,
) -> list[dict[str, Any]]:
    query = f"""
        select max(cast("{rule.freshness_column}" as date)) as max_date
        from read_parquet('{_escape(path)}')
    """
    rows = _query_rows(query)
    max_date = rows[0].get("max_date") if rows else None
    if max_date is None:
        return [
            data_quality_issue_row(
                table_name=rule.table_name,
                entity_key=rule.freshness_column or "freshness",
                rule_name="missing_freshness_value",
                severity="medium",
                issue_text=f"No usable freshness date found in {rule.freshness_column}.",
                detected_at=detected_at,
            )
        ]
    if not isinstance(max_date, date):
        return []
    age_days = (today - max_date).days
    if age_days > max_age_days:
        return [
            data_quality_issue_row(
                table_name=rule.table_name,
                entity_key=rule.freshness_column or "freshness",
                rule_name="stale_table",
                severity="medium",
                issue_text=f"Latest {rule.freshness_column} is {max_date}; age is {age_days} days.",
                detected_at=detected_at,
            )
        ]
    return []


def _query_rows(query: str) -> list[dict[str, Any]]:
    try:
        with duckdb.connect() as con:
            result = con.execute(query)
            columns = [column[0] for column in result.description]
            rows = result.fetchall()
    except duckdb.Error:
        return []
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _required_tables() -> set[str]:
    return {"instruments", "listings", "price_daily", "feature_snapshots", "score_snapshots"}


def _dedupe_issue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped = {}
    for row in rows:
        deduped[row["dq_issue_id"]] = row
    return list(deduped.values())


def _resolved_existing_issues(
    settings: Settings,
    current_issue_rows: list[dict[str, Any]],
    detected_at,
) -> list[dict[str, Any]]:
    path = settings.gold_root / "data_quality_issues" / "current.parquet"
    if not path.exists():
        return []
    current_keys = {
        _issue_match_key(row)
        for row in current_issue_rows
        if row.get("rule_name") in _managed_rule_names()
    }
    resolved = []
    for row in _read_existing_issue_rows(path):
        if row.get("resolved_flag") is True:
            continue
        if row.get("rule_name") not in _managed_rule_names():
            continue
        if _issue_match_key(row) in current_keys:
            continue
        updated = dict(row)
        updated["resolved_flag"] = True
        updated["resolution_note"] = (
            f"Resolved by data-quality review at {detected_at.isoformat()}."
        )
        resolved.append(updated)
    return resolved


def _read_existing_issue_rows(path: Path) -> list[dict[str, Any]]:
    query = f"select * from read_parquet('{_escape(path)}')"
    return _query_rows(query)


def _issue_match_key(row: dict[str, Any]) -> tuple[object, object, object]:
    return (row.get("table_name"), row.get("entity_key"), row.get("rule_name"))


def _managed_rule_names() -> set[str]:
    return {
        "missing_current_table",
        "unreadable_parquet",
        "missing_required_column",
        "duplicate_primary_key",
        "missing_required_value",
        "missing_freshness_value",
        "stale_table",
    }


def _escape(path: Path) -> str:
    return str(path).replace("'", "''")
