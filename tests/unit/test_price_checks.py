from datetime import date
from decimal import Decimal

from india_equity_engine.core.schemas.contracts import NormalizedRecord
from india_equity_engine.quality.price_checks import validate_price_daily


def test_valid_price_daily_passes_quality_checks() -> None:
    records = [
        NormalizedRecord(
            table_name="price_daily",
            row={
                "instrument_id": "INS_1",
                "trade_date": date(2026, 4, 24),
                "open_price": Decimal("10"),
                "high_price": Decimal("12"),
                "low_price": Decimal("9"),
                "close_price": Decimal("11"),
                "volume": 100,
            },
        )
    ]

    assert validate_price_daily(records) == []


def test_bad_price_daily_reports_quality_issue() -> None:
    records = [
        NormalizedRecord(
            table_name="price_daily",
            row={
                "instrument_id": "INS_1",
                "trade_date": date(2026, 4, 24),
                "open_price": Decimal("10"),
                "high_price": Decimal("8"),
                "low_price": Decimal("9"),
                "close_price": Decimal("11"),
                "volume": 100,
            },
        )
    ]

    issues = validate_price_daily(records)
    assert {issue.rule_name for issue in issues} == {"high_below_ohlc", "low_above_ohlc"}
