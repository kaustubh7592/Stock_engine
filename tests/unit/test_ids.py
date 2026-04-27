from india_equity_engine.core.ids import stable_id


def test_stable_id_is_repeatable() -> None:
    assert stable_id("ins", "INE002A01018") == stable_id("ins", "INE002A01018")
    assert stable_id("ins", "INE002A01018").startswith("INS_")
