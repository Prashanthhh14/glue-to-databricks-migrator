import pytest

pytest.importorskip("pyspark")

from glue2dbx.reconcile import reconcile  # noqa: E402

ROWS = [(1, "AAPL", 100, 190.5), (2, "MSFT", 50, 410.0), (3, "NVDA", None, 120.25)]
SCHEMA = "trade_id long, symbol string, qty long, price double"


def test_identical_frames_pass_regardless_of_row_order(spark):
    source = spark.createDataFrame(ROWS, SCHEMA)
    target = spark.createDataFrame(list(reversed(ROWS)), SCHEMA).repartition(3)
    result = reconcile(source, target, keys=["trade_id"])
    assert result.passed, result.to_markdown()
    assert result.source_count == result.target_count == 3


def test_changed_value_is_caught_with_its_key(spark):
    source = spark.createDataFrame(ROWS, SCHEMA)
    target = spark.createDataFrame([ROWS[0], (2, "MSFT", 50, 410.01), ROWS[2]], SCHEMA)
    result = reconcile(source, target, keys=["trade_id"])
    assert not result.passed
    assert result.changed_rows == 1
    assert result.sample_changed_keys == [{"trade_id": 2}]
    failed = [c.column for c in result.column_checks if not c.passed]
    assert failed == ["price"]


def test_null_turned_into_zero_is_caught(spark):
    source = spark.createDataFrame(ROWS, SCHEMA)
    target = spark.createDataFrame([ROWS[0], ROWS[1], (3, "NVDA", 0, 120.25)], SCHEMA)
    result = reconcile(source, target)
    qty = next(c for c in result.column_checks if c.column == "qty")
    assert (qty.source_nulls, qty.target_nulls) == (1, 0)
    assert not result.passed


def test_missing_extra_rows_and_schema_drift(spark):
    source = spark.createDataFrame(ROWS, SCHEMA)
    target = spark.createDataFrame(
        [(1, "AAPL", 100, "190.5", "x"), (4, "TSLA", 10, "250.0", "y")],
        "trade_id long, symbol string, qty long, price string, load_id string",
    )
    result = reconcile(source, target, keys=["trade_id"])
    assert result.missing_in_target == 2
    assert result.extra_in_target == 1
    assert result.extra_columns == ["load_id"]
    assert result.type_mismatches == [("price", "double", "string")]
    report = result.to_markdown()
    assert "FAIL" in report and "load_id" in report


def test_exclude_ignores_audit_columns(spark):
    source = spark.createDataFrame([(1, "a", "2026-01-01")], "id long, v string, loaded_at string")
    target = spark.createDataFrame([(1, "a", "2026-11-02")], "id long, v string, loaded_at string")
    assert not reconcile(source, target).passed
    assert reconcile(source, target, keys=["id"], exclude=["loaded_at"]).passed


def test_unknown_key_raises(spark):
    df = spark.createDataFrame(ROWS, SCHEMA)
    with pytest.raises(ValueError, match="nope"):
        reconcile(df, df, keys=["nope"])
