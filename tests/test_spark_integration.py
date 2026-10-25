"""End-to-end tests against a real local SparkSession (skipped without pyspark/Java)."""

from __future__ import annotations

import pytest

from planlock import Plan, PlanAssertionError, PlanLock, PlanLockMismatch, expect, lint

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def tables(spark, tmp_path_factory):
    from pyspark.sql import functions as F

    root = tmp_path_factory.mktemp("wh")
    events = spark.range(2000).select(
        F.col("id").alias("user_id"),
        (F.col("id") % 7).cast("double").alias("amount"),
        F.when(F.col("id") % 2 == 0, F.lit("2024-01-01")).otherwise(F.lit("2024-01-02")).alias("dt"),
        F.lit("x").alias("payload"),
    )
    events.write.partitionBy("dt").parquet(str(root / "events"))
    users = spark.range(100).select(F.col("id"), (F.col("id") % 5).cast("string").alias("country"))
    users.write.parquet(str(root / "users"))
    return str(root / "events"), str(root / "users")


@pytest.fixture
def default_conf(spark):
    old = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    yield
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", old)


def _revenue(spark, tables):
    from pyspark.sql import functions as F

    ev_path, us_path = tables
    e = spark.read.parquet(ev_path)
    u = spark.read.parquet(us_path)
    return (
        e.where("dt = '2024-01-01' AND amount > 2")
        .join(u, e.user_id == u.id)
        .groupBy("country")
        .agg(F.sum("amount").alias("revenue"))
    )


def test_real_plan_assertions(spark, tables, default_conf):
    df = _revenue(spark, tables)
    (
        expect(df)
        .uses_broadcast_join()
        .no_sort_merge_join()
        .max_shuffles(1)
        .pushes_filter("amount", table="events")
        .prunes_partitions(table="events", column="dt")
        .reads_only(["user_id", "amount"], table="events")
        .no_python_udf()
        .passes_lint()
    )


def test_disabling_broadcast_is_caught(spark, tables, default_conf):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    df = _revenue(spark, tables)
    with pytest.raises(PlanAssertionError, match="Expected a BroadcastHashJoin"):
        expect(df).uses_broadcast_join()
    assert Plan.of(df).summary.shuffles == 3
    assert Plan.of(df).summary.joins == {"SortMergeJoin": 1}


def test_executed_final_plan(spark, tables, default_conf):
    df = _revenue(spark, tables)
    plan = Plan.of(df, executed=True)
    assert plan.is_final
    # Initial-plan branch must not double count.
    assert plan.summary.shuffles == 1
    expect(df, executed=True).is_final_plan().uses_broadcast_join()


def test_python_udf_and_single_partition_window(spark, tables):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    @F.udf("string")
    def shout(s):
        return s.upper()

    u = spark.read.parquet(tables[1])
    df = u.withColumn("c", shout("country")).withColumn("rn", F.row_number().over(Window.orderBy("id")))
    codes = {f.code for f in lint(df)}
    assert {"PL003", "PL006"} <= codes
    with pytest.raises(PlanAssertionError, match="row-at-a-time"):
        expect(df).no_row_at_a_time_udf()
    with pytest.raises(PlanAssertionError, match="single partition"):
        expect(df).no_single_partition_exchange()


def test_pandas_udf_is_vectorised(spark, tables):
    pytest.importorskip("pyarrow")
    pytest.importorskip("pandas")
    from pyspark.sql import functions as F

    @F.pandas_udf("string")
    def shout(s):
        return s.str.upper()

    df = spark.read.parquet(tables[1]).withColumn("c", shout("country"))
    expect(df).no_row_at_a_time_udf()
    assert Plan.of(df).summary.python_operators == {"ArrowEvalPython": 1}


def test_cast_blocks_pushdown(spark, tables):
    from pyspark.sql import functions as F

    e = spark.read.parquet(tables[0])
    df = e.where(F.col("amount").cast("string") == "3.0")
    assert "PL004" in {f.code for f in lint(df)}
    with pytest.raises(PlanAssertionError):
        expect(df).pushes_filter("amount")


def test_cross_join(spark, tables):
    e = spark.read.parquet(tables[0])
    u = spark.read.parquet(tables[1])
    df = e.crossJoin(u)
    # Small right side -> BroadcastNestedLoopJoin; otherwise CartesianProduct.
    with pytest.raises(PlanAssertionError):
        expect(df).no_nested_loop_join().no_cartesian_product()
    assert lint(df)[0].code in {"PL001", "PL002"}


def test_cross_join_without_broadcast(spark, tables, default_conf):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    df = spark.read.parquet(tables[0]).crossJoin(spark.read.parquet(tables[1]))
    with pytest.raises(PlanAssertionError, match="CartesianProduct"):
        expect(df).no_cartesian_product()
    findings = lint(df)
    assert findings[0].code == "PL001" and findings[0].severity == "error"


def test_lock_is_stable_across_rebuilds(spark, tables, tmp_path, default_conf):
    lock = PlanLock(tmp_path, update=False, ci=False, mode="exact")
    assert lock.check(_revenue(spark, tables), "revenue").status == "created"
    # Rebuilding the DataFrame produces new expression ids / plan ids, yet the
    # normalized plan must be byte-identical.
    for _ in range(3):
        assert lock.check(_revenue(spark, tables), "revenue").status == "matched"


def test_lock_catches_regression(spark, tables, tmp_path, default_conf):
    lock = PlanLock(tmp_path, update=False, ci=False)
    lock.check(_revenue(spark, tables), "revenue")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    with pytest.raises(PlanLockMismatch) as exc:
        lock.check(_revenue(spark, tables), "revenue")
    msg = str(exc.value)
    assert "shuffle(s): 1 → 3 (+2)" in msg
    assert "BroadcastHashJoin 1→0, SortMergeJoin 0→1" in msg


def test_lock_same_across_different_temp_dirs(spark, tmp_path_factory, tmp_path):
    """Two identical pipelines over data in different directories lock identically."""
    from pyspark.sql import functions as F

    lock = PlanLock(tmp_path, update=False, ci=False, mode="exact")
    for i in range(2):
        root = tmp_path_factory.mktemp(f"run{i}")
        spark.range(10).write.parquet(str(root / "t"))
        df = spark.read.parquet(str(root / "t")).where(F.col("id") > 3)
        status = lock.check(df, "t").status
        assert status == ("created" if i == 0 else "matched")


def test_extended_explain_text(spark, tables):
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _revenue(spark, tables).explain(True)
    plan = Plan.from_text(buf.getvalue())
    assert plan.summary.joins  # parsed the physical section of extended output


def test_formatted_explain_text(spark, tables):
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _revenue(spark, tables).explain(mode="formatted")
    plan = Plan.from_text(buf.getvalue())
    assert plan.summary.shuffles >= 1
