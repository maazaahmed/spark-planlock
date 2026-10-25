"""Example: guard a pipeline's query plan in CI.

    cd examples && pytest -q            # first run creates __planlocks__/*.plan
    pytest -q                           # later runs compare against them
    pytest -q --planlock-update         # re-lock after an intended change
"""

import pytest
from jobs import daily_revenue
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


@pytest.fixture(scope="session")
def paths(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    spark.range(1000).select(
        F.col("id").alias("user_id"),
        (F.col("id") % 10).cast("double").alias("amount"),
        F.when(F.col("id") % 2 == 0, "2024-01-01").otherwise("2024-01-02").alias("dt"),
    ).write.partitionBy("dt").parquet(str(root / "events"))
    spark.range(50).select("id", (F.col("id") % 3).cast("string").alias("country")).write.parquet(str(root / "users"))
    return str(root / "events"), str(root / "users")


def test_daily_revenue_plan(spark, paths, planlock):
    df = daily_revenue(spark, *paths, day="2024-01-01")

    # Hard rules this job must always satisfy:
    planlock.expect(df) \
        .uses_broadcast_join() \
        .max_shuffles(1) \
        .prunes_partitions(table="events", column="dt") \
        .pushes_filter("amount", table="events") \
        .no_python_udf() \
        .passes_lint()

    # And lock the full plan so any other change shows up in review:
    planlock.check(df)
