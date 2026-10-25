"""A tiny example pipeline used by examples/test_jobs.py."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def daily_revenue(spark: SparkSession, events_path: str, users_path: str, day: str) -> DataFrame:
    events = spark.read.parquet(events_path)
    users = spark.read.parquet(users_path)
    return (
        events.where((F.col("dt") == day) & (F.col("amount") > 0))
        .join(users, events.user_id == users.id)
        .groupBy("country")
        .agg(F.sum("amount").alias("revenue"))
    )
