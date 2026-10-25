"""Capture real explain() output for the installed Spark version.

    python scripts/capture_fixtures.py

Writes tests/fixtures/real/spark-<version>/*.txt. tests/test_real_fixtures.py
then checks that every captured version parses to the same cost summary, so
adding a new Spark version to the test matrix is just: install it, run this
script, commit the files.
"""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "real" / f"spark-{pyspark.__version__}"


def explain(df, **kw) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(**kw)
    return buf.getvalue()


def main() -> int:
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    root = Path(tempfile.mkdtemp())
    spark.range(1000).select(
        F.col("id").alias("user_id"),
        (F.col("id") % 7).cast("double").alias("amount"),
        F.when(F.col("id") % 2 == 0, F.lit("2024-01-01")).otherwise(F.lit("2024-01-02")).alias("dt"),
    ).write.partitionBy("dt").parquet(str(root / "events"))
    spark.range(100).select("id", (F.col("id") % 5).cast("string").alias("country")).write.parquet(str(root / "users"))
    e = spark.read.parquet(str(root / "events"))
    u = spark.read.parquet(str(root / "users"))

    def revenue():
        return (
            e.where("dt = '2024-01-01' AND amount > 2")
            .join(u, e.user_id == u.id)
            .groupBy("country")
            .agg(F.sum("amount").alias("revenue"))
        )

    @F.udf("string")
    def shout(s):
        return s.upper()

    out = {}
    out["broadcast_join"] = explain(revenue())
    out["broadcast_join_extended"] = explain(revenue(), extended=True)
    out["broadcast_join_formatted"] = explain(revenue(), mode="formatted")
    df = revenue()
    df.collect()
    out["broadcast_join_executed"] = explain(df)
    out["udf_window"] = explain(u.withColumn("c", shout("country")).withColumn("rn", F.row_number().over(Window.orderBy("id"))))
    out["cast_blocks_pushdown"] = explain(e.where(F.col("amount").cast("string") == "3.0"))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    out["sort_merge_join"] = explain(revenue())
    out["cartesian"] = explain(e.crossJoin(u))

    OUT.mkdir(parents=True, exist_ok=True)
    for name, text in out.items():
        (OUT / f"{name}.txt").write_text(text, encoding="utf-8")
    print(f"wrote {len(out)} fixtures to {OUT}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
