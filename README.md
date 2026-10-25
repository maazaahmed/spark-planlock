# spark-planlock

**A lockfile for Spark query plans.** Test the *physical plan* of your PySpark jobs, so performance regressions (an extra shuffle, a broadcast join that turned into a sort-merge join, lost predicate pushdown, a Python UDF someone slipped in) fail in CI instead of showing up on the cluster bill.

```python
from planlock import expect

expect(daily_revenue_df) \
    .max_shuffles(1) \
    .uses_broadcast_join() \
    .pushes_filter("event_date") \
    .no_python_udf()
```

```text
planlock.expect.PlanAssertionError: Expected a BroadcastHashJoin, but the plan's joins are {'SortMergeJoin': 1}.

Physical plan (>> marks offending operators):
   AdaptiveSparkPlan isFinalPlan=false
   +- HashAggregate(keys=[country#10], functions=[sum(amount#7)])
      +- Exchange hashpartitioning(country#10, 4), ENSURE_REQUIREMENTS, [plan_id=234]
         +- HashAggregate(keys=[country#10], functions=[partial_sum(amount#7)])
            +- Project [amount#7, country#10]
>>             +- SortMergeJoin [user_id#6L], [id#9L], Inner
...
```

## Why this exists

PySpark testing tools check **data**: chispa and `assertDataFrameEqual` tell you the output rows are right. Linters check **source code**. Nothing checks **what Spark decided to do**, and that is where the cost lives:

| What changed in a PR | Do your tests catch it? |
|---|---|
| A join key gets cast, so the broadcast join becomes a SortMergeJoin with 2 extra shuffles | ❌ same rows |
| `F.col("dt").cast("date") == ...` stops the filter from being pushed into Parquet, so the job scans the whole table | ❌ same rows |
| A helper adds `.repartition()` before a `groupBy` on the same key | ❌ same rows |
| Someone wraps a column in a plain `@udf` | ❌ same rows |
| `Window.orderBy()` without `partitionBy`, so every row goes through ONE task | ❌ same rows (on test data) |
| A Spark/Databricks upgrade changes the plan | ❌ same rows |

`spark-planlock` makes the plan a first-class, reviewable, testable artifact.

## Install

```bash
pip install spark-planlock            # no hard dependency on pyspark
pip install "spark-planlock[spark]"   # with pyspark
```

Supports Python 3.9+ and PySpark 3.3+ (classic and Spark Connect). Tested on Spark 3.5 and 4.0.

## 1. Plan assertions

`expect()` accepts a DataFrame, raw `explain()` text, or a `Plan`. Every check is chainable and raises `PlanAssertionError` (an `AssertionError`) showing the plan with the offending operators marked.

| Assertion | Fails when |
|---|---|
| `no_shuffle()` / `max_shuffles(n)` / `shuffles(n)` | more (or other) `Exchange` operators than expected |
| `no_single_partition_exchange()` | all rows funnel through one task (`Exchange SinglePartition`) |
| `max_broadcasts(n)` | too many `BroadcastExchange` |
| `uses_join("broadcast" \| "sort_merge" \| "shuffle_hash" \| ..., count=None)` | the join strategy isn't used |
| `uses_broadcast_join()`, `no_sort_merge_join()`, `no_join(s)`, `max_joins(n)` | as named |
| `no_cartesian_product()`, `no_nested_loop_join()` | a cross join or a non-equi join fallback appears |
| `no_python_udf()` / `no_row_at_a_time_udf()` | rows are shipped to Python (any UDF / plain `@udf` only) |
| `pushes_filter(col, table=None)` | no real predicate on `col` reached the scan (`IsNotNull` alone doesn't count) |
| `prunes_partitions(table=None, column=None)` | `PartitionFilters` is empty |
| `reads_only([cols], table=None)` | column pruning failed, so extra columns are read |
| `max_scans(n, table=None)` | a table is read more times than expected |
| `contains("*Aggregate", count=None)` / `not_contains(op)` | generic, glob patterns allowed |
| `is_final_plan()` | an AQE plan wasn't executed (see `executed=True`) |
| `passes_lint(ignore=[...])` | any lint rule fires (see below) |
| `satisfies(fn, message)` | your own predicate on the `Plan` returns False |

`table=` matches catalog names (`db.events`, `events`) or path fragments (`raw/events`).

### Final (AQE) plans

With Adaptive Query Execution, the plan Spark *starts* with can differ from the one it *runs* (AQE can turn a SortMergeJoin into a broadcast join at runtime, or coalesce partitions). Use `executed=True` to execute the query first and assert on the final plan. Rows are counted on the executors, never collected to the driver:

```python
expect(df, executed=True).is_final_plan().uses_broadcast_join()
```

The parser understands `== Final Plan ==` / `== Initial Plan ==` sections, so nothing is counted twice.

## 2. Plan lock files (snapshot testing)

```python
import planlock

def test_daily_revenue(spark):
    df = build_daily_revenue(spark)
    planlock.check(df, "daily_revenue")
```

The first run writes `__planlocks__/daily_revenue.plan`, a **normalized** plan (expression ids like `#123L`, `plan_id`s, temp paths, bucket URIs, codegen and AQE stage numbers, and `splits=<cores>` removed), so it's byte-for-byte stable across runs and machines. Commit it.

When a later change alters the plan, the test fails and says **what got more expensive**, not just which lines differ:

```text
Query plan for 'daily_revenue' changed (lock: __planlocks__/daily_revenue.plan, mode=structure).

✗ shuffle(s): 1 → 3 (+2)
✗ sort(s): 0 → 2 (+2)
✗ join strategy changed: BroadcastHashJoin 1→0, SortMergeJoin 0→1
• broadcast(s): 1 → 0 (-1)

Operator diff:
--- locked
+++ current
@@ -3,10 +3,13 @@
     Exchange hashpartitioning
       HashAggregate
         Project
-          BroadcastHashJoin
-            Project
-              Filter
-                FileScan parquet
-            BroadcastExchange
-              Filter
-                FileScan parquet
+          SortMergeJoin
+            Sort
+              Exchange hashpartitioning
+                Project
+                  Filter
+                    FileScan parquet
+            Sort
+              Exchange hashpartitioning
+                Filter
+                  FileScan parquet

If this change is intended, re-lock with PLANLOCK_UPDATE=1 (or pytest --planlock-update) and commit the .plan file.
```

Re-locking is deliberate (`PLANLOCK_UPDATE=1`), and the `.plan` diff shows up in the pull request where a reviewer can see it.

| Option | Values | Meaning |
|---|---|---|
| `mode` | `"structure"` (default) | cost summary **and** operator tree must match |
| | `"summary"` | only the cost summary (shuffles, joins, scans, pushdown, UDFs...). Most tolerant across Spark upgrades |
| | `"exact"` | normalized plan text must match |
| `fail_on` | `"any"` (default) / `"regression"` | `"regression"` lets improvements pass without re-locking |
| `directory` | path | default `$PLANLOCK_DIR` or `./__planlocks__` |
| `extra_rules` | `[(regex, repl)]` | extra normalization, e.g. scrub an environment-specific catalog name |

Environment variables: `PLANLOCK_UPDATE=1` rewrites locks. `CI=true` (set by GitHub Actions, GitLab, etc.) makes a **missing** lock an error instead of silently creating one.

## 3. pytest plugin

Installed automatically. The `planlock` fixture names locks after the test and stores them next to the test file:

```python
def test_sessionize(spark, planlock):
    df = sessionize(load_events(spark))
    planlock.check(df)                         # -> __planlocks__/test_jobs__test_sessionize.plan
    planlock.check(df.where("country = 'US'"), "us_only")
    planlock.expect(df).no_single_partition_exchange().no_row_at_a_time_udf()
```

```bash
pytest                         # compare
pytest --planlock-update       # re-lock after an intended change
pytest --planlock-mode summary --planlock-fail-on regression
```

At the end of the run you get a summary like `plan locks: 12 matched, 1 created`. Defaults can live in `pytest.ini` / `pyproject.toml` (`planlock_mode`, `planlock_fail_on`, `planlock_dir`).

## 4. Plan linter

The lint rules run on the **physical plan**, so they catch problems you can't see in source code:

| Code | Severity | Finds |
|---|---|---|
| PL001 | error | `CartesianProduct` (cross join) |
| PL002 | warning | `BroadcastNestedLoopJoin`: no usable equality key |
| PL003 | warning | row-at-a-time Python `@udf` (`BatchEvalPython`) |
| PL004 | warning | the scan has data filters but **none were pushed down** (casts, UDFs, unsupported types) |
| PL005 | info | the same table is scanned multiple times |
| PL006 | warning | `Exchange SinglePartition`: everything goes through one task |
| PL007 | info | more than `max_shuffles` (default 6) shuffles |
| PL008 | info | many separate Python UDF operators (JVM↔Python round trips) |
| PL009 | warning | SortMergeJoin where one side is local data (should be broadcast) |

```python
from planlock import lint
for finding in lint(df):
    print(finding)
# PL004 [warning] Scan of events has data filters ['(cast(amount as string) = 3.0)'] but none were pushed down ... (line 3: FileScan parquet)
```

## 5. CLI, no Spark needed

Paste a plan from the Spark UI, Databricks, a log file or `df.explain()`:

```bash
planlock lint plan.txt                    # exit 1 on warnings (configurable --fail-on)
planlock summary plan.txt [--json]
planlock diff before.txt after.txt        # semantic diff; exit 1 on regressions
planlock tree plan.txt                    # operator skeleton
planlock normalize plan.txt               # stable text for diffs
cat plan.txt | planlock lint -
```

Simple, extended (`explain(True)`) and formatted (`explain(mode="formatted")`) output are all accepted.

## Python API

```python
from planlock import Plan

plan = Plan.of(df)               # or Plan.from_text(text), Plan.of(df, executed=True)
plan.summary                     # PlanSummary(shuffles=1, broadcasts=1, joins={'BroadcastHashJoin': 1}, ...)
plan.find("Exchange")            # [PlanNode, ...]
plan.scans()                     # [(PlanNode, ScanInfo(table, pushed_filters, partition_filters, read_schema, ...))]
plan.normalized_text()
plan.skeleton()
```

## How it works

* **Capture:** `df.explain()` output is captured from Python, so it works on classic PySpark and Spark Connect.
* **Parse:** Spark renders plans with a fixed three-character indent per level (`:- `, `+- `, `:  `). The parser rebuilds the tree from that, handles AQE `Final`/`Initial` sections and the `formatted` explain mode, and parses scan details (`PushedFilters`, `PartitionFilters`, `ReadSchema`, `Location`) with a bracket-aware field splitter.
* **Normalize:** regex rules strip run-specific noise. The test suite checks that rebuilding a DataFrame, or reading identical data from different temp dirs, produces byte-identical locks.
* **Diff:** the semantic differ compares cost summaries and labels each change as a regression or an improvement.

Because the core is pure text processing, it has zero runtime dependencies and the parser is pinned against real `explain()` output from each supported Spark version (`tests/fixtures/real/`).

## Limitations

* The plan shows *what* Spark will do, not *how much* data it will process. A broadcast join is "good" only if the broadcast side is small; pair planlock with data-volume tests or job metrics.
* Plans legitimately change across Spark major versions. Use `mode="summary"` or re-lock on upgrade; the failure message tells you when the lock was made with a different Spark version.
* Photon / Databricks-specific operators are parsed generically (the name and tree are kept); dedicated rules are welcome contributions.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). The most valuable contributions are real plan fixtures from other Spark/Databricks/EMR/Glue versions (`python scripts/capture_fixtures.py`) and new lint rules.

## License

Apache-2.0
