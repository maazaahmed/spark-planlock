# Why spark-planlock: gap analysis

*October 2026. Notes from choosing what to build.*

## Goal

Find a data-engineering problem (Kafka, PySpark, pipelines) that a lot of teams hit and that no maintained open-source Python library solves.

## Method

General web search was not available in the environment used, so prior art was checked directly against:

* **GitHub repository search API** for the obvious phrasings of each idea (`spark physical plan assert shuffle`, `pyspark query plan regression test`, `spark plan snapshot test`, `assert_no_shuffle`, `spark explain plan diff`, `pyspark plan lint`, `catalyst plan test`, `spark shuffle assertion`, `spark plan regression`, `golden plan spark`, ...).
* **PyPI JSON API** to check package-name availability.

## What already exists (and what it does *not* do)

| Category | Examples | Tests the plan? |
|---|---|---|
| DataFrame equality / data tests | `chispa` (~780★), `pyspark.testing.assertDataFrameEqual`, `pytest-spark`, `soda-spark`, Great Expectations, pandera | No: they compare rows and schemas |
| Static source-code linters | `pyspark-antipattern`, `flake8-pyspark-with-column`, `cylint`, `sparkdoctor`, CatalystOps (VS Code) | No: they read Python AST, not what Catalyst decided |
| Runtime metrics / profilers | sparkMeasure, Sparklens (archived, Scala), DataFlint (UI plugin) | After the fact, on a cluster; not a CI gate |
| One-off "plan analyzer" notebooks/repos | several 0★ repos (`spark-query-plan-analyzer`, `spark-plan-doctor-py`, ...) | Ad-hoc diagnostics; no assertions, no snapshots, no pytest integration |

Searches for plan assertions, plan snapshot/golden testing, or semantic plan diffing returned **zero** repositories.

## The gap

Every performance property of a Spark job (how many shuffles, which join strategy, whether predicates and partitions are pruned, whether rows go through Python) is decided by the optimizer and is only visible in the **physical plan**. Yet:

1. No library lets you **assert** on plan properties in a unit test.
2. No library gives you a **reviewable, stable artifact** of the plan (raw `explain()` output changes on every run because of expression ids, `plan_id`s, temp paths and core counts).
3. No library explains a plan change **in cost terms** ("BroadcastHashJoin → SortMergeJoin, +2 shuffles") rather than as a text diff.

The result is that plan regressions go unnoticed until they show up as slower jobs and higher cloud bills, even though the change was visible in the plan at PR time.

## Other ideas considered

| Idea | Why not (for now) |
|---|---|
| Kafka DLQ / retry-topic helpers for `confluent-kafka-python` | Covered by Quix Streams, Faust forks and many smaller libs |
| Kafka consumer-lag-in-time exporter | Burrow, kafka-lag-exporter, Kafka UI tools exist |
| Data contracts / schema-drift checks | datacontract-cli, Soda, GX, pandera |
| Skew detection / auto-salting for joins | Partially covered by AQE skew-join handling; a good follow-up feature for planlock (`skew` lint rule using executed-plan metrics) |
| Spark event-log analyzer | Sparklens/sparkMeasure/DataFlint occupy the space |

## Name

`planlock` is used by several unrelated GitHub projects (none on PyPI, none about Spark), so the distribution is **`spark-planlock`** (free on PyPI as of 2026-10-24) with import name `planlock`.

## Possible next steps

* Rules that read **executed-plan metrics** (skewed partitions, spill, broadcast size) when available.
* Photon / Databricks operator coverage (fixtures welcome).
* A GitHub Action that comments the semantic diff of changed `.plan` files on PRs.
* Scala/JVM port using the same lock format.
