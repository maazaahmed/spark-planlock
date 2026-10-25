import pytest

from planlock import Plan, PlanAssertionError, expect, format_changes, lint, normalize_plan_text, semantic_diff


# ----------------------------------------------------------------- expect
def test_expectations_pass(aqe_final_text):
    (
        expect(aqe_final_text)
        .max_shuffles(1)
        .shuffles(1)
        .uses_broadcast_join()
        .uses_join("broadcast", count=1)
        .no_sort_merge_join()
        .no_cartesian_product()
        .no_python_udf()
        .pushes_filter("dt", table="events")
        .prunes_partitions(table="events", column="dt")
        .reads_only(["user_id", "amount"], table="events")
        .max_scans(1, table="users")
        .contains("*Aggregate", count=2)
        .not_contains("Window")
        .is_final_plan()
        .no_single_partition_exchange()
    )


def test_expectation_failure_marks_operators(smj_udf_window_text):
    with pytest.raises(PlanAssertionError) as exc:
        expect(smj_udf_window_text).max_shuffles(1)
    msg = str(exc.value)
    assert "at most 1 shuffle(s), found 3" in msg
    assert sum(line.startswith(">> ") for line in msg.splitlines()) == 3
    assert isinstance(exc.value, AssertionError)


@pytest.mark.parametrize(
    "check",
    [
        lambda e: e.uses_broadcast_join(),
        lambda e: e.no_sort_merge_join(),
        lambda e: e.no_python_udf(),
        lambda e: e.no_row_at_a_time_udf(),
        lambda e: e.no_single_partition_exchange(),
        lambda e: e.pushes_filter("ts"),
        lambda e: e.prunes_partitions(),
        lambda e: e.reads_only(["id"]),
        lambda e: e.passes_lint(),
        lambda e: e.max_joins(0),
        lambda e: e.satisfies(lambda p: p.summary.sorts == 0, "no sorts please"),
    ],
)
def test_expectations_fail(smj_udf_window_text, check):
    with pytest.raises(PlanAssertionError):
        check(expect(smj_udf_window_text))


def test_null_checks_do_not_count_as_pushdown(aqe_final_text):
    # Only IsNotNull(user_id) is pushed - that is not a real predicate.
    with pytest.raises(PlanAssertionError, match="No filter on column 'user_id'"):
        expect(aqe_final_text).pushes_filter("user_id", table="events")


def test_unknown_join_alias():
    with pytest.raises(ValueError):
        expect("Range (0, 1, step=1, splits=1)").uses_join("teleport")


def test_missing_scan_is_clear_error(aqe_final_text):
    with pytest.raises(PlanAssertionError, match="No file/datasource scan matching 'orders'"):
        expect(aqe_final_text).pushes_filter("id", table="orders")


# ------------------------------------------------------------------- lint
def test_lint_findings(smj_udf_window_text):
    codes = [f.code for f in lint(smj_udf_window_text)]
    assert "PL003" in codes  # BatchEvalPython
    assert "PL004" in codes  # cast() blocked pushdown
    assert "PL006" in codes  # SinglePartition
    assert "PL009" in codes  # SMJ against local data
    assert "PL001" not in codes


def test_lint_clean_plan(aqe_final_text):
    assert lint(aqe_final_text) == []


def test_lint_select_ignore_and_config(smj_udf_window_text):
    assert [f.code for f in lint(smj_udf_window_text, select=["PL006"])] == ["PL006"]
    assert "PL006" not in [f.code for f in lint(smj_udf_window_text, ignore=["PL006"])]
    assert [f.code for f in lint(smj_udf_window_text, select=["PL007"], max_shuffles=2)] == ["PL007"]


def test_lint_cartesian_and_repeated_scan():
    text = """CartesianProduct
:- FileScan parquet db.t[a#1] Batched: true, DataFilters: [], Format: Parquet, Location: x, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>
+- FileScan parquet db.t[a#2] Batched: true, DataFilters: [], Format: Parquet, Location: x, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<a:int>"""
    findings = lint(text)
    assert findings[0].code == "PL001" and findings[0].severity == "error"
    assert "PL005" in [f.code for f in findings]


# ------------------------------------------------------------------- diff
def test_semantic_diff_detects_regressions(aqe_final_text, smj_udf_window_text):
    changes = semantic_diff(Plan.from_text(aqe_final_text), Plan.from_text(smj_udf_window_text))
    kinds = {c.kind: c for c in changes}
    assert kinds["shuffles"].severity == "regression"
    assert kinds["joins"].severity == "regression"
    assert kinds["python_operators"].severity == "regression"
    rendered = format_changes(changes)
    assert "shuffle(s): 1 → 3 (+2)" in rendered
    # regressions are listed first
    assert rendered.splitlines()[0].startswith("✗")


def test_semantic_diff_detects_improvement(aqe_final_text, smj_udf_window_text):
    changes = semantic_diff(Plan.from_text(smj_udf_window_text), Plan.from_text(aqe_final_text))
    assert {c.kind: c.severity for c in changes}["shuffles"] == "improvement"


def test_pushdown_loss_is_regression(aqe_final_text):
    worse = aqe_final_text.replace("PushedFilters: [IsNotNull(user_id)]", "PushedFilters: []")
    changes = semantic_diff(Plan.from_text(aqe_final_text), Plan.from_text(worse))
    assert any(c.kind == "pushdown" and c.severity == "regression" for c in changes)


def test_no_changes(aqe_final_text):
    assert semantic_diff(Plan.from_text(aqe_final_text), Plan.from_text(aqe_final_text)) == []


# -------------------------------------------------------------- normalize
def test_normalize_strips_run_specific_noise():
    raw = (
        "*(3) Project [id#12L, name#13]\n"
        "+- Exchange hashpartitioning(id#12L, 200), ENSURE_REQUIREMENTS, [plan_id=345]\n"
        "   +- FileScan parquet [id#12L] Location: InMemoryFileIndex(3 paths)[file:/tmp/pytest-1/a, s3a://b/c], "
        "ReadSchema: struct<id:bigint>\n"
        "      +- Range (0, 10, step=1, splits=16)"
    )
    out = normalize_plan_text(raw)
    assert "#" not in out
    assert "plan_id" not in out
    assert "*(N) Project [id, name]" in out
    assert "InMemoryFileIndex(N paths)[<path>, <path>]" in out
    assert "splits=N" in out


def test_normalize_extra_rules():
    assert normalize_plan_text("FileScan prod_bucket_7", [(r"prod_bucket_\d+", "<bucket>")]) == "FileScan <bucket>"
