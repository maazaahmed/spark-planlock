"""Plans captured from real Spark versions (scripts/capture_fixtures.py).

These run without Spark and pin the parser against the exact text each Spark
version prints. Every version must produce the same cost summary.
"""

from pathlib import Path

import pytest

from planlock import Plan, PlanLock, lint

REAL = Path(__file__).parent / "fixtures" / "real"
VERSIONS = sorted(p.name for p in REAL.iterdir() if p.is_dir()) if REAL.exists() else []

EXPECTED = {
    "broadcast_join": dict(shuffles=1, broadcasts=1, joins={"BroadcastHashJoin": 1}, python_operators={}),
    "broadcast_join_extended": dict(shuffles=1, broadcasts=1, joins={"BroadcastHashJoin": 1}, python_operators={}),
    "broadcast_join_executed": dict(shuffles=1, broadcasts=1, joins={"BroadcastHashJoin": 1}, python_operators={}),
    "sort_merge_join": dict(shuffles=3, broadcasts=0, joins={"SortMergeJoin": 1}, python_operators={}),
    "udf_window": dict(shuffles=1, broadcasts=0, joins={}, python_operators={"BatchEvalPython": 1}),
    "cartesian": dict(shuffles=0, broadcasts=0, joins={"CartesianProduct": 1}, python_operators={}),
    "cast_blocks_pushdown": dict(shuffles=0, broadcasts=0, joins={}, python_operators={}),
}

LINT = {
    "broadcast_join": set(),
    "sort_merge_join": set(),
    "udf_window": {"PL003", "PL006"},
    "cartesian": {"PL001"},
    "cast_blocks_pushdown": {"PL004"},
}


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("name", sorted(EXPECTED))
def test_summary(version, name):
    plan = Plan.from_text((REAL / version / f"{name}.txt").read_text())
    s = plan.summary
    got = dict(shuffles=s.shuffles, broadcasts=s.broadcasts, joins=s.joins, python_operators=s.python_operators)
    assert got == EXPECTED[name]


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("name", sorted(LINT))
def test_lint(version, name):
    codes = {f.code for f in lint((REAL / version / f"{name}.txt").read_text())}
    assert codes == LINT[name]


@pytest.mark.parametrize("version", VERSIONS)
def test_formatted_mode(version):
    plan = Plan.from_text((REAL / version / "broadcast_join_formatted.txt").read_text())
    assert plan.summary.shuffles == 1
    assert plan.summary.joins == {"BroadcastHashJoin": 1}


@pytest.mark.parametrize("version", VERSIONS)
def test_executed_plan_is_final(version):
    plan = Plan.from_text((REAL / version / "broadcast_join_executed.txt").read_text())
    assert plan.is_final


@pytest.mark.parametrize("version", VERSIONS)
def test_lock_from_simple_equals_lock_from_extended(tmp_path, version):
    """explain() and explain(True) match in the default "structure" mode.

    (Extended mode prints extra ``output=[...]`` details on aggregates, so
    "exact" mode only matches when both sides use the same explain mode.)
    """
    lock = PlanLock(tmp_path, ci=False, update=False, mode="structure")
    lock.check((REAL / version / "broadcast_join.txt").read_text(), "q")
    assert lock.check((REAL / version / "broadcast_join_extended.txt").read_text(), "q").status == "matched"


def test_versions_present():
    assert VERSIONS, "no real fixtures captured; run scripts/capture_fixtures.py"
