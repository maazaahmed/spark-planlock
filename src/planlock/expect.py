"""Fluent assertions about a physical plan.

    from planlock import expect

    expect(df).max_shuffles(1).uses_broadcast_join().pushes_filter("dt")

Every method returns ``self`` so checks can be chained, and raises
:class:`PlanAssertionError` (a subclass of ``AssertionError``, so pytest shows
it nicely) with the offending plan attached.
"""

from __future__ import annotations

import fnmatch
import re
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple

from .parser import PlanNode, ScanInfo
from .plan import JOIN_OPERATORS, PYTHON_OPERATORS, ROW_AT_A_TIME_PYTHON, Plan

__all__ = ["expect", "PlanExpectation", "PlanAssertionError"]

_JOIN_ALIASES = {
    "broadcast": "BroadcastHashJoin",
    "bhj": "BroadcastHashJoin",
    "broadcast_hash": "BroadcastHashJoin",
    "sort_merge": "SortMergeJoin",
    "smj": "SortMergeJoin",
    "merge": "SortMergeJoin",
    "shuffle_hash": "ShuffledHashJoin",
    "shj": "ShuffledHashJoin",
    "nested_loop": "BroadcastNestedLoopJoin",
    "bnlj": "BroadcastNestedLoopJoin",
    "cartesian": "CartesianProduct",
}


def _join_name(strategy: str) -> str:
    key = strategy.strip()
    if key in JOIN_OPERATORS:
        return key
    alias = _JOIN_ALIASES.get(key.lower().replace("-", "_").replace(" ", "_"))
    if alias:
        return alias
    raise ValueError(
        f"Unknown join strategy {strategy!r}. Use one of {sorted(JOIN_OPERATORS)} or {sorted(_JOIN_ALIASES)}"
    )


class PlanAssertionError(AssertionError):
    """Raised when a plan expectation fails. ``.plan`` holds the plan."""

    def __init__(self, message: str, plan: Plan, nodes: Sequence[PlanNode] = ()) -> None:
        self.plan = plan
        self.nodes = list(nodes)
        super().__init__(self._render(message))

    def _render(self, message: str) -> str:
        marked = {id(n) for n in self.nodes}
        lines = []
        for node in self.plan.all_nodes():
            prefix = ">> " if id(node) in marked else "   "
            lines.append(prefix + node.raw)
        header = "Physical plan (>> marks offending operators):" if marked else "Physical plan:"
        return f"{message}\n\n{header}\n" + "\n".join(lines)


class PlanExpectation:
    """Chainable assertions bound to one :class:`~planlock.Plan`."""

    def __init__(self, plan: Plan) -> None:
        self.plan = plan

    # ------------------------------------------------------------ internals
    def _fail(self, message: str, nodes: Iterable[PlanNode] = ()) -> None:
        raise PlanAssertionError(message, self.plan, list(nodes))

    def _scans(self, table: Optional[str]) -> List[Tuple[PlanNode, ScanInfo]]:
        scans = self.plan.scans()
        if table is None:
            return scans
        return [(n, i) for n, i in scans if _matches_relation(i, table)]

    def _require_scans(self, table: Optional[str]) -> List[Tuple[PlanNode, ScanInfo]]:
        scans = self._scans(table)
        if not scans:
            where = f" matching {table!r}" if table else ""
            known = sorted({i.identity for _, i in self.plan.scans()}) or ["<none>"]
            self._fail(f"No file/datasource scan{where} found in plan. Scans present: {known}")
        return scans

    # ------------------------------------------------------------- shuffles
    def no_shuffle(self) -> PlanExpectation:
        """No ``Exchange`` (shuffle) at all. Broadcasts are allowed."""
        return self.max_shuffles(0)

    def max_shuffles(self, n: int) -> PlanExpectation:
        """At most ``n`` shuffles (``Exchange`` operators; broadcasts excluded)."""
        found = self.plan.find("Exchange")
        if len(found) > n:
            self._fail(f"Expected at most {n} shuffle(s), found {len(found)}.", found)
        return self

    def shuffles(self, n: int) -> PlanExpectation:
        """Exactly ``n`` shuffles."""
        found = self.plan.find("Exchange")
        if len(found) != n:
            self._fail(f"Expected exactly {n} shuffle(s), found {len(found)}.", found)
        return self

    def no_single_partition_exchange(self) -> PlanExpectation:
        """No ``Exchange SinglePartition`` (all rows funnelled into one task).

        Typical cause: ``Window.orderBy(...)`` without ``partitionBy``, or a
        global ``limit``/``monotonically`` pattern.
        """
        bad = [n for n in self.plan.find("Exchange") if n.detail.startswith("SinglePartition")]
        if bad:
            self._fail("Plan funnels all rows through a single partition (Exchange SinglePartition).", bad)
        return self

    def max_broadcasts(self, n: int) -> PlanExpectation:
        found = self.plan.find("BroadcastExchange")
        if len(found) > n:
            self._fail(f"Expected at most {n} broadcast(s), found {len(found)}.", found)
        return self

    # ---------------------------------------------------------------- joins
    def uses_join(self, strategy: str, count: Optional[int] = None) -> PlanExpectation:
        """At least one join of ``strategy`` (or exactly ``count`` of them).

        ``strategy`` is an operator name (``"SortMergeJoin"``) or alias
        (``"broadcast"``, ``"sort_merge"``, ``"shuffle_hash"``, ``"nested_loop"``, ``"cartesian"``).
        """
        name = _join_name(strategy)
        found = self.plan.find(name)
        if count is None and not found:
            joins = self.plan.summary.joins or {"<no joins>": 0}
            self._fail(f"Expected a {name}, but the plan's joins are {joins}.", self._all_joins())
        if count is not None and len(found) != count:
            self._fail(f"Expected exactly {count} {name}, found {len(found)}.", self._all_joins())
        return self

    def uses_broadcast_join(self, count: Optional[int] = None) -> PlanExpectation:
        return self.uses_join("BroadcastHashJoin", count)

    def no_join(self, strategy: str) -> PlanExpectation:
        name = _join_name(strategy)
        found = self.plan.find(name)
        if found:
            self._fail(f"Expected no {name}, found {len(found)}.", found)
        return self

    def no_sort_merge_join(self) -> PlanExpectation:
        return self.no_join("SortMergeJoin")

    def no_cartesian_product(self) -> PlanExpectation:
        return self.no_join("CartesianProduct")

    def no_nested_loop_join(self) -> PlanExpectation:
        return self.no_join("BroadcastNestedLoopJoin")

    def max_joins(self, n: int) -> PlanExpectation:
        found = self._all_joins()
        if len(found) > n:
            self._fail(f"Expected at most {n} join(s), found {len(found)}.", found)
        return self

    def _all_joins(self) -> List[PlanNode]:
        return [n for n in self.plan.nodes() if n.name in JOIN_OPERATORS]

    # --------------------------------------------------------------- python
    def no_python_udf(self) -> PlanExpectation:
        """No operator that ships rows to a Python worker (any UDF flavour)."""
        found = [n for n in self.plan.nodes() if n.name in PYTHON_OPERATORS]
        if found:
            self._fail(f"Expected no Python UDF operators, found {sorted({n.name for n in found})}.", found)
        return self

    def no_row_at_a_time_udf(self) -> PlanExpectation:
        """No plain ``@udf`` (``BatchEvalPython``). Vectorised pandas/Arrow UDFs are allowed."""
        found = [n for n in self.plan.nodes() if n.name in ROW_AT_A_TIME_PYTHON]
        if found:
            self._fail(
                "Found a row-at-a-time Python UDF (BatchEvalPython). Consider a built-in function "
                "or a pandas_udf / useArrow=True UDF.",
                found,
            )
        return self

    # ---------------------------------------------------------------- scans
    def max_scans(self, n: int, table: Optional[str] = None) -> PlanExpectation:
        """At most ``n`` scans (of ``table`` if given). Catches accidental double reads."""
        scans = self._scans(table)
        if len(scans) > n:
            what = f"of {table!r}" if table else "in total"
            self._fail(f"Expected at most {n} scan(s) {what}, found {len(scans)}.", [s for s, _ in scans])
        return self

    def pushes_filter(self, column: str, table: Optional[str] = None) -> PlanExpectation:
        """A predicate on ``column`` is pushed down into the scan (``PushedFilters``).

        Spark adds ``IsNotNull(col)`` to almost every scan, so null checks alone
        do not count: there must be a real predicate (``EqualTo``, ``GreaterThan``,
        ``In``...) on the column.
        """
        scans = self._require_scans(table)
        for _, info in scans:
            candidates = [f for f in info.pushed_filters + info.partition_filters if not _is_null_check(f)]
            if any(_mentions_column(f, column) for f in candidates):
                return self
        pushed = {i.identity: i.pushed_filters for _, i in scans}
        self._fail(
            f"No filter on column {column!r} was pushed down to the scan. PushedFilters: {pushed}",
            [s for s, _ in scans],
        )
        return self

    def prunes_partitions(self, table: Optional[str] = None, column: Optional[str] = None) -> PlanExpectation:
        """The scan uses partition pruning (non-empty ``PartitionFilters``)."""
        scans = self._require_scans(table)
        for _, info in scans:
            pf = [f for f in info.partition_filters if not _is_null_check(f)]
            if pf and (column is None or any(_mentions_column(f, column) for f in pf)):
                return self
        on = f" on {column!r}" if column else ""
        self._fail(f"Expected partition pruning{on}, but PartitionFilters are empty.", [s for s, _ in scans])
        return self

    def reads_only(self, columns: Sequence[str], table: Optional[str] = None) -> PlanExpectation:
        """Column pruning works: scans read no columns outside ``columns``."""
        allowed = {c.lower() for c in columns}
        for node, info in self._require_scans(table):
            cols = info.read_schema or info.columns
            extra = [c for c in cols if c.lower() not in allowed]
            if extra:
                self._fail(f"Scan of {info.identity} reads extra column(s) {extra}; allowed {sorted(allowed)}.", [node])
        return self

    # -------------------------------------------------------------- generic
    def contains(self, operator: str, count: Optional[int] = None) -> PlanExpectation:
        """Plan contains ``operator`` (glob patterns allowed, e.g. ``"*Aggregate"``)."""
        found = self._match(operator)
        if count is None and not found:
            self._fail(f"Expected an operator matching {operator!r}, found none.")
        if count is not None and len(found) != count:
            self._fail(f"Expected {count} operator(s) matching {operator!r}, found {len(found)}.", found)
        return self

    def not_contains(self, operator: str) -> PlanExpectation:
        found = self._match(operator)
        if found:
            self._fail(f"Expected no operator matching {operator!r}, found {len(found)}.", found)
        return self

    def _match(self, pattern: str) -> List[PlanNode]:
        return [
            n
            for n in self.plan.nodes()
            if fnmatch.fnmatchcase(n.name, pattern) or fnmatch.fnmatchcase(n.label, pattern)
        ]

    def is_final_plan(self) -> PlanExpectation:
        """The plan is a finished AQE plan (captured with ``executed=True``)."""
        if self.plan.is_adaptive and not self.plan.is_final:
            self._fail("Plan is an AQE plan that has not been executed (isFinalPlan=false).")
        return self

    def passes_lint(self, ignore: Sequence[str] = (), min_severity: str = "warning") -> PlanExpectation:
        """No lint findings at ``min_severity`` or above (see :mod:`planlock.lint`)."""
        from .lint import SEVERITY_ORDER, lint

        floor = SEVERITY_ORDER[min_severity]
        findings = [f for f in lint(self.plan) if f.code not in ignore and SEVERITY_ORDER[f.severity] >= floor]
        if findings:
            msg = "Plan has lint findings:\n" + "\n".join(f"  {f}" for f in findings)
            self._fail(msg, [f.node for f in findings if f.node is not None])
        return self

    def satisfies(
        self, predicate: Callable[[Plan], bool], message: str = "Custom plan predicate failed"
    ) -> PlanExpectation:
        if not predicate(self.plan):
            self._fail(message)
        return self


def expect(obj: Any, *, executed: bool = False) -> PlanExpectation:
    """Start a chain of plan assertions.

    ``obj`` may be a DataFrame, explain text, or a :class:`~planlock.Plan`.
    Pass ``executed=True`` to assert on the final AQE plan.
    """
    return PlanExpectation(Plan.of(obj, executed=executed))


# ---------------------------------------------------------------------------


def _matches_relation(info: ScanInfo, pattern: str) -> bool:
    candidates = [c for c in (info.table, info.location) if c]
    for c in candidates:
        if fnmatch.fnmatchcase(c, pattern) or c == pattern or c.split(".")[-1] == pattern:
            return True
        if pattern in c:
            return True
    return False


def _is_null_check(filter_text: str) -> bool:
    return filter_text.strip().lower().startswith(("isnotnull(", "isnull("))


def _mentions_column(filter_text: str, column: str) -> bool:
    col = re.escape(column.strip("`"))
    return re.search(rf"(?<![\w.]){col}(?![\w])", filter_text.replace("`", "")) is not None
