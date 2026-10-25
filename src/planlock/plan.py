"""The :class:`Plan` object: a parsed physical plan plus derived metrics."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from .normalize import normalize_plan_text
from .parser import PlanNode, ScanInfo, parse_plan

__all__ = ["Plan", "PlanSummary", "JOIN_OPERATORS", "PYTHON_OPERATORS"]

JOIN_OPERATORS = {
    "BroadcastHashJoin",
    "SortMergeJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
}

# Operators that move rows between the JVM and a Python worker.
PYTHON_OPERATORS = {
    "BatchEvalPython",  # row-at-a-time @udf
    "ArrowEvalPython",  # @pandas_udf / arrow-optimised udf
    "FlatMapGroupsInPandas",
    "FlatMapCoGroupsInPandas",
    "FlatMapGroupsInArrow",
    "FlatMapCoGroupsInArrow",
    "MapInPandas",
    "MapInArrow",
    "PythonMapInArrow",
    "AggregateInPandas",
    "ArrowAggregatePython",
    "WindowInPandas",
    "ArrowWindowPython",
    "BatchEvalPythonUDTF",
    "ArrowEvalPythonUDTF",
    "PythonDataSourceScan",
}

ROW_AT_A_TIME_PYTHON = {"BatchEvalPython", "BatchEvalPythonUDTF"}

AGGREGATE_OPERATORS = {"HashAggregate", "ObjectHashAggregate", "SortAggregate"}


@dataclass
class PlanSummary:
    """Cost-relevant facts about a plan. This is what snapshots compare."""

    shuffles: int = 0
    shuffle_partitionings: List[str] = field(default_factory=list)
    single_partition_exchanges: int = 0
    reused_exchanges: int = 0
    broadcasts: int = 0
    joins: Dict[str, int] = field(default_factory=dict)
    sorts: int = 0
    aggregates: int = 0
    windows: int = 0
    python_operators: Dict[str, int] = field(default_factory=dict)
    scans: List[Dict[str, Any]] = field(default_factory=list)
    cached_scans: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PlanSummary:
        known = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**known)


class Plan:
    """A Spark physical plan.

    Build one with :meth:`Plan.of` (from a DataFrame, explain text or another
    ``Plan``).
    """

    def __init__(self, text: str, *, source: str = "text") -> None:
        self.raw_text = text
        self.source = source
        self.root: PlanNode = parse_plan(text)

    # ------------------------------------------------------------ factories
    @classmethod
    def of(cls, obj: Any, *, executed: bool = False) -> Plan:
        """Coerce ``obj`` into a :class:`Plan`.

        ``obj`` may be a PySpark DataFrame (classic or Spark Connect), explain
        output as a string, or a ``Plan``. With ``executed=True`` the
        DataFrame is run first (into the ``noop`` sink) so that the *final*
        adaptive (AQE) plan is captured instead of the initial one.
        """
        if isinstance(obj, Plan):
            return obj
        if isinstance(obj, str):
            return cls(obj, source="text")
        from ._capture import explain_text  # lazy: pyspark is optional

        return cls(explain_text(obj, executed=executed), source="dataframe")

    @classmethod
    def from_text(cls, text: str) -> Plan:
        return cls(text, source="text")

    # ------------------------------------------------------------- traversal
    def all_nodes(self) -> Iterator[PlanNode]:
        """Every node, including the AQE ``Initial Plan`` branch if present."""
        return self.root.walk()

    def nodes(self) -> Iterator[PlanNode]:
        """The nodes that will actually run.

        For an executed adaptive plan Spark prints both the ``Final Plan`` and
        the ``Initial Plan``; only the final one is real, so the initial branch
        is skipped (otherwise every shuffle would be counted twice).
        """

        def visit(node: PlanNode) -> Iterator[PlanNode]:
            headers = [c for c in node.children if c.is_section_header]
            final = [c for c in headers if "Final" in c.name]
            if not node.is_section_header:
                yield node
            for child in node.children:
                if final and child.is_section_header and child not in final:
                    continue
                yield from visit(child)

        return visit(self.root)

    def find(self, name: str) -> List[PlanNode]:
        """All effective nodes whose operator name (or label) equals ``name``."""
        return [n for n in self.nodes() if n.name == name or n.label == name]

    def contains(self, name: str) -> bool:
        return bool(self.find(name))

    @property
    def is_adaptive(self) -> bool:
        return self.root.name == "AdaptiveSparkPlan"

    @property
    def is_final(self) -> bool:
        return self.is_adaptive and "isFinalPlan=true" in self.root.detail

    # ---------------------------------------------------------------- scans
    def scans(self) -> List[Tuple[PlanNode, ScanInfo]]:
        out = []
        for node in self.nodes():
            info = node.scan_info()
            if info is not None:
                out.append((node, info))
        return out

    # -------------------------------------------------------------- summary
    @property
    def summary(self) -> PlanSummary:
        if not hasattr(self, "_summary"):
            self._summary = self._summarize()
        return self._summary

    def _summarize(self) -> PlanSummary:
        s = PlanSummary()
        joins: Counter = Counter()
        py: Counter = Counter()
        for node in self.nodes():
            name = node.name
            if name == "Exchange":
                s.shuffles += 1
                kind = node.detail.split("(", 1)[0].split(",", 1)[0].split(" ", 1)[0].strip()
                s.shuffle_partitionings.append(kind or "unknown")
                if kind == "SinglePartition":
                    s.single_partition_exchanges += 1
            elif name == "ReusedExchange":
                s.reused_exchanges += 1
            elif name == "BroadcastExchange":
                s.broadcasts += 1
            elif name in JOIN_OPERATORS:
                joins[name] += 1
            elif name == "Sort":
                s.sorts += 1
            elif name in AGGREGATE_OPERATORS:
                s.aggregates += 1
            elif name == "Window":
                s.windows += 1
            elif name in PYTHON_OPERATORS:
                py[name] += 1
            elif name == "InMemoryTableScan":
                s.cached_scans += 1
        s.joins = dict(sorted(joins.items()))
        s.python_operators = dict(sorted(py.items()))
        for node, info in self.scans():
            s.scans.append(
                {
                    "operator": node.label,
                    "relation": _normalize_identity(info.identity),
                    "pushed_filters": [_normalize_identity(f) for f in info.pushed_filters],
                    "partition_filters": [_normalize_identity(f) for f in info.partition_filters],
                    "read_columns": len(info.read_schema),
                }
            )
        s.scans.sort(key=lambda d: (d["relation"], d["operator"]))
        return s

    # ---------------------------------------------------------- rendering
    def normalized_text(self, extra_rules: Sequence[Tuple[str, str]] = ()) -> str:
        """The plan with run-specific noise removed (stable across runs)."""
        lines = [n.raw for n in self.all_nodes()]
        return normalize_plan_text("\n".join(lines), extra_rules)

    def skeleton(self) -> str:
        """Operator tree only (no arguments). Robust to cosmetic changes."""
        out: List[str] = []
        # Only the effective tree (AQE initial branch dropped), re-indented.
        effective = set(id(n) for n in self.nodes())

        def visit_eff(node: PlanNode, depth: int) -> None:
            if id(node) in effective:
                label = node.label
                if node.name == "Exchange":
                    kind = node.detail.split("(", 1)[0].split(",", 1)[0].split(" ", 1)[0]
                    label = f"Exchange {kind}".strip()
                out.append("  " * depth + label)
                depth += 1
            for c in node.children:
                visit_eff(c, depth)

        visit_eff(self.root, 0)
        return "\n".join(out)

    def __repr__(self) -> str:
        s = self.summary
        return (
            f"<Plan root={self.root.name} shuffles={s.shuffles} joins={s.joins} "
            f"broadcasts={s.broadcasts} python={sum(s.python_operators.values())}>"
        )

    def __str__(self) -> str:
        return "\n".join(n.raw for n in self.all_nodes())


def _normalize_identity(text: Optional[str]) -> str:
    return normalize_plan_text(text) if text else ""


Plannable = Union[Plan, str, Any]
"""Anything accepted by :meth:`Plan.of`: a DataFrame, explain text or a Plan."""
