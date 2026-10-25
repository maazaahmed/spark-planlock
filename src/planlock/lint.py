"""Rules that flag expensive or risky patterns in a physical plan.

Unlike source-code linters, these rules look at what Spark *actually decided
to do*, so they catch problems that are invisible in the code (a join that
silently fell back to a nested loop, a filter that could not be pushed down,
a table read three times).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from .parser import PlanNode
from .plan import ROW_AT_A_TIME_PYTHON, Plan

__all__ = ["Finding", "lint", "RULES", "SEVERITY_ORDER"]

SEVERITY_ORDER = {"info": 0, "warning": 1, "error": 2}


@dataclass
class Finding:
    code: str
    severity: str
    message: str
    node: Optional[PlanNode] = None

    def __str__(self) -> str:
        where = f" (line {self.node.line_no}: {self.node.label})" if self.node is not None else ""
        return f"{self.code} [{self.severity}] {self.message}{where}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "line": self.node.line_no if self.node else None,
            "operator": self.node.label if self.node else None,
        }


Rule = Callable[[Plan, Dict[str, Any]], List[Finding]]
RULES: Dict[str, Rule] = {}


def rule(code: str) -> Callable[[Rule], Rule]:
    def deco(fn: Rule) -> Rule:
        RULES[code] = fn
        fn.code = code  # type: ignore[attr-defined]
        return fn

    return deco


@rule("PL001")
def cartesian_product(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """CartesianProduct: every row joined with every row."""
    return [
        Finding("PL001", "error", "CartesianProduct (cross join) - output grows as rows(left) x rows(right).", n)
        for n in plan.find("CartesianProduct")
    ]


@rule("PL002")
def nested_loop_join(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """BroadcastNestedLoopJoin: usually a non-equi join condition or a missing join key."""
    return [
        Finding(
            "PL002",
            "warning",
            "BroadcastNestedLoopJoin - join has no usable equality key; cost is O(left x right).",
            n,
        )
        for n in plan.find("BroadcastNestedLoopJoin")
    ]


@rule("PL003")
def row_at_a_time_udf(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """Plain Python @udf: rows are pickled one at a time to a Python worker."""
    return [
        Finding(
            "PL003",
            "warning",
            "Row-at-a-time Python UDF (BatchEvalPython). Prefer built-in functions, a pandas_udf, or useArrow=True.",
            n,
        )
        for n in plan.nodes()
        if n.name in ROW_AT_A_TIME_PYTHON
    ]


@rule("PL004")
def filter_not_pushed(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """The scan evaluates filters itself but none reached the data source."""
    out = []
    for node, info in plan.scans():
        real_filters = [f for f in info.data_filters if not f.lower().startswith("isnotnull(")]
        pushed_real = [f for f in info.pushed_filters if not f.startswith("IsNotNull(")]
        if real_filters and not pushed_real:
            out.append(
                Finding(
                    "PL004",
                    "warning",
                    f"Scan of {info.identity} has data filters {real_filters} but none were pushed down "
                    "(casts on the column, UDFs and unsupported types block pushdown).",
                    node,
                )
            )
    return out


@rule("PL005")
def repeated_scan(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """The same relation is read more than once without caching or exchange reuse."""
    groups: Dict[str, List[PlanNode]] = defaultdict(list)
    for node, info in plan.scans():
        groups[f"{node.label}|{info.identity}"].append(node)
    out = []
    for key, nodes in groups.items():
        if len(nodes) > 1:
            ident = key.split("|", 1)[1]
            out.append(
                Finding(
                    "PL005",
                    "info",
                    f"{ident} is scanned {len(nodes)} times. If the branches share work, consider .cache()/.persist() "
                    "or restructuring (e.g. one pass with conditional aggregation).",
                    nodes[1],
                )
            )
    return out


@rule("PL006")
def single_partition(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """Exchange SinglePartition: all data goes through one task."""
    return [
        Finding(
            "PL006",
            "warning",
            "Exchange SinglePartition - every row is moved into ONE task (e.g. Window without partitionBy, "
            "global sort/limit). This does not scale.",
            n,
        )
        for n in plan.find("Exchange")
        if n.detail.startswith("SinglePartition")
    ]


@rule("PL007")
def too_many_shuffles(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """More shuffles than ``max_shuffles`` (default 6)."""
    limit = int(cfg.get("max_shuffles", 6))
    shuffles = plan.find("Exchange")
    if len(shuffles) > limit:
        return [
            Finding(
                "PL007",
                "info",
                f"{len(shuffles)} shuffles (threshold {limit}). Check for repeated repartition/groupBy on the same keys.",
                shuffles[0],
            )
        ]
    return []


@rule("PL008")
def python_to_jvm_roundtrips(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """Many separate Python UDF operators (each one is a JVM<->Python round trip)."""
    limit = int(cfg.get("max_python_operators", 2))
    from .plan import PYTHON_OPERATORS

    found = [n for n in plan.nodes() if n.name in PYTHON_OPERATORS]
    if len(found) > limit:
        return [
            Finding(
                "PL008",
                "info",
                f"{len(found)} Python UDF operators - each serialises rows to Python and back. "
                "Combining UDFs or using built-ins avoids repeated round trips.",
                found[0],
            )
        ]
    return []


@rule("PL009")
def sort_merge_with_local_side(plan: Plan, cfg: Dict[str, Any]) -> List[Finding]:
    """SortMergeJoin where one input is an in-memory LocalTableScan (could be broadcast)."""
    out = []
    for node in plan.find("SortMergeJoin"):
        for child in node.children:
            leaves = [n for n in child.walk() if not n.children]
            if leaves and all(leaf.name == "LocalTableScan" for leaf in leaves):
                out.append(
                    Finding(
                        "PL009",
                        "warning",
                        "SortMergeJoin with a side built from local data - a broadcast() hint would avoid two shuffles.",
                        node,
                    )
                )
                break
    return out


def lint(
    obj: Any, *, select: Optional[List[str]] = None, ignore: Optional[List[str]] = None, **config: Any
) -> List[Finding]:
    """Run lint rules on a DataFrame / explain text / :class:`Plan`.

    :param select: only run these rule codes.
    :param ignore: skip these rule codes.
    :param config: rule options, e.g. ``max_shuffles=4``.
    """
    plan = Plan.of(obj)
    findings: List[Finding] = []
    for code, fn in RULES.items():
        if select and code not in select:
            continue
        if ignore and code in ignore:
            continue
        findings.extend(fn(plan, config))
    findings.sort(key=lambda f: (-SEVERITY_ORDER[f.severity], f.node.line_no if f.node else 0, f.code))
    return findings
