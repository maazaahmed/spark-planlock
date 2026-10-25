"""Explain *what changed* between two plans in cost terms, not just text."""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import Any, Dict, List, Union

from .plan import Plan, PlanSummary

__all__ = ["Change", "semantic_diff", "text_diff", "format_changes"]

# Which direction is worse for each counter.
_COUNTERS = [
    ("shuffles", "shuffle(s)", "up"),
    ("single_partition_exchanges", "single-partition exchange(s)", "up"),
    ("broadcasts", "broadcast(s)", None),
    ("reused_exchanges", "reused exchange(s)", "down"),
    ("sorts", "sort(s)", "up"),
    ("aggregates", "aggregate operator(s)", None),
    ("windows", "window operator(s)", None),
    ("cached_scans", "cached (InMemory) scan(s)", None),
]

_BAD_JOINS = {
    "CartesianProduct": 3,
    "BroadcastNestedLoopJoin": 2,
    "SortMergeJoin": 1,
    "ShuffledHashJoin": 1,
    "BroadcastHashJoin": 0,
}


@dataclass
class Change:
    kind: str
    message: str
    severity: str  # "regression" | "improvement" | "neutral"

    def __str__(self) -> str:
        icon = {"regression": "✗", "improvement": "✓", "neutral": "•"}[self.severity]
        return f"{icon} {self.message}"


def _as_summary(x: Union[Plan, PlanSummary, Dict[str, Any]]) -> PlanSummary:
    if isinstance(x, Plan):
        return x.summary
    if isinstance(x, PlanSummary):
        return x
    return PlanSummary.from_dict(x)


def _severity(delta: int, worse: Any) -> str:
    if worse is None or delta == 0:
        return "neutral"
    if worse == "up":
        return "regression" if delta > 0 else "improvement"
    return "regression" if delta < 0 else "improvement"


def semantic_diff(
    old: Union[Plan, PlanSummary, Dict[str, Any]], new: Union[Plan, PlanSummary, Dict[str, Any]]
) -> List[Change]:
    """List cost-relevant differences from ``old`` to ``new``."""
    a, b = _as_summary(old), _as_summary(new)
    changes: List[Change] = []

    for attr, label, worse in _COUNTERS:
        x, y = getattr(a, attr), getattr(b, attr)
        if x != y:
            changes.append(Change(attr, f"{label}: {x} → {y} ({y - x:+d})", _severity(y - x, worse)))

    if a.shuffle_partitionings != b.shuffle_partitionings and a.shuffles == b.shuffles:
        changes.append(
            Change(
                "shuffle_partitionings",
                f"shuffle partitioning changed: {a.shuffle_partitionings} → {b.shuffle_partitionings}",
                "neutral",
            )
        )

    # Joins: report per-strategy changes and judge by "badness".
    if a.joins != b.joins:
        keys = sorted(set(a.joins) | set(b.joins))
        parts = []
        score = 0
        for k in keys:
            x, y = a.joins.get(k, 0), b.joins.get(k, 0)
            if x != y:
                parts.append(f"{k} {x}→{y}")
                score += (y - x) * _BAD_JOINS.get(k, 1)
        sev = "regression" if score > 0 else "improvement" if score < 0 else "neutral"
        if sum(b.joins.values()) == sum(a.joins.values()) and score == 0:
            sev = "neutral"
        changes.append(Change("joins", "join strategy changed: " + ", ".join(parts), sev))

    if a.python_operators != b.python_operators:
        x, y = sum(a.python_operators.values()), sum(b.python_operators.values())
        sev = "regression" if y > x else "improvement" if y < x else "neutral"
        changes.append(
            Change(
                "python_operators",
                f"Python UDF operators: {a.python_operators or {}} → {b.python_operators or {}}",
                sev,
            )
        )

    changes.extend(_scan_changes(a, b))
    return changes


def _scan_changes(a: PlanSummary, b: PlanSummary) -> List[Change]:
    out: List[Change] = []

    def by_rel(s: PlanSummary) -> Dict[str, List[Dict[str, Any]]]:
        d: Dict[str, List[Dict[str, Any]]] = {}
        for scan in s.scans:
            d.setdefault(scan["relation"] or scan["operator"], []).append(scan)
        return d

    ra, rb = by_rel(a), by_rel(b)
    for rel in sorted(set(ra) | set(rb)):
        xs, ys = ra.get(rel, []), rb.get(rel, [])
        if len(xs) != len(ys):
            if not xs:
                out.append(Change("scans", f"new scan of {rel}", "neutral"))
            elif not ys:
                out.append(Change("scans", f"scan of {rel} removed", "neutral"))
            else:
                sev = "regression" if len(ys) > len(xs) else "improvement"
                out.append(Change("scans", f"{rel} scanned {len(xs)} → {len(ys)} time(s)", sev))
            continue
        for x, y in zip(xs, ys):
            lost = [f for f in x["pushed_filters"] if f not in y["pushed_filters"]]
            gained = [f for f in y["pushed_filters"] if f not in x["pushed_filters"]]
            if lost:
                out.append(Change("pushdown", f"{rel}: pushed filter(s) lost {lost}", "regression"))
            if gained:
                out.append(Change("pushdown", f"{rel}: new pushed filter(s) {gained}", "improvement"))
            if bool(x["partition_filters"]) != bool(y["partition_filters"]):
                if y["partition_filters"]:
                    out.append(
                        Change(
                            "partition_pruning",
                            f"{rel}: partition pruning enabled {y['partition_filters']}",
                            "improvement",
                        )
                    )
                else:
                    out.append(
                        Change(
                            "partition_pruning",
                            f"{rel}: partition pruning LOST (was {x['partition_filters']})",
                            "regression",
                        )
                    )
            if x["read_columns"] != y["read_columns"]:
                d = y["read_columns"] - x["read_columns"]
                out.append(
                    Change(
                        "columns",
                        f"{rel}: columns read {x['read_columns']} → {y['read_columns']}",
                        "regression" if d > 0 else "improvement",
                    )
                )
    return out


def text_diff(old_text: str, new_text: str, old_label: str = "locked", new_label: str = "current") -> str:
    return "\n".join(
        difflib.unified_diff(old_text.splitlines(), new_text.splitlines(), old_label, new_label, lineterm="")
    )


def format_changes(changes: List[Change]) -> str:
    if not changes:
        return "No cost-relevant changes."
    order = {"regression": 0, "improvement": 1, "neutral": 2}
    return "\n".join(str(c) for c in sorted(changes, key=lambda c: order[c.severity]))
