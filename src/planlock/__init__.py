"""planlock - a lockfile for Spark query plans.

Assert on, snapshot, lint and diff PySpark physical plans so that performance
regressions (extra shuffles, lost broadcast joins, lost predicate pushdown,
sneaky Python UDFs) fail in CI instead of on the cluster bill.

    import planlock
    from planlock import expect

    expect(df).max_shuffles(1).uses_broadcast_join().pushes_filter("event_date")
    planlock.check(df, "daily_revenue")
"""

__version__ = "0.1.0"

from .diff import Change, format_changes, semantic_diff
from .expect import PlanAssertionError, PlanExpectation, expect
from .lint import Finding, lint
from .normalize import normalize_plan_text
from .parser import PlanNode, ScanInfo, parse_plan
from .plan import Plan, PlanSummary
from .snapshot import CheckResult, PlanLock, PlanLockMismatch, PlanLockMissing, check

__all__ = [
    "__version__",
    "Plan",
    "PlanSummary",
    "PlanNode",
    "ScanInfo",
    "parse_plan",
    "normalize_plan_text",
    "expect",
    "PlanExpectation",
    "PlanAssertionError",
    "check",
    "PlanLock",
    "PlanLockMismatch",
    "PlanLockMissing",
    "CheckResult",
    "lint",
    "Finding",
    "semantic_diff",
    "format_changes",
    "Change",
]
