"""pytest integration (auto-registered through the ``pytest11`` entry point).

Provides:

* ``--planlock-update`` - rewrite lock files instead of comparing.
* ``--planlock-mode`` - default compare mode (summary / structure / exact).
* ``--planlock-fail-on`` - ``any`` (default) or ``regression``.
* the ``planlock`` fixture::

      def test_daily_revenue(spark, planlock):
          df = build_daily_revenue(spark)
          planlock.check(df)                      # lock named after the test
          planlock.check(df.limit(10), "preview")  # extra named locks
          planlock.expect(df).max_shuffles(2)

Lock files are stored next to the test file in ``__planlocks__/``.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, List, Optional

import pytest

from .expect import PlanExpectation, expect
from .snapshot import CheckResult, PlanLock


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("planlock", "Spark query plan locks")
    group.addoption(
        "--planlock-update",
        action="store_true",
        default=False,
        help="Rewrite Spark plan lock files instead of comparing against them.",
    )
    group.addoption(
        "--planlock-mode",
        default=None,
        choices=["summary", "structure", "exact"],
        help="How strictly plans are compared (default: structure).",
    )
    group.addoption(
        "--planlock-fail-on",
        default=None,
        choices=["any", "regression"],
        help="Fail on any plan change (default) or only on regressions.",
    )
    parser.addini("planlock_mode", "Default planlock compare mode", default="structure")
    parser.addini("planlock_fail_on", "Default planlock fail_on policy", default="any")
    parser.addini("planlock_dir", "Lock directory name (relative to each test file)", default="__planlocks__")


class PlanLockFixture:
    """Object behind the ``planlock`` fixture."""

    def __init__(self, request: Any) -> None:
        config = request.config
        test_path = Path(str(request.node.fspath))
        dirname = config.getini("planlock_dir") or "__planlocks__"
        mode = config.getoption("--planlock-mode") or config.getini("planlock_mode") or "structure"
        fail_on = config.getoption("--planlock-fail-on") or config.getini("planlock_fail_on") or "any"
        update = True if config.getoption("--planlock-update") else None
        self._lock = PlanLock(test_path.parent / dirname, mode=mode, fail_on=fail_on, update=update)
        self._base = f"{test_path.stem}__{_clean(request.node.name)}"
        self._counter = 0
        self.results: List[CheckResult] = []

    @property
    def lock(self) -> PlanLock:
        return self._lock

    def check(
        self,
        df: Any,
        name: Optional[str] = None,
        *,
        executed: bool = False,
        mode: Optional[str] = None,
        fail_on: Optional[str] = None,
    ) -> CheckResult:
        """Lock ``df``'s plan. ``name`` defaults to the test name (+ a counter)."""
        if name is None:
            name = self._base if self._counter == 0 else f"{self._base}_{self._counter}"
            self._counter += 1
        else:
            name = f"{self._base}__{_clean(name)}"
        result = self._lock.check(df, name, executed=executed, mode=mode, fail_on=fail_on)
        self.results.append(result)
        return result

    def expect(self, df: Any, *, executed: bool = False) -> PlanExpectation:
        return expect(df, executed=executed)


def _clean(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_")


@pytest.fixture
def planlock(request: Any) -> PlanLockFixture:
    """Lock and assert on Spark query plans. See :class:`PlanLockFixture`."""
    return PlanLockFixture(request)


_RESULTS_KEY = "_planlock_results"


def pytest_runtest_teardown(item: Any) -> None:
    fixture = getattr(item, "funcargs", {}).get("planlock")
    if isinstance(fixture, PlanLockFixture):
        store = item.config.__dict__.setdefault(_RESULTS_KEY, [])
        store.extend(fixture.results)


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: Any) -> None:
    results = config.__dict__.get(_RESULTS_KEY, [])
    if not results:
        return
    counts: dict = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    text = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    terminalreporter.write_sep("-", "planlock")
    terminalreporter.write_line(f"plan locks: {text}")
    for r in results:
        if r.status in ("created", "updated"):
            terminalreporter.write_line(f"  {r.status}: {os.path.relpath(r.path)}")
