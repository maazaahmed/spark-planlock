"""Lock files for query plans ("snapshot testing" for Spark plans).

The first time ``check(df, "name")`` runs it writes ``__planlocks__/name.plan``
containing the *normalized* physical plan. On later runs the current plan is
compared to the locked one and, if it changed in a cost-relevant way, the
check fails with a semantic explanation such as::

    ✗ shuffle(s): 1 → 3 (+2)
    ✗ join strategy changed: BroadcastHashJoin 1→0, SortMergeJoin 0→1

Commit the ``.plan`` files. Re-lock intentionally with ``PLANLOCK_UPDATE=1``
(or ``pytest --planlock-update``) and review the diff in your pull request.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple, Union

from .diff import Change, format_changes, semantic_diff, text_diff
from .plan import Plan

__all__ = ["PlanLock", "PlanLockMismatch", "PlanLockMissing", "CheckResult", "check", "MODES"]

MODES = ("summary", "structure", "exact")
HEADER = "# planlock v1 - generated file. Re-lock with PLANLOCK_UPDATE=1 or `pytest --planlock-update`."
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on", "all"}


def _spark_version() -> Optional[str]:
    try:
        import pyspark  # type: ignore

        return str(pyspark.__version__)
    except Exception:
        return None


class PlanLockMismatch(AssertionError):
    """The current plan differs from the locked one."""

    def __init__(self, message: str, changes: Sequence[Change] = ()) -> None:
        self.changes = list(changes)
        super().__init__(message)


class PlanLockMissing(AssertionError):
    """No lock file exists and we are running in CI (so it was not created)."""


@dataclass
class CheckResult:
    name: str
    path: Path
    status: str  # "created" | "updated" | "matched" | "changed-allowed"
    changes: List[Change] = field(default_factory=list)


@dataclass
class _LockFile:
    name: str
    spark_version: Optional[str]
    plan_text: str

    @classmethod
    def read(cls, path: Path) -> _LockFile:
        name, version, body = path.stem, None, []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("#"):
                m = re.match(r"#\s*(\w+):\s*(.*)$", line)
                if m and m.group(1) == "name":
                    name = m.group(2).strip()
                elif m and m.group(1) == "spark_version":
                    version = m.group(2).strip() or None
                continue
            body.append(line)
        return cls(name=name, spark_version=version, plan_text="\n".join(body).strip("\n"))

    def render(self, plan: Plan) -> str:
        s = plan.summary
        joins = ", ".join(f"{k}={v}" for k, v in s.joins.items()) or "none"
        py = ", ".join(f"{k}={v}" for k, v in s.python_operators.items()) or "none"
        meta = [
            HEADER,
            f"# name: {self.name}",
            f"# spark_version: {self.spark_version or ''}",
            f"# shuffles: {s.shuffles}  broadcasts: {s.broadcasts}  joins: {joins}  python_udfs: {py}  scans: {len(s.scans)}",
        ]
        return "\n".join(meta) + "\n" + self.plan_text.rstrip("\n") + "\n"


class PlanLock:
    """A directory of plan lock files.

    :param directory: where ``.plan`` files live (default ``$PLANLOCK_DIR`` or
        ``./__planlocks__``).
    :param mode: ``"structure"`` (default) compares the cost summary and the
        operator tree; ``"summary"`` compares only the cost summary (most
        tolerant across Spark versions); ``"exact"`` compares normalized text.
    :param fail_on: ``"any"`` fails on every difference; ``"regression"``
        only fails when at least one change makes the plan more expensive.
    :param update: overwrite lock files instead of comparing. Defaults to the
        ``PLANLOCK_UPDATE`` environment variable.
    :param ci: if true, a missing lock file is an error instead of being
        created. Defaults to the ``CI`` environment variable.
    :param extra_rules: additional ``(regex, replacement)`` normalization rules.
    """

    def __init__(
        self,
        directory: Union[str, Path, None] = None,
        *,
        mode: str = "structure",
        fail_on: str = "any",
        update: Optional[bool] = None,
        ci: Optional[bool] = None,
        extra_rules: Sequence[Tuple[str, str]] = (),
    ) -> None:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
        if fail_on not in ("any", "regression"):
            raise ValueError("fail_on must be 'any' or 'regression'")
        self.directory = Path(directory or os.environ.get("PLANLOCK_DIR") or "__planlocks__")
        self.mode = mode
        self.fail_on = fail_on
        self.update = _truthy(os.environ.get("PLANLOCK_UPDATE")) if update is None else update
        self.ci = _truthy(os.environ.get("CI")) if ci is None else ci
        self.extra_rules = list(extra_rules)

    def path_for(self, name: str) -> Path:
        safe = _SAFE_NAME.sub("_", name).strip("_") or "plan"
        return self.directory / f"{safe}.plan"

    def check(
        self,
        obj: Any,
        name: str,
        *,
        executed: bool = False,
        mode: Optional[str] = None,
        fail_on: Optional[str] = None,
    ) -> CheckResult:
        """Compare ``obj``'s plan with the lock ``name`` (creating it if needed)."""
        mode = mode or self.mode
        fail_on = fail_on or self.fail_on
        plan = Plan.of(obj, executed=executed)
        normalized = plan.normalized_text(self.extra_rules)
        path = self.path_for(name)
        version = _spark_version()
        new_lock = _LockFile(name=name, spark_version=version, plan_text=normalized)

        if not path.exists():
            if self.ci and not self.update:
                raise PlanLockMissing(
                    f"No plan lock {path} for {name!r} and running in CI. "
                    "Generate it locally (run the tests once, or with PLANLOCK_UPDATE=1) and commit it."
                )
            self._write(path, new_lock)
            return CheckResult(name, path, "created")

        old_lock = _LockFile.read(path)
        old_plan = Plan.from_text(old_lock.plan_text)
        current_plan = Plan.from_text(normalized)
        changes = semantic_diff(old_plan, current_plan)

        if mode == "summary":
            same = old_plan.summary.to_dict() == current_plan.summary.to_dict()
        elif mode == "structure":
            same = (
                old_plan.summary.to_dict() == current_plan.summary.to_dict()
                and old_plan.skeleton() == current_plan.skeleton()
            )
        else:
            same = old_lock.plan_text.strip() == normalized.strip()

        if same:
            return CheckResult(name, path, "matched", changes)

        if self.update:
            self._write(path, new_lock)
            return CheckResult(name, path, "updated", changes)

        regressions = [c for c in changes if c.severity == "regression"]
        if fail_on == "regression" and not regressions:
            return CheckResult(name, path, "changed-allowed", changes)

        raise PlanLockMismatch(
            self._message(name, path, old_lock, new_lock, changes, old_plan, current_plan, mode), changes
        )

    # ---------------------------------------------------------------- utils
    def _write(self, path: Path, lock: _LockFile) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(lock.render(Plan.from_text(lock.plan_text)), encoding="utf-8")

    @staticmethod
    def _message(
        name: str,
        path: Path,
        old: _LockFile,
        new: _LockFile,
        changes: List[Change],
        old_plan: Plan,
        new_plan: Plan,
        mode: str,
    ) -> str:
        parts = [f"Query plan for {name!r} changed (lock: {path}, mode={mode}).", ""]
        parts.append(format_changes(changes))
        if (
            old.spark_version
            and new.spark_version
            and old.spark_version.split(".")[:2] != new.spark_version.split(".")[:2]
        ):
            parts += [
                "",
                f"note: locked with Spark {old.spark_version}, running Spark {new.spark_version}; "
                "plan changes across Spark versions are expected. Consider mode='summary'.",
            ]
        if mode == "exact":
            diff = text_diff(old.plan_text, new.plan_text)
        else:
            diff = text_diff(old_plan.skeleton(), new_plan.skeleton())
        if diff:
            parts += ["", "Operator diff:" if mode != "exact" else "Plan diff:", diff]
        parts += [
            "",
            "If this change is intended, re-lock with PLANLOCK_UPDATE=1 (or pytest --planlock-update) and commit the .plan file.",
        ]
        return "\n".join(parts)


def check(
    obj: Any,
    name: str,
    *,
    directory: Union[str, Path, None] = None,
    executed: bool = False,
    mode: Optional[str] = None,
    fail_on: Optional[str] = None,
) -> CheckResult:
    """Module-level convenience wrapper around :meth:`PlanLock.check`."""
    lock = PlanLock(directory)
    return lock.check(obj, name, executed=executed, mode=mode, fail_on=fail_on)
