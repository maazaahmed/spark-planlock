"""Command line interface. Works on plan *text*, so no Spark is required.

planlock lint plan.txt            # paste from Spark UI / df.explain()
planlock summary plan.txt --json
planlock diff before.txt after.txt
planlock tree plan.txt
planlock normalize plan.txt
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from . import __version__
from .diff import format_changes, semantic_diff, text_diff
from .lint import SEVERITY_ORDER, lint
from .plan import Plan


def _read(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _cmd_lint(args: argparse.Namespace) -> int:
    plan = Plan.from_text(_read(args.file))
    config = {}
    if args.max_shuffles is not None:
        config["max_shuffles"] = args.max_shuffles
    findings = lint(plan, select=args.select, ignore=args.ignore, **config)
    if args.json:
        print(json.dumps([f.to_dict() for f in findings], indent=2))
    elif findings:
        for f in findings:
            print(f)
    else:
        print("No findings.")
    floor = SEVERITY_ORDER[args.fail_on]
    return 1 if any(SEVERITY_ORDER[f.severity] >= floor for f in findings) else 0


def _cmd_summary(args: argparse.Namespace) -> int:
    plan = Plan.from_text(_read(args.file))
    s = plan.summary
    if args.json:
        print(json.dumps(s.to_dict(), indent=2))
        return 0
    print(f"root:            {plan.root.name}{' (final AQE plan)' if plan.is_final else ''}")
    print(f"shuffles:        {s.shuffles} {s.shuffle_partitionings if s.shuffle_partitionings else ''}")
    print(f"broadcasts:      {s.broadcasts}")
    print(f"reused exchange: {s.reused_exchanges}")
    print(f"joins:           {s.joins or 'none'}")
    print(f"sorts:           {s.sorts}")
    print(f"aggregates:      {s.aggregates}")
    print(f"windows:         {s.windows}")
    print(f"python udfs:     {s.python_operators or 'none'}")
    print(f"scans:           {len(s.scans)}")
    for scan in s.scans:
        print(f"  - {scan['operator']} {scan['relation']}")
        print(f"      pushed filters:    {scan['pushed_filters'] or '[]'}")
        print(f"      partition filters: {scan['partition_filters'] or '[]'}")
        print(f"      columns read:      {scan['read_columns']}")
    return 0


def _cmd_diff(args: argparse.Namespace) -> int:
    old = Plan.from_text(_read(args.old))
    new = Plan.from_text(_read(args.new))
    changes = semantic_diff(old, new)
    print(format_changes(changes))
    d = text_diff(old.skeleton(), new.skeleton(), args.old, args.new)
    if d:
        print("\nOperator diff:\n" + d)
    if args.fail_on == "any":
        return 1 if changes or d else 0
    return 1 if any(c.severity == "regression" for c in changes) else 0


def _cmd_tree(args: argparse.Namespace) -> int:
    print(Plan.from_text(_read(args.file)).skeleton())
    return 0


def _cmd_normalize(args: argparse.Namespace) -> int:
    print(Plan.from_text(_read(args.file)).normalized_text())
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="planlock", description="Analyse, lint and diff Spark physical plans.")
    p.add_argument("--version", action="version", version=f"planlock {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    lp = sub.add_parser("lint", help="flag expensive patterns in a plan")
    lp.add_argument("file", help="file with explain() output, or - for stdin")
    lp.add_argument("--select", nargs="*", help="only these rule codes")
    lp.add_argument("--ignore", nargs="*", help="skip these rule codes")
    lp.add_argument("--max-shuffles", type=int, default=None)
    lp.add_argument("--fail-on", choices=list(SEVERITY_ORDER), default="warning", help="exit 1 at/above this severity")
    lp.add_argument("--json", action="store_true")
    lp.set_defaults(func=_cmd_lint)

    sp = sub.add_parser("summary", help="cost summary of a plan")
    sp.add_argument("file")
    sp.add_argument("--json", action="store_true")
    sp.set_defaults(func=_cmd_summary)

    dp = sub.add_parser("diff", help="semantic diff between two plans")
    dp.add_argument("old")
    dp.add_argument("new")
    dp.add_argument("--fail-on", choices=["any", "regression", "never"], default="regression")
    dp.set_defaults(func=_cmd_diff)

    tp = sub.add_parser("tree", help="print the operator tree only")
    tp.add_argument("file")
    tp.set_defaults(func=_cmd_tree)

    np_ = sub.add_parser("normalize", help="print the plan with run-specific ids/paths removed")
    np_.add_argument("file")
    np_.set_defaults(func=_cmd_normalize)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except (OSError, ValueError) as exc:
        print(f"planlock: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
