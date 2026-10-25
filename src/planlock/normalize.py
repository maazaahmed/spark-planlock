"""Make plan text stable across runs, machines and clusters.

Raw ``explain()`` output contains values that change on every run even when
the query is identical: expression IDs (``id#123L``), ``plan_id`` counters,
temp-directory paths, the number of local cores (``splits=8``), codegen stage
numbers and AQE stage ids. Snapshots must ignore all of these, otherwise they
are useless in CI.
"""

from __future__ import annotations

import re
from typing import List, Pattern, Sequence, Tuple, Union

__all__ = ["normalize_plan_text", "DEFAULT_RULES"]

Rule = Tuple[Pattern[str], str]

DEFAULT_RULES: List[Rule] = [
    # expression ids: id#12L, sum(x)#33, subquery#5
    (re.compile(r"#\d+L?\b"), ""),
    # exchange / broadcast identifiers
    (re.compile(r",?\s*\[plan_id=\d+\]"), ""),
    (re.compile(r",?\s*\[id=#?\d+\]"), ""),
    (re.compile(r"\bplan_id=\d+"), "plan_id=N"),
    # codegen stage numbers: "*(3) Project" -> "*(N) Project"
    (re.compile(r"\*\(\d+\)"), "*(N)"),
    # AQE stage numbers
    (re.compile(r"\b(ShuffleQueryStage|BroadcastQueryStage|ResultQueryStage|TableCacheQueryStage) \d+"), r"\1 N"),
    # Range(..., splits=8) depends on local cores
    (re.compile(r"splits=\d+"), "splits=N"),
    # filesystem locations / URIs
    (
        re.compile(r"\b(?:file|hdfs|s3a?|s3n|gs|abfss?|wasbs?|dbfs|oss|viewfs):/{0,3}[^\s,\]\)]*"),
        "<path>",
    ),
    (re.compile(r"(?<=\[)/(?:[^\s,\]\)]+/)*[^\s,\]\)]*(?=[\],])"), "<path>"),
    # "InMemoryFileIndex(2 paths)" -> count is environment dependent
    (re.compile(r"\((\d+) paths\)"), "(N paths)"),
    # pandas/arrow UDF result names: pythonUDF0#12 handled above; numbered ones too
    (re.compile(r"\bpythonUDF\d+\b"), "pythonUDF"),
    # Object ids like "obj#5" handled above; RDD ids "MapPartitionsRDD[12]"
    (re.compile(r"RDD\[\d+\]"), "RDD[N]"),
    # Scala object hash codes, e.g. org.apache...Foo@1a2b3c
    (re.compile(r"@[0-9a-f]{5,}\b"), "@HASH"),
]


def normalize_plan_text(
    text: str,
    extra_rules: Sequence[Union[Rule, Tuple[str, str]]] = (),
) -> str:
    """Return ``text`` with run-specific noise removed.

    ``extra_rules`` lets you add ``(pattern, replacement)`` pairs for things
    specific to your environment (for example a bucket name).
    """
    rules: List[Rule] = list(DEFAULT_RULES)
    for pat, repl in extra_rules:
        rules.append((re.compile(pat) if isinstance(pat, str) else pat, repl))
    out_lines = []
    for line in text.splitlines():
        for pat, repl in rules:
            line = pat.sub(repl, line)
        out_lines.append(line.rstrip())
    return "\n".join(ln for ln in out_lines if ln.strip())
