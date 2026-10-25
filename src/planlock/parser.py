"""Parse Spark physical-plan text (the output of ``df.explain()``) into a tree.

The parser works on *text*, so it does not need a running Spark session. That
means you can feed it plans copied from the Spark UI, from logs, or from a
Databricks notebook, and it is fully unit-testable.

Spark renders plans with ``TreeNode.generateTreeString``. Every nesting level
adds exactly three characters of prefix: ``":  "`` or ``"   "`` for ancestors and
``":- "`` / ``"+- "`` for the node itself. We rely on that to recover depth.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Tuple

__all__ = [
    "PlanNode",
    "ScanInfo",
    "parse_plan",
    "extract_physical_section",
    "split_top_level",
    "parse_fields",
]

# ``*(3) HashAggregate(...)`` -> whole-stage-codegen marker
_CODEGEN_RE = re.compile(r"^\*\((\d+|N)\)\s+")
# The tree prefix: groups of three chars, then an optional ``+- `` / ``:- `` marker.
_PREFIX_RE = re.compile(r"^((?:[: ] {2})*)([:+]- )?")
_NAME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)")
_SECTION_RE = re.compile(r"^== (.+?) ==\s*$")
_AQE_HEADER_RE = re.compile(r"^== (Final|Initial|Current) Plan ==$")
_FIELD_KEY_RE = re.compile(r"([A-Z][A-Za-z]*): ")

_OPEN = "([{<"
_CLOSE = ")]}>"

# Operators whose first word is followed by a "format" word that is part of the identity.
_SCAN_LIKE = {"FileScan", "Scan", "BatchScan", "PhotonScan"}


@dataclass
class ScanInfo:
    """Details pulled out of a scan node (FileScan / BatchScan / Scan ...)."""

    format: str = ""
    table: Optional[str] = None
    location: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    pushed_filters: List[str] = field(default_factory=list)
    partition_filters: List[str] = field(default_factory=list)
    data_filters: List[str] = field(default_factory=list)
    read_schema: List[str] = field(default_factory=list)
    fields: Dict[str, str] = field(default_factory=dict)

    @property
    def identity(self) -> str:
        """A stable identifier for *what* is being read.

        The catalog table name when there is one, otherwise the last component
        of the path (so ``s3a://bucket/raw/events`` and ``file:/tmp/x/events``
        both become ``events`` and locks don't depend on temp dirs or buckets).
        """
        if self.table:
            return self.table
        if self.location:
            first = self.location.split(",")[0].strip().rstrip("/")
            last = re.split(r"[/:]", first)[-1]
            return last or first
        return self.format or "<unknown>"


@dataclass
class PlanNode:
    """One operator in a physical plan."""

    name: str
    detail: str
    raw: str
    depth: int
    codegen_stage: Optional[int] = None
    children: List[PlanNode] = field(default_factory=list)
    parent: Optional[PlanNode] = field(default=None, repr=False, compare=False)
    line_no: int = 0

    # ------------------------------------------------------------------ tree
    def walk(self) -> Iterator[PlanNode]:
        """Pre-order traversal including this node."""
        yield self
        for child in self.children:
            yield from child.walk()

    @property
    def is_section_header(self) -> bool:
        return self.name.startswith("==")

    @property
    def label(self) -> str:
        """Operator name, plus the format for scans (``FileScan parquet``)."""
        if self.name in _SCAN_LIKE or self.name == "Execute":
            first = self.detail.split(" ", 1)[0] if self.detail else ""
            first = first.split("[", 1)[0]
            return f"{self.name} {first}".strip()
        return self.name

    # ----------------------------------------------------------------- scans
    @property
    def is_scan(self) -> bool:
        return self.name in _SCAN_LIKE or self.name in {
            "InMemoryTableScan",
            "LocalTableScan",
            "RowDataSourceScan",
            "HiveTableScan",
            "Range",
        }

    def scan_info(self) -> Optional[ScanInfo]:
        """Structured details for file/datasource scans, otherwise ``None``."""
        if self.name not in _SCAN_LIKE and self.name not in {"RowDataSourceScan", "HiveTableScan"}:
            return None
        head, fields = parse_fields(self.detail)
        info = ScanInfo(fields=fields)
        head = head.strip()
        parts = head.split(" ", 1)
        info.format = parts[0].split("[", 1)[0] if parts else ""
        rest = parts[1].strip() if len(parts) > 1 else ""
        # ``spark_catalog.db.t[a#1,b#2]`` or ``[a#1,b#2]`` or ``file:/x[a#1]``
        bracket = rest.find("[")
        target = rest[:bracket].strip() if bracket >= 0 else rest.split(" ", 1)[0]
        if bracket >= 0:
            end = _matching_close(rest, bracket)
            cols = rest[bracket + 1 : end] if end > bracket else ""
            info.columns = [_strip_expr_id(c) for c in split_top_level(cols, ",") if c]
        if target:
            if "://" in target or target.startswith(("file:", "dbfs:", "/")):
                info.location = target
            else:
                info.table = target
        info.pushed_filters = _as_list(fields.get("PushedFilters"))
        info.partition_filters = [_strip_expr_id(f) for f in _as_list(fields.get("PartitionFilters"))]
        info.data_filters = [_strip_expr_id(f) for f in _as_list(fields.get("DataFilters"))]
        if "Location" in fields and not info.location:
            loc = fields["Location"]
            m = re.search(r"\[(.*)\]\s*$", loc)
            info.location = m.group(1) if m else loc
        schema = fields.get("ReadSchema", "")
        info.read_schema = _struct_field_names(schema)
        return info

    def __str__(self) -> str:  # pragma: no cover - debugging helper
        return self.raw.strip()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _strip_expr_id(text: str) -> str:
    return re.sub(r"#\d+L?", "", text).strip()


def _matching_close(text: str, open_idx: int) -> int:
    depth = 0
    for i in range(open_idx, len(text)):
        ch = text[i]
        if ch in _OPEN:
            depth += 1
        elif ch in _CLOSE:
            depth -= 1
            if depth == 0:
                return i
    return len(text)


def split_top_level(text: str, sep: str = ",") -> List[str]:
    """Split ``text`` on ``sep`` but only outside of brackets."""
    out: List[str] = []
    depth = 0
    buf: List[str] = []
    for ch in text:
        if ch in _OPEN:
            depth += 1
        elif ch in _CLOSE:
            depth = max(0, depth - 1)
        if ch == sep and depth == 0:
            out.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _depth_map(text: str) -> List[int]:
    depths = []
    depth = 0
    for ch in text:
        if ch in _OPEN:
            depths.append(depth)
            depth += 1
        elif ch in _CLOSE:
            depth = max(0, depth - 1)
            depths.append(depth)
        else:
            depths.append(depth)
    return depths


def parse_fields(detail: str) -> Tuple[str, Dict[str, str]]:
    """Parse ``head Key: value, Key2: [a, b]`` into ``(head, {Key: value})``.

    Only keys at bracket depth 0 are considered, so values may themselves
    contain ``": "``.
    """
    depths = _depth_map(detail)
    keys = []
    for m in _FIELD_KEY_RE.finditer(detail):
        start = m.start()
        if depths[start] != 0:
            continue
        if start > 0 and detail[start - 1] not in " ,":
            continue
        keys.append(m)
    if not keys:
        return detail, {}
    head = detail[: keys[0].start()].rstrip(" ,")
    fields: Dict[str, str] = {}
    for i, m in enumerate(keys):
        end = keys[i + 1].start() if i + 1 < len(keys) else len(detail)
        value = detail[m.end() : end].strip().rstrip(",").strip()
        fields[m.group(1)] = value
    return head, fields


def _as_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    value = value.strip()
    if value.startswith("[") and value.endswith("]"):
        value = value[1:-1]
    return [v for v in split_top_level(value, ",") if v]


def _struct_field_names(schema: str) -> List[str]:
    schema = schema.strip()
    if not schema.startswith("struct<") or not schema.endswith(">"):
        return []
    inner = schema[len("struct<") : -1]
    names = []
    for part in split_top_level(inner, ","):
        name = part.split(":", 1)[0].strip().strip("`")
        if name:
            names.append(name)
    return names


def extract_physical_section(text: str) -> str:
    """Return only the ``== Physical Plan ==`` section of explain output.

    If no section headers exist the text is assumed to already be a physical
    plan and is returned unchanged.
    """
    lines = text.splitlines()
    start = None
    for i, line in enumerate(lines):
        m = _SECTION_RE.match(line.strip())
        if m and m.group(1).strip().lower() == "physical plan" and not line.startswith((" ", "+", ":")):
            start = i + 1
            break
    if start is None:
        return text.strip("\n")
    body = []
    for line in lines[start:]:
        m = _SECTION_RE.match(line.strip())
        # A new top-level section (not an AQE "+- == Final Plan ==" marker) ends the plan.
        if m and not line.startswith((" ", "+", ":")) and not _AQE_HEADER_RE.match(line.strip()):
            break
        body.append(line)
    # The "formatted" explain mode appends node details after a blank line.
    out: List[str] = []
    for line in body:
        if not line.strip() and out:
            break
        if line.strip():
            out.append(line)
    return "\n".join(out)


def parse_plan(text: str) -> PlanNode:
    """Parse explain output into a :class:`PlanNode` tree and return the root.

    Accepts the output of ``explain()``, ``explain(True)`` / ``"extended"``
    (only the physical section is used) or a bare physical plan.
    """
    body = extract_physical_section(text)
    lines = [ln.rstrip() for ln in body.splitlines() if ln.strip()]
    if not lines:
        raise ValueError("No physical plan found in the given text")

    root: Optional[PlanNode] = None
    # stack[d] = last node seen at depth d
    stack: Dict[int, PlanNode] = {}
    for line_no, line in enumerate(lines, start=1):
        m = _PREFIX_RE.match(line)
        assert m is not None
        indent, marker = m.group(1), m.group(2)
        prefix_len = len(indent) + (len(marker) if marker else 0)
        content = line[prefix_len:]
        depth = prefix_len // 3

        codegen = None
        cg = _CODEGEN_RE.match(content)
        if cg:
            codegen = int(cg.group(1)) if cg.group(1).isdigit() else 0
            content = content[cg.end() :]
        elif content.startswith("* "):  # "formatted" explain mode marker
            codegen = 0
            content = content[2:]

        header = _AQE_HEADER_RE.match(content.strip())
        if header:
            name, detail = content.strip(), ""
        else:
            nm = _NAME_RE.match(content)
            name = nm.group(1) if nm else content.split(" ", 1)[0]
            detail = content[len(name) :].strip()

        node = PlanNode(
            name=name,
            detail=detail,
            raw=line,
            depth=depth,
            codegen_stage=codegen,
            line_no=line_no,
        )

        if root is None:
            root = node
            stack = {0: node}
            continue

        if marker is None:
            # A marker-less line directly below an AQE "== Final Plan ==" header
            # is that header's (only) child, rendered at the header's indentation.
            parent = stack.get(depth)
            if parent is None or not parent.is_section_header:
                parent = stack.get(depth - 1) or root
        else:
            parent = stack.get(depth - 1) or root
        node.parent = parent
        parent.children.append(node)
        stack[depth] = node
        # Forget deeper levels: they belong to a finished subtree.
        for d in [d for d in stack if d > depth]:
            del stack[d]

    assert root is not None
    return root
