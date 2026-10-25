from planlock import Plan, parse_plan
from planlock.parser import extract_physical_section, parse_fields, split_top_level


def test_parses_tree_depths_and_children(aqe_final_text):
    root = parse_plan(aqe_final_text)
    assert root.name == "AdaptiveSparkPlan"
    headers = [c.name for c in root.children]
    assert headers == ["== Final Plan ==", "== Initial Plan =="]
    final = root.children[0]
    assert final.children[0].name == "ResultQueryStage"
    bhj = next(n for n in root.walk() if n.name == "BroadcastHashJoin")
    assert [c.name for c in bhj.children] == ["Filter", "BroadcastQueryStage"]
    assert bhj.codegen_stage == 2
    assert bhj.parent.name == "Project"


def test_effective_nodes_skip_initial_aqe_branch(aqe_final_text):
    plan = Plan.from_text(aqe_final_text)
    assert plan.is_adaptive and plan.is_final
    names = [n.name for n in plan.nodes()]
    assert "SortMergeJoin" not in names  # only in the Initial Plan
    assert "SortMergeJoin" in [n.name for n in plan.all_nodes()]
    assert plan.summary.shuffles == 1
    assert plan.summary.joins == {"BroadcastHashJoin": 1}


def test_scan_info(aqe_final_text):
    plan = Plan.from_text(aqe_final_text)
    infos = {i.table: i for _, i in plan.scans()}
    events = infos["spark_catalog.default.events"]
    assert events.format == "parquet"
    assert events.columns == ["user_id", "amount", "dt"]
    assert events.pushed_filters == ["IsNotNull(user_id)"]
    assert events.partition_filters == ["isnotnull(dt)", "(dt = 2024-01-01)"]
    assert events.read_schema == ["user_id", "amount"]
    assert events.location == "file:/tmp/wh/events"


def test_extended_explain_uses_physical_section(smj_udf_window_text):
    body = extract_physical_section(smj_udf_window_text)
    assert body.startswith("AdaptiveSparkPlan")
    plan = Plan.from_text(smj_udf_window_text)
    s = plan.summary
    assert s.shuffles == 3
    assert s.single_partition_exchanges == 1
    assert s.joins == {"SortMergeJoin": 1}
    assert s.python_operators == {"BatchEvalPython": 1}
    assert s.windows == 1
    scan = plan.scans()[0][1]
    assert scan.table is None
    assert scan.location == "s3a://bucket/raw/events"


def test_formatted_mode_tree():
    text = """== Physical Plan ==
AdaptiveSparkPlan (6)
+- HashAggregate (5)
   +- Exchange (4)
      +- * HashAggregate (3)
         +- * Project (2)
            +- * Range (1)

(1) Range [codegen id : 1]
Output [1]: [id#0L]
"""
    plan = Plan.from_text(text)
    assert [n.name for n in plan.nodes()] == [
        "AdaptiveSparkPlan",
        "HashAggregate",
        "Exchange",
        "HashAggregate",
        "Project",
        "Range",
    ]
    assert plan.summary.shuffles == 1


def test_split_and_fields():
    assert split_top_level("a, f(b, c), [d, e]") == ["a", "f(b, c)", "[d, e]"]
    head, fields = parse_fields(
        "parquet [a#1] Batched: true, PushedFilters: [EqualTo(a,1), In(b, [x,y])], ReadSchema: struct<a:int>"
    )
    assert head == "parquet [a#1]"
    assert fields["Batched"] == "true"
    assert fields["PushedFilters"] == "[EqualTo(a,1), In(b, [x,y])]"
    assert fields["ReadSchema"] == "struct<a:int>"


def test_bare_plan_without_header():
    plan = Plan.from_text("*(1) Project [id#0L]\n+- *(1) Range (0, 10, step=1, splits=2)")
    assert plan.root.name == "Project"
    assert plan.root.children[0].name == "Range"


def test_empty_text_raises():
    import pytest

    with pytest.raises(ValueError):
        Plan.from_text("   \n")
