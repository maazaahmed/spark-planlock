import json

import pytest

from planlock import PlanLock, PlanLockMismatch, PlanLockMissing, check
from planlock.cli import main


def _worse(text):
    return text.replace(
        "BroadcastHashJoin [user_id#1L], [id#10L], Inner, BuildRight, false",
        "SortMergeJoin [user_id#1L], [id#10L], Inner",
    )


def test_create_then_match(tmp_path, aqe_final_text):
    lock = PlanLock(tmp_path, update=False, ci=False)
    r1 = lock.check(aqe_final_text, "daily")
    assert r1.status == "created"
    content = r1.path.read_text()
    assert content.startswith("# planlock v1")
    assert "# shuffles: 1  broadcasts: 1  joins: BroadcastHashJoin=1" in content
    assert "#1L" not in content  # normalized
    # Re-running with different expression ids / plan ids still matches.
    renumbered = aqe_final_text.replace("#1L", "#901L").replace("plan_id=88", "plan_id=5")
    assert lock.check(renumbered, "daily").status == "matched"


def test_mismatch_explains_change(tmp_path, aqe_final_text):
    lock = PlanLock(tmp_path, update=False, ci=False)
    lock.check(aqe_final_text, "daily")
    with pytest.raises(PlanLockMismatch) as exc:
        lock.check(_worse(aqe_final_text), "daily")
    msg = str(exc.value)
    assert "join strategy changed: BroadcastHashJoin 1→0, SortMergeJoin 0→1" in msg
    assert "Operator diff:" in msg
    assert "PLANLOCK_UPDATE=1" in msg
    assert exc.value.changes


def test_update_rewrites(tmp_path, aqe_final_text):
    PlanLock(tmp_path, update=False, ci=False).check(aqe_final_text, "daily")
    r = PlanLock(tmp_path, update=True, ci=False).check(_worse(aqe_final_text), "daily")
    assert r.status == "updated"
    assert PlanLock(tmp_path, update=False, ci=False).check(_worse(aqe_final_text), "daily").status == "matched"


def test_fail_on_regression_allows_improvements(tmp_path, aqe_final_text):
    lock = PlanLock(tmp_path, update=False, ci=False, fail_on="regression")
    lock.check(_worse(aqe_final_text), "daily")
    assert lock.check(aqe_final_text, "daily").status == "changed-allowed"
    lock2 = PlanLock(tmp_path / "b", update=False, ci=False, fail_on="regression")
    lock2.check(aqe_final_text, "daily")
    with pytest.raises(PlanLockMismatch):
        lock2.check(_worse(aqe_final_text), "daily")


def test_ci_refuses_to_create(tmp_path, aqe_final_text):
    with pytest.raises(PlanLockMissing):
        PlanLock(tmp_path, update=False, ci=True).check(aqe_final_text, "new")


def test_modes(tmp_path, aqe_final_text):
    cosmetic = aqe_final_text.replace("functions=[sum(amount#3)]", "functions=[sum(amount#3) AS total]")
    for mode, should_match in [("summary", True), ("structure", True), ("exact", False)]:
        lock = PlanLock(tmp_path / mode, mode=mode, update=False, ci=False)
        lock.check(aqe_final_text, "q")
        if should_match:
            assert lock.check(cosmetic, "q").status == "matched"
        else:
            with pytest.raises(PlanLockMismatch, match="Plan diff:"):
                lock.check(cosmetic, "q")


def test_env_vars(tmp_path, monkeypatch, aqe_final_text):
    monkeypatch.delenv("PLANLOCK_UPDATE", raising=False)
    monkeypatch.setenv("PLANLOCK_DIR", str(tmp_path / "envdir"))
    monkeypatch.setenv("CI", "true")
    with pytest.raises(PlanLockMissing):
        check(aqe_final_text, "x")
    monkeypatch.setenv("PLANLOCK_UPDATE", "1")
    assert check(aqe_final_text, "x").status == "created"
    assert (tmp_path / "envdir" / "x.plan").exists()


def test_bad_options(tmp_path):
    with pytest.raises(ValueError):
        PlanLock(tmp_path, mode="fuzzy")
    with pytest.raises(ValueError):
        PlanLock(tmp_path, fail_on="sometimes")


def test_name_sanitized(tmp_path, aqe_final_text):
    r = PlanLock(tmp_path, ci=False).check(aqe_final_text, "tests/x::y[param 1]")
    assert r.path.name == "tests_x_y_param_1.plan"


# -------------------------------------------------------------------- CLI
@pytest.fixture
def files(tmp_path, aqe_final_text, smj_udf_window_text):
    good = tmp_path / "good.txt"
    bad = tmp_path / "bad.txt"
    good.write_text(aqe_final_text)
    bad.write_text(smj_udf_window_text)
    return good, bad


def test_cli_lint(files, capsys):
    good, bad = files
    assert main(["lint", str(good)]) == 0
    assert "No findings." in capsys.readouterr().out
    assert main(["lint", str(bad)]) == 1
    out = capsys.readouterr().out
    assert "PL006" in out and "PL003" in out
    assert main(["lint", str(bad), "--json", "--fail-on", "error"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert {d["code"] for d in data} >= {"PL003", "PL006"}


def test_cli_summary_diff_tree_normalize(files, capsys):
    good, bad = files
    assert main(["summary", str(good)]) == 0
    assert "BroadcastHashJoin" in capsys.readouterr().out
    assert main(["summary", str(good), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["shuffles"] == 1
    assert main(["diff", str(good), str(bad)]) == 1
    assert "✗ shuffle(s): 1 → 3" in capsys.readouterr().out
    assert main(["diff", str(bad), str(good)]) == 0  # improvement only
    capsys.readouterr()
    assert main(["tree", str(good)]) == 0
    assert "BroadcastHashJoin" in capsys.readouterr().out
    assert main(["normalize", str(good)]) == 0
    assert "#" not in capsys.readouterr().out.replace("# ", "")


def test_cli_missing_file(capsys):
    assert main(["lint", "/nonexistent/plan.txt"]) == 2


def test_pytest_plugin(pytester, monkeypatch, aqe_final_text):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("PLANLOCK_UPDATE", raising=False)
    pytester.makepyfile(
        test_q=f"""
PLAN = {aqe_final_text!r}

def test_daily(planlock):
    planlock.check(PLAN)
    planlock.check(PLAN, "second")
    planlock.expect(PLAN).max_shuffles(1)
"""
    )
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*plan locks: 2 created*"])
    locks = sorted(p.name for p in (pytester.path / "__planlocks__").iterdir())
    assert locks == ["test_q__test_daily.plan", "test_q__test_daily__second.plan"]

    # Change the plan -> fails; --planlock-update fixes it.
    src = (pytester.path / "test_q.py").read_text().replace("BroadcastHashJoin [user_id", "SortMergeJoin [user_id")
    (pytester.path / "test_q.py").write_text(src)
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*join strategy changed*"])
    result = pytester.runpytest("-p", "no:cacheprovider", "--planlock-update")
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*2 updated*"])
