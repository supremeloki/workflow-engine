import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import pytest

from workflow_engine import (
    CircularDependencyError,
    TaskState,
    Workflow,
    WorkflowError,
)


def test_sequential_execution_with_context_flow():
    flow = Workflow("etl")
    flow.add_task("extract", lambda ctx: [1, 2, 3])
    flow.add_task("transform", lambda ctx: [x * 2 for x in ctx["extract"]],
                  depends_on=("extract",))
    flow.add_task("load", lambda ctx: sum(ctx["transform"]),
                  depends_on=("transform",))
    result = flow.run()
    assert result.outputs["extract"] == [1, 2, 3]
    assert result.outputs["load"] == 12
    assert result.completed == ("extract", "transform", "load")


def test_execution_order_respects_dependencies():
    flow = Workflow("order")
    flow.add_task("b", lambda c: 1)
    flow.add_task("a", lambda c: 2, depends_on=("b",))
    assert flow.execution_order() == ["b", "a"]


def test_cycle_detected():
    flow = Workflow("cyclic")
    with pytest.raises(CircularDependencyError):
        flow.add_task("x", lambda c: 1, depends_on=("x",))
    flow2 = Workflow("two_node")
    flow2.add_task("a", lambda c: 1)
    flow2.add_task("b", lambda c: 2, depends_on=("a",))
    assert flow2.execution_order() == ["a", "b"]


def test_self_dependency_rejected():
    with pytest.raises(CircularDependencyError):
        Workflow("self").add_task("me", lambda c: 1, depends_on=("me",))


def test_duplicate_task_rejected():
    flow = Workflow("dup")
    flow.add_task("t", lambda c: 1)
    with pytest.raises(WorkflowError):
        flow.add_task("t", lambda c: 2)


def test_failure_skips_dependents_and_runs_independent():
    def broken(ctx):
        raise ValueError("bad input")
    flow = Workflow("partial")
    flow.add_task("broken_root", broken)
    flow.add_task("dependent", lambda c: c, depends_on=("broken_root",))
    flow.add_task("independent", lambda c: "fine")
    result = flow.run()
    assert result.failed == ("broken_root",)
    assert "dependent" in result.skipped
    assert result.outputs.get("independent") == "fine"


def test_retry_succeeds_after_transient_failures():
    attempts = {"n": 0}

    def flaky(ctx):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return "recovered"

    flow = Workflow("retry")
    flow.add_task("flaky_task", flaky, retry_limit=3)
    result = flow.run()
    assert result.failed == ()
    assert attempts["n"] == 3
    assert result.outputs["flaky_task"] == "recovered"


def test_retry_exhaustion_marks_failed():
    flow = Workflow("exhausted")
    flow.add_task("always_bad", lambda c: (_ for _ in ()).throw(RuntimeError("down")),
                  retry_limit=2)
    result = flow.run()
    assert result.failed == ("always_bad",)


def test_ready_tasks_respects_dependencies():
    flow = Workflow("ready")
    flow.add_task("root", lambda c: 1)
    flow.add_task("child", lambda c: 2, depends_on=("root",))
    assert flow.ready_tasks() == ["root"]
    flow.run()


def test_reset_all_restores_pending():
    flow = Workflow("reset")
    flow.add_task("t", lambda c: 1)
    flow.run()
    flow.reset_all()
    assert flow._tasks["t"].state == TaskState.PENDING


def test_parallel_branches_both_complete():
    flow = Workflow("branches")
    flow.add_task("left_a", lambda c: "la")
    flow.add_task("right_b", lambda c: "rb")
    flow.add_task("join", lambda c: (c["left_a"], c["right_b"]),
                  depends_on=("left_a", "right_b"))
    result = flow.run()
    assert set(result.completed) == {"left_a", "right_b", "join"}
