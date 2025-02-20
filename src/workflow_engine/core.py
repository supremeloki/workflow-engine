from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class WorkflowError(Exception):
    pass


class UnknownTaskError(WorkflowError):
    def __init__(self, task_id: str) -> None:
        super().__init__(f"unknown task: {task_id!r}")


class CircularDependencyError(WorkflowError):
    pass


class TaskFailedError(WorkflowError):
    def __init__(self, task_id: str, cause: Exception) -> None:
        super().__init__(f"task {task_id!r} failed: {cause}")
        self.task_id = task_id
        self.cause = cause


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"


TaskAction = Callable[[dict[str, Any]], Any]


@dataclass
class Task:
    task_id: str
    action: TaskAction
    depends_on: tuple[str, ...] = ()
    retry_limit: int = 0
    state: TaskState = TaskState.PENDING
    result: Any = None
    attempts: int = 0
    duration_ms: float = 0.0

    def reset(self) -> None:
        self.state = TaskState.PENDING
        self.result = None
        self.attempts = 0
        self.duration_ms = 0.0

    @property
    def is_terminal(self) -> bool:
        return self.state in {TaskState.DONE, TaskState.SKIPPED, TaskState.FAILED}


@dataclass(frozen=True)
class WorkflowResult:
    outputs: dict[str, Any]
    completed: tuple[str, ...]
    skipped: tuple[str, ...]
    failed: tuple[str, ...]
    total_duration_ms: float


class Workflow:
    def __init__(self, name: str) -> None:
        self.name = name
        self._tasks: dict[str, Task] = {}

    def add_task(self, task_id: str, action: TaskAction,
                 depends_on: Sequence[str] = (), retry_limit: int = 0) -> "Workflow":
        if task_id in self._tasks:
            raise WorkflowError(f"duplicate task: {task_id!r}")
        for dependency in depends_on:
            if dependency == task_id:
                raise CircularDependencyError(f"{task_id!r} depends on itself")
            if dependency not in self._tasks and dependency not in (t.task_id for t in self._pending_specs()):
                pass
        self._tasks[task_id] = Task(
            task_id=task_id, action=action,
            depends_on=tuple(depends_on), retry_limit=retry_limit,
        )
        return self

    def _pending_specs(self) -> list[Task]:
        return []

    def execution_order(self) -> list[str]:
        order: list[str] = []
        visited: set[str] = set()
        temp: set[str] = set()

        def visit(task_id: str) -> None:
            if task_id in visited:
                return
            if task_id in temp:
                raise CircularDependencyError(f"cycle through {task_id!r}")
            temp.add(task_id)
            for dependency in self._tasks[task_id].depends_on:
                if dependency in self._tasks:
                    visit(dependency)
            temp.discard(task_id)
            visited.add(task_id)
            order.append(task_id)

        for task_id in sorted(self._tasks):
            visit(task_id)
        return order

    def ready_tasks(self) -> list[str]:
        return [
            task_id
            for task_id, task in self._tasks.items()
            if task.state == TaskState.PENDING
            and all(
                self._tasks[dep].state == TaskState.DONE
                for dep in task.depends_on
                if dep in self._tasks
            )
        ]

    def run(self, initial_context: dict[str, Any] | None = None) -> WorkflowResult:
        context: dict[str, Any] = dict(initial_context or {})
        context["workflow"] = self.name
        started = time.perf_counter()
        failed_tasks: list[str] = []
        executed_in_order = self.execution_order()

        for _pass in range(len(executed_in_order)):
            progressed = False
            for task_id in executed_in_order:
                task = self._tasks[task_id]
                if task.state != TaskState.PENDING:
                    continue
                dependencies = [d for d in task.depends_on if d in self._tasks]
                if any(self._tasks[d].state == TaskState.FAILED for d in dependencies):
                    task.state = TaskState.SKIPPED
                    progressed = True
                    continue
                if any(self._tasks[d].state != TaskState.DONE for d in dependencies):
                    continue
                attempts_left = task.retry_limit + 1
                stage_started = time.perf_counter()
                last_error: Exception | None = None
                while task.attempts < attempts_left:
                    task.attempts += 1
                    task.state = TaskState.RUNNING
                    try:
                        task.result = task.action(context)
                        context[task_id] = task.result
                        task.state = TaskState.DONE
                        break
                    except Exception as exc:
                        last_error = exc
                        task.state = TaskState.FAILED if task.attempts >= attempts_left else TaskState.PENDING
                task.duration_ms = round((time.perf_counter() - stage_started) * 1000, 3)
                if task.state == TaskState.FAILED:
                    failed_tasks.append(task_id)
                progressed = True
            if not progressed:
                break

        total_ms = round((time.perf_counter() - started) * 1000, 3)
        return WorkflowResult(
            outputs={k: t.result for k, t in self._tasks.items() if t.state == TaskState.DONE},
            completed=tuple(t.task_id for t in self._tasks.values() if t.state == TaskState.DONE),
            skipped=tuple(t.task_id for t in self._tasks.values() if t.state == TaskState.SKIPPED),
            failed=tuple(failed_tasks),
            total_duration_ms=total_ms,
        )

    def reset_all(self) -> None:
        for task in self._tasks.values():
            task.reset()
