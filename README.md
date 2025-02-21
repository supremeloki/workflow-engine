# workflow-engine

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A DAG workflow engine: tasks with dependencies, retry policies, automatic skipping of downstream work on failure, and context-based output flow — Airflow's execution model, in-process and dependency-free.

## 🚀 Overview

`workflow-engine` executes a task graph: each task declares what it does and what it depends on; the engine topologically orders execution, passes every task's return value into the shared context (so `ctx["extract"]` is the extract task's output), retries transient failures up to `retry_limit`, marks dependents of failed tasks as SKIPPED instead of cascading errors, and reports exactly what completed, skipped, and failed.

## ✨ Features

- **DAG execution:** topological ordering; cycles detected during resolution
- **Context flow:** task outputs land in the run context keyed by `task_id`
- **Retry policy:** per-task `retry_limit`; transient failures retried transparently
- **Graceful degradation:** failure skips only downstream tasks — independent branches still run
- **Task states:** `PENDING → RUNNING → DONE/SKIPPED/FAILED`, inspectable after every run
- **Reset support:** rerun workflows cleanly with `reset_all()`
- **Zero dependencies**

## 🚧 Structure

```
workflow-engine/
├── src/workflow_engine/
│   ├── __init__.py
│   └── core.py
├── tests/
│   └── test_core.py
├── README.md
└── pyproject.toml
```

## 📦 Installation

```bash
git clone https://github.com/supremeloki/workflow-engine.git
cd workflow-engine
python -m venv .venv
.venv\Scripts\activate
pip install -e ".[dev]"
```

## 📋 Requirements

- Python 3.11+
- No runtime dependencies

## 🏃 Quick Start

```python
from workflow_engine import Workflow

flow = Workflow("nightly-etl")
flow.add_task("extract", lambda ctx: fetch_rows())
flow.add_task("transform", lambda ctx: clean(ctx["extract"]),
              depends_on=("extract",))
flow.add_task("load", lambda ctx: write(ctx["transform"]),
              depends_on=("transform",), retry_limit=2)

result = flow.run()
print(result.completed, result.skipped, result.failed)
```

### Parallel branches

```python
flow.add_task("left", ..., depends_on=("root",))
flow.add_task("right", ..., depends_on=("root",))
flow.add_task("join", ..., depends_on=("left", "right"))
```

## 🔧 Error Handling

```text
WorkflowError
├── CircularDependencyError    # self-dependency or resolution-time cycle
└── TaskFailedError            # reserved for strict single-task runs
```

Task failures become state, not exceptions — the run completes with a full report.

## 🧪 Testing

```bash
pytest tests/ -v
```

## 📝 Code Quality

- Full type hints (`X | None` style)
- Zero comments — names carry the meaning
- Failure isolation tested: broken branches never poison independent ones

## 📄 License

MIT — see [LICENSE](LICENSE).

## 👤 Author

**Kooroush Masoumi**

---

⭐ Star this repo if you find it useful!
